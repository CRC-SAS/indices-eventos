# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("DBI", "dplyr", "dbplyr", "lubridate", "magrittr", "purrr", 
                      "RPostgreSQL", "stringr", "tidyr", "utils", "yaml")
for (pack in list.of.packages) {
  if (!require(pack, character.only = TRUE)) {
    stop(paste0("Paquete no encontrado: ", pack))
  }
}
rm(pack, list.of.packages); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 2. Leer archivo de configuracion ----
# -----------------------------------------------------------------------------#
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_chequeo_faltantes.yml")
}
if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuracion de ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuracion ", archivo.config, "...\n"))
  config <- yaml.load_file(archivo.config)
}

rm(archivo.config, args); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 3. Inicializar script (cargar librerias y conectar a BD) ----
# -----------------------------------------------------------------------------#
# a) Cargar codigo
source(paste0(config$dir$lib, "Facade.R"), echo = FALSE)
source(paste0(config$dir$lib, "FechaUtils.R"), echo = FALSE)
source(paste0(config$dir$lib, "Estacion.R"), echo = FALSE)
source(paste0(config$dir$lib, "IndiceSequia.R"), echo = FALSE)

# b) Conectar a la base de datos
con <- DBI::dbConnect(drv = RPostgreSQL::PostgreSQL(),
                      dbname = config$db$name,
                      user = config$db$user,
                      password = config$db$pass,
                      host = config$db$host
)
DBI::dbExecute(con, "SET TIME ZONE 'UTC'")

# c) Crear facades
estacion.facade <- EstacionFacade$new(con)
indice.sequia.facade <- IndiceSequiaFacade$new(con)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Buscar datos para cada configuracion ----
# -----------------------------------------------------------------------------#

parametros <- purrr::map_dfr(
  .x = config$configuraciones,
  .f = function(configuracion) {
    # i. Buscar configuracion
    indice.configuracion <- indice.sequia.facade$buscarConfiguraciones(indice = configuracion$indice,
                                                                       metodo_ajuste = configuracion$metodo_ajuste) %>%
      dplyr::select(id, metodo_ajuste, escala) %>%
      dplyr::rename(indice_configuracion_id = id)
    
    # ii. Buscar datos
    indice.parametros <- indice.sequia.facade$buscarParametros(indice_configuracion_id = indice.configuracion$id,
                                                               omm_id = config$omm_id, pentada_fin = 6 * config$mes) %>%
      dplyr::inner_join(indice.configuracion) %>%
      dplyr::select(metodo_ajuste, escala, omm_id, pentada_fin, parametro, valor)
    
    return (indice.parametros)
  }
) %>% tidyr::spread(key = parametro, value = valor)

indices <- purrr::map_dfr(
  .x = config$configuraciones,
  .f = function(configuracion) {
    # i. Buscar configuracion
    indice.configuracion <- indice.sequia.facade$buscarConfiguraciones(indice = configuracion$indice,
                                                                       metodo_ajuste = configuracion$metodo_ajuste) %>%
      dplyr::select(id, metodo_ajuste, escala) %>%
      dplyr::rename(indice_configuracion_id = id)
    
    # ii. Buscar datos
    indice.valores <- indice.sequia.facade$buscarValores(indice_configuracion_id = indice.configuracion$id,
                                                         omm_id = config$omm_id, mes = config$mes,
                                                         fecha_hasta = config$fecha_hasta) %>%
      dplyr::inner_join(indice.configuracion) %>%
      dplyr::select(metodo_ajuste, escala, omm_id, pentada_fin, ano, valor_dato, valor_indice, percentil_dato)
    
    return (indice.valores)
  }
)

DBI::dbDisconnect(con)
rm (estacion.facade, indice.sequia.facade)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 5. Calcular estadísticas por estación y mes del año ----
# -----------------------------------------------------------------------------#

# i. Calcular porcentaje de faltantes
estadisticas.faltantes <- indices %>%
  dplyr::mutate(mes = pentada_fin / 6) %>%
  dplyr::group_by(metodo_ajuste, escala, omm_id, mes) %>%
  dplyr::summarise(total = dplyr::n(), disponibles = sum(dplyr::if_else(! is.na(valor_indice), 1, 0))) %>%
  dplyr::mutate(porcentaje_disponibles = 100 * disponibles / total,
                porcentaje_faltantes = 100 - porcentaje_disponibles) %>%
  dplyr::arrange(porcentaje_faltantes)

# ii. Calcular diferencia para los distintos metodos de ajuste
diferencias <- estadisticas.faltantes %>%
  dplyr::select(metodo_ajuste, escala, omm_id, mes, porcentaje_disponibles) %>%
  tidyr::spread(key = metodo_ajuste, value = porcentaje_disponibles) %>%
  dplyr::mutate(diferencia = `ML-SinRemuestreo` - NoParametrico)

# iii. Graficar mapa de calor de diferencias
ggplot2::ggplot(data = diferencias) +
  ggplot2::geom_tile(mapping = ggplot2::aes(x = as.factor(omm_id), y = as.factor(escala), 
                                            fill = cut(diferencia, breaks = c(seq(-100, 100, 10))))) +
  ggplot2::facet_wrap(~mes) +
  ggplot2::scale_fill_brewer(type = "seq", palette = "Spectral", direction = -1, aesthetics = "fill") +
  ggplot2::labs(x = "ID OMM", y = "Escala", fill = "Diferencia de porcentajes", 
                title = "Diferencia de porcentaje de datos disponibles para SPI",
                subtitle = "Máxima verosimilitud sin remuestreo - No paramétrico")
  
# iv. Graficar mapa de calor de porcentaje de datos disponibles para ML-SinRemuestreo
ggplot2::ggplot(data = diferencias) +
  ggplot2::geom_tile(mapping = ggplot2::aes(x = as.factor(omm_id), y = as.factor(escala), 
                                            fill = cut(`ML-SinRemuestreo`, breaks = c(-Inf, seq(0, 100, 10))))) +
  ggplot2::facet_wrap(~mes) +
  ggplot2::scale_fill_brewer(type = "seq", palette = "Spectral", direction = 1, aesthetics = "fill") +
  ggplot2::labs(x = "ID OMM", y = "Escala", fill = "% Disponible", 
                title = "Porcentaje de valores de índice disponibles para SPI",
                subtitle = "Ajuste por máxima verosimilitud sin remuestreo")

# v. Estadisticas sobre la base de datos disponibles (valor_dato no es NA) para ML-SinRemuestreo
estadisticas.faltantes.dato.no.na <- indices %>%
  dplyr::mutate(mes = pentada_fin / 6) %>%
  dplyr::filter(! is.na(valor_dato) & (metodo_ajuste == "ML-SinRemuestreo")) %>%
  dplyr::group_by(metodo_ajuste, escala, omm_id, mes) %>%
  dplyr::summarise(total = dplyr::n(), disponibles = sum(dplyr::if_else(! is.na(valor_indice), 1, 0))) %>%
  dplyr::mutate(porcentaje_disponibles = 100 * disponibles / total,
                porcentaje_faltantes = 100 - porcentaje_disponibles) %>%
  dplyr::arrange(porcentaje_faltantes)

# vi. Graficar mapa de calor de porcentaje de datos disponibles para ML-SinRemuestreo sobre la base de datos no NA
ggplot2::ggplot(data = estadisticas.faltantes.dato.no.na) +
  ggplot2::geom_tile(mapping = ggplot2::aes(x = as.factor(omm_id), y = as.factor(escala), 
                                            fill = cut(porcentaje_disponibles, breaks = c(-Inf, 0, 100), labels = c("Sin ajuste", "Con ajuste")))) +
  ggplot2::facet_wrap(~mes) +
  ggplot2::labs(x = "ID OMM", y = "Escala", fill = "% Disponible", 
                title = "Comparación de casos de no ajuste vs. ajuste",
                subtitle = "Ajuste por máxima verosimilitud sin remuestreo")

# vii. Estadisticas de casos sin datos
estadisticas.sin.datos <- indices %>%
  dplyr::mutate(mes = pentada_fin / 6) %>%
  dplyr::filter(metodo_ajuste == "ML-SinRemuestreo") %>%
  dplyr::group_by(metodo_ajuste, escala, omm_id, mes) %>%
  dplyr::summarise(total = dplyr::n(), disponibles = sum(dplyr::if_else(! is.na(valor_dato), 1, 0))) %>%
  dplyr::mutate(porcentaje_disponibles = 100 * disponibles / total,
                porcentaje_faltantes = 100 - porcentaje_disponibles) %>%
  dplyr::arrange(porcentaje_faltantes)

# vi. Graficar mapa de calor de porcentaje de datos disponibles para ML-SinRemuestreo sobre la base de datos no NA
ggplot2::ggplot(data = estadisticas.sin.datos) +
  ggplot2::geom_tile(mapping = ggplot2::aes(x = as.factor(omm_id), y = as.factor(escala), 
                                            fill = cut(porcentaje_disponibles, breaks = c(-Inf, seq(0, 100, 10))))) +
  ggplot2::facet_wrap(~mes) +
  ggplot2::scale_fill_brewer(type = "seq", palette = "Spectral", direction = 1, aesthetics = "fill") +
  ggplot2::labs(x = "ID OMM", y = "Escala", fill = "% Disponible", 
                title = "Porcentaje de datos disponibles para SPI",
                subtitle = "Ajuste por máxima verosimilitud sin remuestreo")
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 6. Series temporales ----
# -----------------------------------------------------------------------------#

# Generar set de datos
series.temporales <- indices %>%
  dplyr::filter(omm_id %in% config$series_temporales$omm_id &
                ano >= config$series_temporales$ano_desde &
                ano <= config$series_temporales$ano_hasta) %>%
  dplyr::mutate(fecha = fecha.fin.pentada(pentada.ano.a.fecha.inicio(pentada_fin, ano)))

# Graficar
ggplot2::ggplot(data = dplyr::filter(series.temporales, escala == 3)) +
  ggplot2::geom_line(mapping = ggplot2::aes(x = fecha, y = valor_indice, 
                                            col = metodo_ajuste, linetype = metodo_ajuste)) +
  ggplot2::theme(
    legend.position = "bottom"
  )
# ------------------------------------------------------------------------------