# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("ADGofTest", "DBI", "dplyr", "dbplyr", "lubridate", "magrittr", 
                      "mgcv", "purrr", "rlang", "RPostgreSQL", "SPEI", "stringr", 
                      "tidyr", "utils", "yaml")
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
  archivo.config <- paste0(getwd(), "/configuracion_diagnosticos.yml")
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
source(paste0(config$dir$lib, "Estadistico.R"), echo = FALSE)
source(paste0(config$dir$lib, "IndiceSequia.R"), echo = FALSE)
source(paste0(config$dir$base, "lib/funciones_calculo.R"), echo = FALSE)

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
estadistico.facade <- EstadisticoFacade$new(con)
indice.sequia.facade <- IndiceSequiaFacade$new(con)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Leer estadisticas moviles de estaciones de prueba ----
# -----------------------------------------------------------------------------#
estadisticas <- purrr::map_dfr(
  .x = config$estaciones.prueba,
  .f = function(estacion.data) {
    estacion                   <- estacion.facade$buscar(omm_id = estacion.data$omm_id)
    estadisticas.precipitacion <- estadistico.facade$buscar(omm_id = estacion$omm_id, variable_id = 'prcp', 
                                                            estadistico = 'Suma', metodo.imputacion.id = 0) %>%
      dplyr::select(fecha_desde, fecha_hasta, ancho_ventana_pentadas, valor) %>%
      dplyr::rename(prcp = valor)
    estadisticas.temp.min      <- estadistico.facade$buscar(omm_id = estacion$omm_id, variable_id = 'tmin', 
                                                            estadistico = 'Media', metodo.imputacion.id = 0) %>%
      dplyr::select(fecha_desde, fecha_hasta, ancho_ventana_pentadas, valor) %>%
      dplyr::rename(tmin = valor)
    estadisticas.temp.max      <- estadistico.facade$buscar(omm_id = estacion$omm_id, variable_id = 'tmax', 
                                                            estadistico = 'Media', metodo.imputacion.id = 0) %>%
      dplyr::select(fecha_desde, fecha_hasta, ancho_ventana_pentadas, valor) %>% 
      dplyr::rename(tmax = valor)
    
    # Calcular fecha de ultimos datos y alinear series de variables climaticas por fechas. 
    # Agregar columna con dato de evapotranspiracion potencial calculada a partir de
    # metodo de Hargreaves-Samani. Este metodo solamente requiere valores de precipitacion,
    # temperatura minima, maxima y la latitud del punto. 
    #
    # El problema es que el paquete que utiliza constantes calibradas para calculos a nivel 
    # mensual (6 pentadas). Por lo tanto, hay que hacer los calculos mensuales, interpolar el
    # valor de et0 (que es ciclico) para completar faltantes y luego agregar a nivel de mayor
    # cantidad de meses.
    estadisticas.variables <- estadisticas.precipitacion %>%
      dplyr::left_join(estadisticas.temp.min, by = c("fecha_desde", "fecha_hasta", "ancho_ventana_pentadas")) %>%
      dplyr::left_join(estadisticas.temp.max, by = c("fecha_desde", "fecha_hasta", "ancho_ventana_pentadas")) %>%
      dplyr::mutate(srad = CalcularRadiacionSolarExtraterrestre(fecha_desde, fecha_hasta, estacion$lat_dec)) %>%
      dplyr::mutate(et0 = SPEI::hargreaves(Tmin = tmin, Tmax = tmax, Pre = prcp, Ra = srad, na.rm = TRUE)) %>%
      dplyr::select(-srad, -tmin, -tmax)
    fecha.ultimos.datos    <- max(estadisticas.variables$fecha_hasta)
    rm(estadisticas.precipitacion, estadisticas.temp.max, estadisticas.temp.min)
    
    # Interpolacion de et0 a nivel mensual para completar faltantes
    estadisticas.mensuales.variables <- estadisticas.variables %>%
      dplyr::filter(ancho_ventana_pentadas == 6) %>%
      dplyr::mutate(pentada_inicio = fecha.a.pentada.ano(fecha_desde))
    et0        <- dplyr::pull(estadisticas.mensuales.variables, et0)
    pentada    <- dplyr::pull(estadisticas.mensuales.variables, pentada_inicio)
    fit.et0    <- mgcv::gam(et0 ~ s(pentada, bs = "cc"), method = "REML", na.action = na.omit)
    pent.pred  <- sort(unique(pentada))
    et0.pred   <- mgcv::predict.gam(object = fit.et0, newdata = data.frame(pentada = pent.pred))
    et0.smooth <- data.frame(pentada_inicio = pent.pred, et0_pred = et0.pred)
    estadisticas.mensuales.et0 <- estadisticas.mensuales.variables %>%
      dplyr::inner_join(et0.smooth, by = "pentada_inicio") %>%
      dplyr::mutate(et0_completo = dplyr::if_else(! is.na(et0), et0, et0_pred)) %>%
      dplyr::select(fecha_desde, fecha_hasta, ancho_ventana_pentadas, et0_completo)
    rm(et0, pentada, fit.et0, pent.pred, et0.pred, et0.smooth, estadisticas.mensuales.variables)
    
    # Ahora, elimino el valor de et0 de todos los niveles y dejo solo el nivel mensual
    estadisticas.variables <- estadisticas.variables %>%
      dplyr::left_join(estadisticas.mensuales.et0, by = c("fecha_desde", "fecha_hasta", "ancho_ventana_pentadas")) %>%
      dplyr::select(-et0) %>%
      dplyr::rename(et0 = et0_completo)
    rm(estadisticas.mensuales.et0)
    
    # Por ultimo, para los valores faltantes de et0 cuyo ancho de ventana sea mayor a 6 pentadas,
    # sumo los valores correspondientes a los subgrupos de 6 pentadas que componen el periodo.
    estadisticas.variables.completas <- estadisticas.variables %>%
      dplyr::mutate(et0_completo = AgregarET0(fecha_desde, fecha_hasta, ancho_ventana_pentadas, et0)) %>%
      dplyr::select(-et0) %>%
      dplyr::rename(et0 = et0_completo)
    rm(estadisticas.variables)
    
    return (estadisticas.variables.completas)
  }
)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 5. Leer indices calculados para estaciones de prueba ----
# -----------------------------------------------------------------------------#
indice.configuracion <- indice.sequia.facade$buscarConfiguraciones()
indice.parametro <- purrr::map_dfr(
  .x = config$estaciones.prueba,
  .f = function(estacion) {
    return (indice.sequia.facade$buscarParametros(omm_id = estacion$omm_id))
  }
)
indice.resultado.test <- purrr::map_dfr(
  .x = config$estaciones.prueba,
  .f = function(estacion) {
    return (indice.sequia.facade$buscarResultadosTests(omm_id = estacion$omm_id))
  }
)
indice.valor <- purrr::map_dfr(
  .x = config$estaciones.prueba,
  .f = function(estacion) {
    return (indice.sequia.facade$buscarValores(omm_id = estacion$omm_id))
  }
)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 6. Analizar casos de NO-ajuste ----
# -----------------------------------------------------------------------------#
# Los casos de NO-ajuste son aquellos en donde el ajuste falla en primera 
# instancia. No confundir con casos donde hay ajuste, pero este es rechazado
# por no pasar las pruebas de bondad de ajuste

# a) Identificar combinaciones donde los parametros son NA
indice.parametro.na <- indice.parametro %>%
  dplyr::group_by(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes > 0) %>%
  dplyr::select(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id)

# b) Identificar resultados donde hubo fallo y ademas tambien algun parametro original era NA
indice.resultado.na <- indice.resultado.test %>%
  dplyr::filter(test == "") %>%
  dplyr::select(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id, parametro, valor) %>%
  dplyr::group_by(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes > 0) %>%
  dplyr::select(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id)

# c) Hacer join con configuraciones para identificar casos de NO-ajuste
casos.no.ajuste <- indice.parametro.na %>%
  dplyr::inner_join(indice.resultado.na) %>%
  dplyr::inner_join(indice.configuracion, by = c("indice_configuracion_id" = "id"))
rm(indice.parametro.na, indice.resultado.na)

# d) Agregar resultados por indice y metodo de ajuste
casos.no.ajuste.agregados <- casos.no.ajuste %>%
  dplyr::group_by(indice, metodo_ajuste) %>%
  dplyr::summarise(cantidad = n())
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 7. Analizar casos de ajuste rechazado ----
# -----------------------------------------------------------------------------#
# Los casos de ajuste rechazado son aquellos en donde el ajuste devuelve
# parametros no nulos (distintos de NA), pero al aplicarse los tests de bondad
# de ajuste, alguno de ellos falla.

# a) Identificar combinaciones donde los parametros son NA
indice.parametro.na <- indice.parametro %>%
  dplyr::group_by(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes > 0) %>%
  dplyr::select(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id)

# b) Identificar resultados donde hubo fallo pero ningun parametro original era NA
indice.resultado.na <- indice.resultado.test %>%
  dplyr::filter(test == "") %>%
  dplyr::select(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id, parametro, valor) %>%
  dplyr::group_by(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes == 0) %>%
  dplyr::select(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id)

# c) Hacer join con configuraciones para identificar casos de NO-ajuste
casos.ajuste.rechazado <- indice.parametro.na %>%
  dplyr::inner_join(indice.resultado.na) %>%
  dplyr::inner_join(indice.configuracion, by = c("indice_configuracion_id" = "id"))
rm(indice.parametro.na, indice.resultado.na)

# d) Agregar resultados por indice y metodo de ajuste
casos.ajuste.rechazado.agregados <- casos.ajuste.rechazado %>%
  dplyr::group_by(indice, metodo_ajuste) %>%
  dplyr::summarise(cantidad = n())
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 8. Analizar casos de indice nulo ----
# -----------------------------------------------------------------------------#
# Los casos de indice nulo, son aquellos casos en donde el ajuste es exitoso o
# no parametrico (en estos casos no hay parametros y por lo tanto los tests de
# bondad de ajuste parametricos no son aplicables), pero por alguna razon el valor 
# calculado del indice es NA.

# a) Identificar combinaciones donde los parametros son distintos de NA
indice.parametro.ok <- indice.parametro %>%
  dplyr::group_by(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes == 0) %>%
  dplyr::select(indice_configuracion_id, omm_id, pentada_fin, metodo_imputacion_id)

# b) Generar todas las combinaciones posibles para ajustes no-parametricos
indice.no.parametrico <- indice.configuracion %>%
  dplyr::filter(metodo_ajuste == "NoParametrico") %>%
  dplyr::pull(id)

# c) Identificar valores nulos de indice que no provengan de valores nulos de datos
casos.indice.nulo <- indice.valor %>%
  dplyr::filter(is.na(valor_indice) & ! is.na(valor_dato))
casos.indice.nulo.parametrico <- casos.indice.nulo %>%
  dplyr::inner_join(indice.parametro.ok)
casos.indice.nulo.no.parametrico <- casos.indice.nulo %>%
  dplyr::filter(indice_configuracion_id %in% indice.no.parametrico)
casos.indice.nulo <- rbind(casos.indice.nulo.parametrico, casos.indice.nulo.no.parametrico) %>%
  dplyr::inner_join(indice.configuracion, by = c("indice_configuracion_id" = "id"))
rm(indice.parametro.ok, indice.no.parametrico)

# d) Agregar resultados por indice y metodo de ajuste
casos.indice.nulo.agregados <- casos.indice.nulo %>%
  dplyr::group_by(indice, metodo_ajuste) %>%
  dplyr::summarise(cantidad = n())
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 9. Analizar casos de indice no nulo ----
# -----------------------------------------------------------------------------#
# Para aqullos casos donde el indice sea SPI o SPEI, se calculara la normalidad
# de los indices calculados (para los casos donde el indice no sea nulo).

# a) Identificar configuraciones de SPI o SPEI. Obtener valores de indices calculados.
indices.spi.spei <- indice.configuracion %>%
  dplyr::filter(indice %in% c("SPI", "SPEI")) %>%
  dplyr::inner_join(indice.valor, by = c("id" = "indice_configuracion_id")) %>%
  dplyr::rename(indice_configuracion_id = id)

# b) Para cada combinacion(configuracion, estacion, metodo_imputacion) obtener
#    los valores y generar una tabla con:
#    i. % de casos (pentada_fin/ano) con fallo de test (p-value < 0.05)
#   ii. mediana de p-value para de todos los (pentada_fin/ano) sacando los fallos
#  iii. MAD de p-value para de todos los (pentada_fin/ano) sacando los fallos
combinaciones.no.nulos   <- indices.spi.spei %>%
  dplyr::distinct(indice_configuracion_id, omm_id, metodo_imputacion_id)
informe.indices.no.nulos <- purrr::pmap_dfr(
  .l = combinaciones.no.nulos,
  .f = function(indice_configuracion_id, omm_id, metodo_imputacion_id) {
    casos   <- indices.spi.spei %>%
      dplyr::filter(indice_configuracion_id == !! indice_configuracion_id &
                    omm_id == !! omm_id &
                    metodo_imputacion_id == !! metodo_imputacion_id)
    resultados.casos <- purrr::map_dfr(
      .x = unique(casos$pentada_fin),
      .f = function(pentada) {
        casos.pentada  <- casos %>%
          dplyr::filter(pentada_fin == pentada)
        valores.indice <- casos.pentada$valor_indice

        # Eliminar valores faltantes e infinitos
        val.finitos <- valores.indice[which(! is.na(valores.indice) & ! is.infinite(valores.indice))]
        if (length(val.finitos) > 0) {
          gof.res <- ADGofTest::ad.test(x = val.finitos, distr.fun = pnorm)
          return (data.frame(pentada = pentada, p_value = gof.res$p.value))
        } else {
          return (data.frame(pentada = pentada, p_value = NA))
        }
      }
    )
    p.values    <- resultados.casos$p_value
    p.values.ok <- p.values[which(! is.na(p.values) & (p.values >= config$umbral.p.valor))]
    tasa.fallos <- 1 - (length(p.values.ok) / length(p.values))

    return (data.frame(indice_configuracion_id = indice_configuracion_id,
                       omm_id = omm_id, metodo_imputacion_id = metodo_imputacion_id,
                       tasa_fallos = tasa.fallos, p_value_mediana = median(p.values.ok), 
                       p_value_mad = mad(p.values.ok)))
  }
) %>% dplyr::inner_join(indice.configuracion, by = c("indice_configuracion_id" = "id")) %>%
  dplyr::select(indice, escala, metodo_ajuste, omm_id, tasa_fallos, p_value_mediana, p_value_mad)

informe.indices.no.nulos.fallos <- informe.indices.no.nulos %>%
  dplyr::filter(tasa_fallos > 0)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 10. Guardar diagnosticos, cerrar conexion a BD y salir ----
# -----------------------------------------------------------------------------#
save(estadisticas, indice.configuracion, indice.parametro, indice.resultado.test, indice.valor, 
     file = paste0(config$dir$diagnosticos, "DatosEntrada.RData"))
DBI::dbDisconnect(con)
# ------------------------------------------------------------------------------
