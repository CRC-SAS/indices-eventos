# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("ADGofTest", "caret", "DBI", "dplyr", "dbplyr", "fitdistrplus", 
                      "lmomco", "logspline", "lubridate", "magrittr", "mgcv", "purrr", 
                      "RPostgres", "SCI", "sirad", "SPEI", "stats", "stringr", 
                      "utils", "WRS2", "yaml", "yardstick")
for (pack in list.of.packages) {
  if (!require(pack, character.only = TRUE)) {
    stop(paste0("Paquete no encontrado: ", pack))
  }
}
rm(pack); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 2. Leer archivo de configuracion ----
# -----------------------------------------------------------------------------#
# i. Leer YML de configuracion
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos.
  # Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_calculador_indices.yml")
}
if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuracion de ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuracion ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
}

# b) YAML de parametros
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parametros_calculador_indices.yml")
}
if (! file.exists(archivo.params)) {
  stop(paste0("El archivo de parametros de ", archivo.params, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de parametros ", archivo.params, "...\n"))
  config$params <- yaml::yaml.load_file(archivo.params)
}

# c) YAML de conf de índices
if (length(args) > 0) {
  archivo.config.indices <- args[3]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.config.indices <- paste0(getwd(), "/configuracion_generador_configuraciones.yml")
}
if (! file.exists(archivo.config.indices)) {
  stop(paste0("El archivo de configuracion de ", archivo.config.indices, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuracion ", archivo.config.indices, "...\n"))
  config$conf_indices <- yaml.load_file(archivo.config.indices)
}

rm(archivo.config, archivo.params, archivo.config.indices, args); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 3. Cargar librerias propias e iniciar script ----
# -----------------------------------------------------------------------------#

# a) Carga de clases de uso general
source(paste0(config$dir$lib, "Facade.R"), echo = FALSE)
source(paste0(config$dir$lib, "FechaUtils.R"), echo = FALSE)
source(paste0(config$dir$lib, "Estacion.R"), echo = FALSE)
source(paste0(config$dir$lib, "Estadistico.R"), echo = FALSE)
source(paste0(config$dir$lib, "IndiceSequia.R"), echo = FALSE)
source(paste0(config$dir$lib, "Script.R"), echo = FALSE)
source(paste0(config$dir$lib, "Task.R"), echo = FALSE)

# b. Carga de codigo para ajuste de distribuciones, calculo de indices y ejecucion distribuida
source(paste0(config$dir$base, "lib/", "funciones_ajuste.R"), echo = FALSE)
source(paste0(config$dir$base, "lib/", "funciones_calculo.R"), echo = FALSE)
source(paste0(config$dir$base, "lib/", "funciones_test_ajuste.R"), echo = FALSE)
source(paste0(config$dir$base, "lib/", "funciones_worker.R"), echo = FALSE)

# c. Iniciar script y obtener fecha de ejecucion
script <- Script$new(run.dir = config$dir$run,
                     name = "IndicesGenerador")
script$start()

# d. Datos obtenidos del generador
datos_climaticos_generados <- feather::read_feather("datos_simulados_alessio.feather")

# e. Fechas mínima y máxima
fecha.minima <- min(datos_climaticos_generados$fecha)
fecha.maxima <- max(datos_climaticos_generados$fecha)

# f. Calcular fecha minima de inicio de pentada
fecha.minima.inicio.pentada <- fecha.inicio.pentada(fecha.minima)

# g. Calcular fecha maxima de inicio y fin de pentada
fecha.maxima.inicio.pentada <- fecha.inicio.pentada(fecha.maxima)
fecha.maxima.fin.pentada    <- fecha.fin.pentada(fecha.maxima)
if (fecha.maxima.fin.pentada > fecha.maxima) {
  # Ir una pentada hacia atras
  fecha.maxima.inicio.pentada <- sumar.pentadas(fecha.maxima.inicio.pentada, -1)
  fecha.maxima.fin.pentada    <- fecha.fin.pentada(fecha.maxima.inicio.pentada)
}

rm(fecha.maxima.fin.pentada)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Conectar a base de datos y crear facades ----
# -----------------------------------------------------------------------------#
# a) Conectar a base de datos e indicar timezone para la sesion
con <- DBI::dbConnect(drv = RPostgres::Postgres(),
                      dbname = config$db$name,
                      user = config$db$user,
                      password = config$db$pass,
                      host = config$db$host
)
DBI::dbExecute(con, "SET TIME ZONE 'UTC'")

# b) Crear facades de estacion
estacion.facade <- EstacionFacade$new(con)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 5. Calcular indices de sequia por estacion ----
# -----------------------------------------------------------------------------#

# Buscar estaciones convencionales a las cuales se les aplicara el calculo de indices de sequia
#script$info("Buscando estaciones para calcular indices de sequia")
#estaciones <- estacion.facade$buscar()

# PARAMETROS FALTANTES DE CalcularIndicesSequiaEstacion!!
estadistico.facade <- EstadisticoFacade$new(con=con)
indice.sequia.facade <- IndiceSequiaFacade$new(con=con)


# Estaciones de prueba:
# 86285 Cap Meza  # Paraguay
# 87548 Junín
# 87544 Pehuajó
# 85406 Desierto  # es una estación con poca lluvia! ver test CES04

# Obtener datos estación
estacion = estacion.facade$buscar(omm_id = 87548)

# Asignar funcion rgenlog a Global Environment (sino la ejecucion en procesos hijos no funciona bien)
assign("rgenlog", rgenlog, .GlobalEnv)

# Obtener configuraciones de indices
# configuraciones.indice <- indice.sequia.facade$buscarConfiguraciones()
configuraciones.indice <- configuraciones <- purrr::map_dfr(
  .x = names(config$conf_indices$indices),
  .f = function(indice, config) {
    indice.data <- config$indices[[indice]]
    indice.conf <- purrr::cross_df(
      .l = list(escala = indice.data$escala, distribucion = indice.data$distribucion, 
                metodo_ajuste = indice.data$metodo.ajuste, periodo_referencia = indice.data$periodo.referencia)
    ) %>% 
      dplyr::mutate(indice = indice) %>%
      as.data.frame()
    
    # Calcular anos de periodo de referencia
    anos.referencia <- stringr::str_match(string = indice.conf$periodo_referencia,
                                          pattern = "(\\d+)-(\\d+)")[,2:3]
    
    indice.conf <- dplyr::bind_cols(indice.conf,
                                    data.frame(referencia_ano_desde = anos.referencia[,1],
                                               referencia_ano_hasta = anos.referencia[,2],
                                               stringsAsFactors = FALSE))
    return (indice.conf)  
  },
  config = config$conf_indices
) %>%
  dplyr::mutate(id = dplyr::row_number(), 
                referencia_comienzo = sprintf("%s-01-01", referencia_ano_desde),
                referencia_fin = sprintf("%s-12-31", referencia_ano_hasta)) %>%
  dplyr::select(id, indice, escala, distribucion, metodo_ajuste, referencia_comienzo, referencia_fin) 

# Obtener datos generados por el generador de series sintéticas
estadisticos_datos_generados <- feather::read_feather("estadisticos_25r_1stn.feather")

# Buscar las estadisticas moviles de precipitacion por si son necesarias
# estadisticas.precipitacion <- estadistico.facade$buscar(omm_id = estacion$omm_id, variable_id = 'prcp', 
#                                                         estadistico = 'Suma', metodo.imputacion.id = 0) %>%
#   dplyr::select(fecha_desde, fecha_hasta, ancho_ventana_pentadas, valor) %>%
#   dplyr::rename(prcp = valor)
estadisticas.precipitacion <- estadisticos_datos_generados %>%
  dplyr::filter(station_id == estacion$omm_id & variable_id == "prcp" & 
                estadistico == "Suma" & metodo_imputacion_id == 0) %>%
  dplyr::select(realizacion, fecha_desde, fecha_hasta, ancho_ventana_pentadas, prcp = valor) 
# estadisticas.temp.min      <- estadistico.facade$buscar(omm_id = estacion$omm_id, variable_id = 'tmin', 
#                                                         estadistico = 'Media', metodo.imputacion.id = 0) %>%
#   dplyr::select(fecha_desde, fecha_hasta, ancho_ventana_pentadas, valor) %>%
#   dplyr::rename(tmin = valor)
estadisticas.temp.min      <- estadisticos_datos_generados %>%
  dplyr::filter(station_id == estacion$omm_id & variable_id == "tmin" & 
                  estadistico == "Media" & metodo_imputacion_id == 0) %>%
  dplyr::select(realizacion, fecha_desde, fecha_hasta, ancho_ventana_pentadas, tmin = valor) 
# estadisticas.temp.max      <- estadistico.facade$buscar(omm_id = estacion$omm_id, variable_id = 'tmax', 
#                                                         estadistico = 'Media', metodo.imputacion.id = 0) %>%
#   dplyr::select(fecha_desde, fecha_hasta, ancho_ventana_pentadas, valor) %>% 
#   dplyr::rename(tmax = valor)
estadisticas.temp.max      <- estadisticos_datos_generados %>%
  dplyr::filter(station_id == estacion$omm_id & variable_id == "tmax" & 
                  estadistico == "Media" & metodo_imputacion_id == 0) %>%
  dplyr::select(realizacion, fecha_desde, fecha_hasta, ancho_ventana_pentadas, tmax = valor) 

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
  dplyr::left_join(estadisticas.temp.min, by = c("realizacion", "fecha_desde", "fecha_hasta", "ancho_ventana_pentadas")) %>%
  dplyr::left_join(estadisticas.temp.max, by = c("realizacion", "fecha_desde", "fecha_hasta", "ancho_ventana_pentadas")) %>%
  dplyr::mutate(srad = CalcularRadiacionSolarExtraterrestre(fecha_desde, fecha_hasta, estacion$lat_dec)) %>%
  dplyr::mutate(et0 = SPEI::hargreaves(Tmin = tmin, Tmax = tmax, Pre = prcp, Ra = srad, na.rm = TRUE)) %>%
  dplyr::select(-srad, -tmin, -tmax)

fecha.primeros.datos   <- min(estadisticas.variables$fecha_desde)
fecha.ultimos.datos    <- max(estadisticas.variables$fecha_hasta)
#rm(estadisticas.precipitacion, estadisticas.temp.max, estadisticas.temp.min)

# Interpolacion de et0 a nivel mensual para completar faltantes
estadisticas.mensuales.variables <- estadisticas.variables %>%
  dplyr::filter(ancho_ventana_pentadas == 6) %>%
  dplyr::mutate(pentada_inicio = fecha.a.pentada.ano(fecha_desde))
et0        <- dplyr::pull(estadisticas.mensuales.variables, et0)
pentada    <- dplyr::pull(estadisticas.mensuales.variables, pentada_inicio)
tryCatch({
  fit.et0    <- mgcv::gam(et0 ~ s(pentada, bs = "cc"), method = "REML", na.action = na.omit)
  pent.pred  <- sort(unique(pentada))
  et0.pred   <- mgcv::predict.gam(object = fit.et0, newdata = data.frame(pentada = pent.pred))
  et0.smooth <- data.frame(pentada_inicio = pent.pred, et0_pred = et0.pred)
  estadisticas.mensuales.et0 <- estadisticas.mensuales.variables %>%
    dplyr::inner_join(et0.smooth, by = "pentada_inicio") %>%
    dplyr::mutate(et0_completo = dplyr::if_else(! is.na(et0), et0, et0_pred)) %>%
    dplyr::select(realizacion, fecha_desde, fecha_hasta, ancho_ventana_pentadas, et0_completo)
  #rm(fit.et0, pent.pred, et0.pred, et0.smooth)
  
  # Ahora, elimino el valor de et0 de todos los niveles y dejo solo el nivel mensual
  estadisticas.variables <- estadisticas.variables %>%
    dplyr::left_join(estadisticas.mensuales.et0, by = c("realizacion", "fecha_desde", "fecha_hasta", "ancho_ventana_pentadas")) %>%
    dplyr::select(-et0) %>%
    dplyr::rename(et0 = et0_completo)
  #rm(estadisticas.mensuales.et0)
  
  # Por ultimo, para los valores faltantes de et0 cuyo ancho de ventana sea mayor a 6 pentadas,
  # sumo los valores correspondientes a los subgrupos de 6 pentadas que componen el periodo.
  estadisticas.variables.completas <<- purrr::map_dfr(
      .x = unique(estadisticas.variables$realizacion),
      .f = function(r){
        estadisticas.variables %>% dplyr::filter(realizacion == r) %>%
        dplyr::mutate(et0_completo = AgregarET0(fecha_desde, fecha_hasta, ancho_ventana_pentadas, et0)) %>%
        dplyr::select(-et0) %>% dplyr::rename(et0 = et0_completo)
    })
}, error = function(e) {
  script$warn(paste0("No es posible ajustar ciclo estacional para ET0: ", e$message, "\n"))
  estadisticas.variables.completas <<- estadisticas.variables
})
#rm(et0, pentada, estadisticas.mensuales.variables, estadisticas.variables)

# Ejecutar calculo para cada configuracion.
# Cada configuracion es una combinacion unica de indice, escala, distribucion,
# metodo de ajuste y periodo de referencia.
#for (row_index in seq(from = 1, to = nrow(configuraciones.indice))) {
resultado <- purrr::map_dfr(
  .x = seq(from = 1, to = nrow(configuraciones.indice)),
  .f = function(row_index) {
    # Obtener configuracion de calculo
    configuracion.indice <- configuraciones.indice[row_index, ]
    
    # Obtener estadisticas para esa escala de tiempo
    estadisticas.variables <- estadisticas.variables.completas %>%
      dplyr::filter(ancho_ventana_pentadas == configuracion.indice$escala * 6)
    
    # No hay indices calculados para ningun periodo.
    # Procesar desde la primera pentada con datos estadisticos.
    fechas.procesables <- seq.pentadas(fecha.primeros.datos, fecha.ultimos.datos)
    
    #### ----------------------------------------------------------------------#
    #### Para cada fecha procesable:
    #### 1. Buscar parametros de ajuste para esta configuracion, red, estacion,
    ####    y pentada de fin (correspondiente a la fecha procesable)
    #### 2. Si no exiten parametros:
    ####    a. Determinar periodo de agregacion. El final del periodo esta dado
    ####       por la fecha procesable. El inicio, de acuerdo a la longitud de la escala
    ####    b. Agregar valores para el periodo de referencia. Cuando haya periodos que caigan
    ####       parte en un ano y en otro, siempre la extension es hacia el fin del periodo
    ####       de referencia asumiendo que es mas probable que haya datos mas recientes.
    ####    c. Ajustar distribucion segun parametros de configuracion. En este paso deben
    ####       considerarse la cantidad de faltantes. Si hay demasiados faltantes (de acuerdo
    ####       a unbrales definidos), entonces todos los parametros devueltos son NA.
    ####    d. Aplicar tests de bondad de ajuste (solo para el caso de ajustes parametricos
    ####       con distribucion definina). Si alguno de los tests falla, entonces considerar 
    ####       que el ajuste no es bueno. En ese caso, reemplazar todos los valores de parametros por NA.
    ####    e. Guardar parametros en base de datos. En el caso de ser un objeto de ajuste, 
    ####       no se guarda en la base de datos. Ir directamente al paso 3.
    #### 3. Una vez obtenidos los parametros de ajuste:
    ####    i. Si alguno de los parametros es NA, tanto el valor del indice como su
    ####       percentil asociado son NA. Ir directamente al paso iii.
    ####    ii. Calcular valor de indice y percentil asociado.
    ####    iii. Guardar valor de indice y percentil asociado en base de datos.
    #### ----------------------------------------------------------------------#
    
    # Primero determinar que fechas pertenecen a la misma pentada del ano.
    # Esas fechas tiene los mismos parametros. De este modos, optimizamos calculos.
    fechas.pentada.ano <- fecha.a.pentada.ano(fechas.procesables)
    pentadas.unicas    <- sort(unique(fechas.pentada.ano))
    script$info(paste0("Calculando indices de sequia de estacion ", estacion$nombre,
                       " (", estacion$omm_id, ") para configuracion ", configuracion.indice$id))
    
    resultados.indice.configuracion <- purrr::map_dfr(
      .x = pentadas.unicas,
      .f = function(pentada.ano) {
        # a. Buscar fechas procesables correspondientes a esa pentada
        fechas.procesables.pentada <- fechas.procesables[which(fechas.pentada.ano == pentada.ano)]
        
        # b. Tomando la primera fecha como ejemplo (daria lo mismo tomar cualquiera),
        #    obtener parametros de ajuste.
        parametros.ajuste <- AjustarParametrosEstacionFecha(estacion, fechas.procesables.pentada[1], configuracion.indice,
                                                            script, config, estadisticas.variables)
        
        # c. Calcular indices para fechas correspondientes a esa pentada (los parametros son los mismos)
        resultados.indice.configuracion.pentada <- purrr::map_dfr(
          .x = fechas.procesables.pentada,
          .f = function(fecha.procesable) {
            purrr::map_dfr(
              .x = unique(estadisticas.variables$realizacion),
              .f = function(r) {
                estadisticas.variables.realizacion <- estadisticas.variables %>% dplyr::filter(realizacion == r)
                return (tibble::tibble(realizacion = r) %>% tidyr::crossing(
                  CalcularIndicesSequiaEstacionFecha(estacion, fecha.procesable, parametros.ajuste, configuracion.indice, 
                                                     script, config,estadisticas.variables.realizacion)))
              }
            )
          }
        )
        return (resultados.indice.configuracion.pentada)
      }
    )
    
    # iii. Guardar valor de indice y percentil asociado en base de datos.
    if (nrow(resultados.indice.configuracion) > 0) {
      script$info(paste0("Almacenando indices de sequia de estacion ", estacion$nombre,
                         " (", estacion$omm_id, ") para configuracion ", configuracion.indice$id))
      valores.indice <- resultados.indice.configuracion %>%
        dplyr::mutate(indice = configuracion.indice$indice, escala = configuracion.indice$escala, 
                      distribucion = configuracion.indice$distribucion, metodo_ajuste = configuracion.indice$metodo_ajuste, 
                      metodo_imputacion_id = 0, station_id = estacion$omm_id) %>%
        dplyr::select(station_id, realizacion, pentada_fin, ano, metodo_imputacion_id, indice, escala, 
                      distribucion, metodo_ajuste, valor_dato, valor_indice, percentil_dato)
      #indice.sequia.facade$actualizarValoresIndiceEstacion(valores = valores.indice)
      return(valores.indice)
    }
  }
)
#} # Fin de loop para iterar sobre todas las configuraciones
# ------------------------------------------------------------------------------

feather::write_feather(resultado, "indices_estacion_87548.feather")

# medias_pentadas <- resultado %>% dplyr::filter(indice == 'SPEI') %>%
#   dplyr::group_by(pentada_fin, ano) %>%
#   dplyr::summarise(media = median(valor_indice)) %>%
#   dplyr::mutate(fecha = pentada.ano.a.fecha.inicio(pentada_fin, ano))

resultados_grafico <- resultado %>% dplyr::filter(indice == 'SPI') %>%
  dplyr::mutate(fecha = pentada.ano.a.fecha.inicio(pentada_fin, ano))

# Precipitación anual simulada
ggplot2::ggplot() +
  ggplot2::geom_line(data = resultados_grafico, ggplot2::aes(x = fecha, y = valor_indice, group = realizacion),
                     color = 'Steelblue', alpha = 0.2) +
  #ggplot2::geom_line(data = medias_pentadas, ggplot2::aes(x = fecha, y = media)) +
  #ggplot2::scale_x_continuous(breaks = seq(1961, 2018, 5)) +
  ggplot2::ylab('SPI') + ggplot2::xlab("Años") +
  ggplot2::labs(title = 'SPI-1', subtitle = paste0('Estacion: ', unique(resultados_grafico$omm_id))) +
  ggplot2::theme_bw() +
  ggplot2::ggsave("spi.png", dpi = 600, device = 'png')

# -----------------------------------------------------------------------------#
# --- PASO 6. Finalizar script cerrando conexion a base de datos ----
# -----------------------------------------------------------------------------#

# a) Cerrar conexion a base de datos
DBI::dbDisconnect(con)

# b) Finalizar script
script$stop()