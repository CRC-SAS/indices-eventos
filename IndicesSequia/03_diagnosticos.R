# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("ADGofTest", "dplyr", "dbplyr", "lubridate", "magrittr", 
                      "mgcv", "purrr", "rlang", "SPEI", "stringr", "tidyr", 
                      "utils", "yaml", "ncdf4", "futile.logger")
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

normalize_dirnames <- function(dirnames) {
  if (is.atomic(dirnames)) 
    dirnames <- base::sub('/$', '', dirnames)
  if (!is.atomic(dirnames))
    for (nm in names(dirnames)) 
      dirnames[[nm]] <- normalize_dirnames(dirnames[[nm]])
  return (dirnames)
}

# a) YAML de configuracion del generador de diagnósticos
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_diagnosticos.yml")
}
if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuración ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuración ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
  config$dir <- normalize_dirnames(config$dir)
}

# b) YAML de parametros del generador de diagnósticos
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parametros_diagnosticos.yml")
}
if (! file.exists(archivo.params)) {
  stop(paste0("El archivo de parámetros ", archivo.params, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de parámetros ", archivo.params, "...\n"))
  config$params <- yaml::yaml.load_file(archivo.params)
}

# c) YAML de configuración del intercambio de archivos del proceso de generación de índices
if (length(args) > 1) {
  archivo.nombres <- args[3]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.nombres <- paste0(config$dir$data, "/configuracion_archivos_utilizados.yml")
}
if (! file.exists(archivo.nombres)) {
  stop(paste0("El archivo de configuración ", archivo.nombres, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuración ", archivo.nombres, "...\n"))
  config$files <- yaml::yaml.load_file(archivo.nombres)
}

rm(archivo.config, archivo.params, archivo.nombres, args); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 3. Inicializar script (cargar librerias y conectar a BD) ----
# -----------------------------------------------------------------------------#

# a) Cargar codigo
source(glue::glue("{config$dir$lib}/FechaUtils.R"), echo = FALSE)
source(glue::glue("{config$dir$lib}/Helpers.R"), echo = FALSE)
source(glue::glue("{config$dir$base}/lib/funciones_calculo.R"), echo = FALSE)

# b) Buscar ubicaciones a las cuales se aplicara el calculo de indices de sequia
# b.1) Obtener datos producidos por el generador y filtrarlos
futile.logger::flog.info("Leyendo netcdf con datos de entrada")
netcdf_filename <- glue::glue("{config$dir$data}/{config$files$clima_generado}")
points_filename <- glue::glue("{config$dir$data}/{config$files$puntos_a_extraer}")
if (is.null(config$files$puntos_a_extraer))
  datos_climaticos_generados <- gamwgen::netcdf.as.sf(netcdf_filename, add.id = T)
if (!is.null(config$files$puntos_a_extraer))
  datos_climaticos_generados <- gamwgen::netcdf.extract.points.as.sf(netcdf_filename, readRDS(points_filename))
futile.logger::flog.info("Lectura del netcdf finalizada")
# B.x) Reducción de trabajo (solo para pruebas)
# datos_climaticos_generados <- datos_climaticos_generados %>%
#   dplyr::filter( realization %in% c(1, 2), dplyr::between(date, as.Date('1981-01-01'), as.Date('2010-12-31')) )
# b.2) Generar tibble con ubicaciones sobre las cuales iterar
futile.logger::flog.info("Obtener ubicaciones sobre las cuales iterar")
ubicaciones_a_procesar <- datos_climaticos_generados %>%
  dplyr::select(dplyr::ends_with("_id"), longitude, latitude) %>%
  sf::st_transform(crs = sf::st_crs(4326)) %>%
  dplyr::mutate(lon_dec = sf::st_coordinates(geometry)[,'X'],
                lat_dec = sf::st_coordinates(geometry)[,'Y']) %>%
  sf::st_drop_geometry() %>% tibble::as_tibble() %>% dplyr::distinct()
futile.logger::flog.info("Obtención finalizada")

# c) Controlar que las estaciones seleccionadas estén entre aquellas para
# las que se generaron índices de sequía en los pasos previos!
id_column <- IdentificarIdColumn(ubicaciones_a_procesar)
ids_in_params <- sapply(config$params$ubicaciones.prueba, function(u) {u$uid})
ids_in_netcdf <- ubicaciones_a_procesar %>% dplyr::pull(id_column)
if (!all(ids_in_params %in% ids_in_netcdf))
  stop("No hay datos para todas las ubicaciones en parametros_diagnosticos.yml")

# d) Filtrar las ubicaciones a procesar, para correr diagnósticos solo para 
# las ubicaciones señaladas en el archivo parametros_diagnosticos.yml
id_column <- IdentificarIdColumn(ubicaciones_a_procesar)
ubicaciones_a_procesar <- ubicaciones_a_procesar %>%
  dplyr::filter(!!rlang::sym(id_column) %in% ids_in_params)

# e) Buscar las estadisticas moviles 
futile.logger::flog.info("Buscando estadísticas móviles para calcular indices de sequia")
archivo <- glue::glue("{config$dir$data}/{config$files$estadisticas_moviles$resultados}")
estadisticas.moviles <- feather::read_feather(archivo); rm(archivo)

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Leer estadisticas moviles de estaciones de prueba ----
# -----------------------------------------------------------------------------#
estadisticas <- purrr::pmap_dfr(
  .l = ubicaciones_a_procesar,
  .f = function(..., estadisticas.moviles) {
    ubicacion <- tibble::tibble(...)
    
    # Identificar la columna con el id de la ubicación (usualmente station_id, o point_id)
    id_column <- IdentificarIdColumn(ubicacion)
    
    # Filtrar estadisticas.moviles para quedarnos solo con las asociadas a la ubicación analizada
    estadisticas.moviles.ubicacion = estadisticas.moviles %>% 
      dplyr::filter(!!rlang::sym(id_column) == dplyr::pull(ubicacion, !!id_column))
    
    # Estadísticas móviles para prcp
    estadisticas.precipitacion <- estadisticas.moviles.ubicacion %>% 
      dplyr::filter(variable_id == 'prcp', estadistico == 'Suma', metodo_imputacion_id == 0) %>%
      dplyr::select(realizacion, fecha_desde, fecha_hasta, ancho_ventana_pentadas, prcp = valor) 
    # Estadísticas móviles para tmin
    estadisticas.temp.min      <- estadisticas.moviles.ubicacion %>% 
      dplyr::filter(variable_id == 'tmin', estadistico == 'Media', metodo_imputacion_id == 0) %>%
      dplyr::select(realizacion, fecha_desde, fecha_hasta, ancho_ventana_pentadas, tmin = valor) 
    # Estadísticas móviles para tmax
    estadisticas.temp.max      <- estadisticas.moviles.ubicacion %>% 
      dplyr::filter(variable_id == 'tmax', estadistico == 'Media', metodo_imputacion_id == 0) %>%
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
      dplyr::mutate(srad = CalcularRadiacionSolarExtraterrestre(fecha_desde, fecha_hasta, ubicacion$lat_dec)) %>%
      dplyr::mutate(et0 = SPEI::hargreaves(Tmin = tmin, Tmax = tmax, Pre = prcp, Ra = srad, na.rm = TRUE)) %>%
      dplyr::select(-srad, -tmin, -tmax)
    
    fecha.primeros.datos   <- min(estadisticas.variables$fecha_desde)
    fecha.ultimos.datos    <- max(estadisticas.variables$fecha_hasta)
    
    # Se borran datos que ya no será utilizados
    rm(estadisticas.precipitacion, estadisticas.temp.max, estadisticas.temp.min)
    
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
      rm(fit.et0, pent.pred, et0.pred, et0.smooth)
      
      # Ahora, elimino el valor de et0 de todos los niveles y dejo solo el nivel mensual
      estadisticas.variables <- estadisticas.variables %>%
        dplyr::left_join(estadisticas.mensuales.et0, by = c("realizacion","fecha_desde", "fecha_hasta", "ancho_ventana_pentadas")) %>%
        dplyr::select(-et0) %>%
        dplyr::rename(et0 = et0_completo)
      rm(estadisticas.mensuales.et0)
      
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
      script$warn(glue::glue("No es posible ajustar ciclo estacional para ET0: {e$message}\n"))
      estadisticas.variables.completas <<- estadisticas.variables
    })
    rm(et0, pentada, estadisticas.mensuales.variables, estadisticas.variables)
    
    return (estadisticas.variables.completas)
    
  }, 
  estadisticas.moviles = estadisticas.moviles
)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 5. Leer indices calculados para estaciones de prueba ----
# -----------------------------------------------------------------------------#

# a) Obtener configuraciones para el cálculo de los indices de sequía
futile.logger::flog.info("Buscando configuraciones utilizadas para el cálculo de índices")
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$configuraciones}")
indice.configuracion <- feather::read_feather(archivo); rm(archivo)

# b) Obtener parametros generados para los indices de sequía
futile.logger::flog.info("Buscando parámetros generados al calcular los índices")
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$parametros}")
indice.parametro <- feather::read_feather(archivo); rm(archivo)
# indice.parametro <- purrr::map_dfr(
#   .x = config$estaciones.prueba,
#   .f = function(estacion) {
#     return (indice.sequia.facade$buscarParametros(omm_id = 87544))
#   }
# ) # Se guardan en el paso 2 (no se estaban guardando)

# a) Obtener resultados de los tests realizados al calcular de los indices de sequía
futile.logger::flog.info("Buscando resultados de los tests realizados sobre los índices")
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$result_tst}")
indice.resultado.test <- feather::read_feather(archivo); rm(archivo)
# indice.resultado.test <- purrr::map_dfr(
#   .x = config$estaciones.prueba,
#   .f = function(estacion) {
#     return (indice.sequia.facade$buscarResultadosTests(omm_id = 87544))
#   }
# ) # Archivos con resultados de los tests

# c) Buscar índices de sequia 
futile.logger::flog.info("Buscando índices de sequía calculados previamente")
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$resultados}")
resultados_indices_sequia <- feather::read_feather(archivo); rm(archivo)
indice.valor <- resultados_indices_sequia %>%
  dplyr::left_join(indice.configuracion %>% dplyr::rename(indice_configuracion_id = id),
                   by = c("indice", "escala", "distribucion", "metodo_ajuste")) %>%
  dplyr::select(-indice, -escala, -distribucion, -metodo_ajuste, 
                -referencia_comienzo, -referencia_fin)
# indice.valor <- purrr::map_dfr(
#   .x = config$estaciones.prueba,
#   .f = function(estacion) {
#     return (indice.sequia.facade$buscarValores(omm_id = 87544))
#   }
# ) # Los indices calculados antes

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 6. Analizar casos de NO-ajuste ----
# -----------------------------------------------------------------------------#
# Los casos de NO-ajuste son aquellos en donde el ajuste falla en primera 
# instancia. No confundir con casos donde hay ajuste, pero este es rechazado
# por no pasar las pruebas de bondad de ajuste

# a) Identificar combinaciones donde los parametros son NA
indice.parametro.na <- indice.parametro %>%
  dplyr::group_by(indice_configuracion_id, !!rlang::sym(id_column), pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes > 0) %>%
  dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, metodo_imputacion_id)

# b) Identificar resultados donde hubo fallo y ademas tambien algun parametro original era NA
indice.resultado.na <- indice.resultado.test %>%
  dplyr::filter(test == "") %>%
  dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, metodo_imputacion_id, parametro, valor) %>%
  dplyr::group_by(indice_configuracion_id, !!rlang::sym(id_column), pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes > 0) %>%
  dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, metodo_imputacion_id)

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
  dplyr::group_by(indice_configuracion_id, !!rlang::sym(id_column), pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes > 0) %>%
  dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, metodo_imputacion_id)

# b) Identificar resultados donde hubo fallo pero ningun parametro original era NA
indice.resultado.na <- indice.resultado.test %>%
  dplyr::filter(test == "") %>%
  dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, metodo_imputacion_id, parametro, valor) %>%
  dplyr::group_by(indice_configuracion_id, !!rlang::sym(id_column), pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes == 0) %>%
  dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, metodo_imputacion_id)

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
  dplyr::group_by(indice_configuracion_id, !!rlang::sym(id_column), pentada_fin, metodo_imputacion_id) %>%
  dplyr::summarise(faltantes = length(which(is.na(valor)))) %>%
  dplyr::filter(faltantes == 0) %>%
  dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, metodo_imputacion_id)

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
  dplyr::distinct(indice_configuracion_id, !!rlang::sym(id_column), metodo_imputacion_id)
informe.indices.no.nulos <- purrr::pmap_dfr(
  .l = combinaciones.no.nulos,
  .f = function(...) {
    row <- tibble::tibble(...) 
    
    casos   <- indices.spi.spei %>%
      dplyr::filter(indice_configuracion_id == row$indice_configuracion_id &
                    !!rlang::sym(id_column) == dplyr::pull(row, !!id_column) &
                    metodo_imputacion_id == row$metodo_imputacion_id)
    
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
    p.values.ok <- p.values[which(! is.na(p.values) & (p.values >= config$params$umbral.p.valor))]
    tasa.fallos <- 1 - (length(p.values.ok) / length(p.values))

    return (tibble::tibble(indice_configuracion_id = row$indice_configuracion_id,
                           !!id_column := dplyr::pull(row, !!id_column) , 
                           metodo_imputacion_id = row$metodo_imputacion_id,
                           tasa_fallos = tasa.fallos, p_value_mediana = median(p.values.ok), 
                           p_value_mad = mad(p.values.ok)))
  }
) %>% dplyr::inner_join(indice.configuracion, by = c("indice_configuracion_id" = "id")) %>%
  dplyr::select(indice, escala, metodo_ajuste, !!id_column, tasa_fallos, p_value_mediana, p_value_mad)

informe.indices.no.nulos.fallos <- informe.indices.no.nulos %>%
  dplyr::filter(tasa_fallos > 0)
# ------------------------------------------------------------------------------
