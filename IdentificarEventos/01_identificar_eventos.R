# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("dplyr")
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

normalize_dirnames <- function(dirnames) {
  if (is.atomic(dirnames)) 
    dirnames <- base::sub('/$', '', dirnames)
  if (!is.atomic(dirnames))
    for (nm in names(dirnames)) 
      dirnames[[nm]] <- normalize_dirnames(dirnames[[nm]])
    return (dirnames)
}

# a) YAML de configuracion de la identificación de eventos
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_identificar_eventos.yml")
}
if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuración ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuración ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
  config$dir <- normalize_dirnames(config$dir)
}

# b) YAML de parametros de la identificación de eventos
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parametros_identificar_eventos.yml")
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
# --- PASO 3. Cargar librerias propias e iniciar script ----
# -----------------------------------------------------------------------------#

# a) Cargar librerias
source(glue::glue("{config$dir$lib}/FechaUtils.R"), echo = FALSE)
source(glue::glue("{config$dir$lib}/Script.R"), echo = FALSE)
source(glue::glue("{config$dir$lib}/Task.R"), echo = FALSE)
source(glue::glue("{config$dir$lib}/Helpers.R"), echo = FALSE)

# b. Carga de codigo para ajuste de distribuciones, calculo de indices y ejecucion distribuida
source(glue::glue("{config$dir$base}/lib/funciones_eventos.R"), echo = FALSE)

# c) Chequear que no este corriendo el script de índices de sequia.
#    Si esta corriendo, la ejecucion debe cancelarse.
script.indices.sequia <- Script$new(run.dir = config$dir$indices.sequia$run,
                                    name = "IndicesSequia")
script.indices.sequia$assertNotRunning()
rm(script.indices.sequia)

# d) Iniciar script y obtener fecha de ejecucion
script <- Script$new(run.dir = config$dir$run,
                     name = "IdentificarEventos")
script$start()

# e) Buscar los resultados del cálculo de índices de sequía
script$info("Buscando resultados del cálculo de índices de sequía")
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$resultados}")
resultados.indices.sequia <- feather::read_feather(archivo); rm(archivo)

# f) Obtener configuraciones para el cálculo de los indices de sequía
script$info("Buscando configuraciones para los índices a ser calculados")
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$configuraciones}")
configuraciones.indices <- feather::read_feather(archivo); rm(archivo)
script$info("Seleccionando configuraciones contempladas al calcular los índices de sequía")
configuraciones.indices <- configuraciones.indices %>%
  dplyr::filter(escala %in% resultados.indices.sequia$escala)

# g) Verificar que hayan configuraciones para todas las escalas en resultados.indices.sequia
configuraciones.indices <- configuraciones.indices %>%
  dplyr::group_by(indice, distribucion, metodo_ajuste) %>%
  dplyr::group_walk(.f = function(g, k) {
    if (!all(resultados.indices.sequia$escala %in% g$escala)) {
      stop_msg <- glue::glue("Algunas de las escalas definidas en el archivo parametros_calculador.indices.yml ",
                             "no están presentes para la configuración con indice:{k$indice}, distribucion:",
                             "{k$distribucion} y metodo_ajuste:{k$metodo_ajuste}!!")
      stop(stop_msg)
    }
  })

# h) Buscar ubicaciones a las cuales se aplicara el calculo de indices de sequia
# h.1) Obtener datos producidos por el generador y filtrarlos
script$info("Leyendo netcdf con datos de entrada")
netcdf_filename <- glue::glue("{config$dir$data}/{config$files$clima_generado}")
points_filename <- glue::glue("{config$dir$data}/{config$files$puntos_a_extraer}")
if (is.null(config$files$puntos_a_extraer))
  datos_climaticos_generados <- gamwgen::netcdf.as.sf(netcdf_filename, add.id = T)
if (!is.null(config$files$puntos_a_extraer))
  datos_climaticos_generados <- gamwgen::netcdf.extract.points.as.sf(netcdf_filename, readRDS(points_filename))
script$info("Lectura del netcdf finalizada")
# h.x) Reducción de trabajo (solo para pruebas)
datos_climaticos_generados <- datos_climaticos_generados %>%
  dplyr::filter( realization %in% c(1, 2), dplyr::between(date, as.Date('1981-01-01'), as.Date('2010-12-31')) )
# h.2) Generar tibble con ubicaciones sobre las cuales iterar
script$info("Obtener ubicaciones sobre las cuales iterar")
ubicaciones_a_procesar <- datos_climaticos_generados %>%
  dplyr::select(dplyr::ends_with("_id"), longitude, latitude) %>%
  sf::st_transform(crs = sf::st_crs(4326)) %>%
  dplyr::mutate(lon_dec = sf::st_coordinates(geometry)[,'X'],
                lat_dec = sf::st_coordinates(geometry)[,'Y']) %>%
  sf::st_drop_geometry() %>% tibble::as_tibble() %>% dplyr::distinct()
script$info("Obtención finalizada")

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Calcular indices de sequia ----
# -----------------------------------------------------------------------------#

# Crear tarea distribuida y ejecutarla
task.identificar.eventos <- Task$new(parent.script = script,
                                     func.name = "IdentificarEventos",
                                     packages = list.of.packages)

# Ejecutar tarea distribuida
script$info("Identificando Eventos")
cantidad.de.realizaciones <- datos_climaticos_generados %>% dplyr::pull(realization) %>% base::unique()
resultados.identificar.eventos <- task.identificar.eventos$run(number.of.processes = config$max.procesos, 
                                                               config = config, input.values = ubicaciones_a_procesar,
                                                               configuraciones.indices, resultados.indices.sequia,
                                                               numero.realizaciones = cantidad.de.realizaciones)

# Transformar resultados a un objeto de tipo tibble
resultados.identificar.eventos.tibble <- resultados.identificar.eventos %>% purrr::map_dfr(~.x)

# Guardar resultados en un archivo fácil de compartir
feather::write_feather(resultados.identificar.eventos.tibble, 
                       glue::glue("{config$dir$data}/{config$files$identificar_eventos$resultados}"))

# Si hay errores, terminar ejecucion
task.identificar.eventos.errors <- task.identificar.eventos$getErrors()
if (length(task.identificar.eventos.errors) > 0) {
  for (error.obj in task.identificar.eventos.errors) {
    id_column <- IdentificarIdColumn(ubicaciones_a_procesar %>% dplyr::top_n(1))
    script$warn(glue::glue("({id_column}={error.obj$input.value[[id_column]]}): {error.obj$error}"))
  }
  script$error("Finalizando script de forma ANORMAL")
}
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 6. Finalizar script cerrando conexion a base de datos ----
# -----------------------------------------------------------------------------#

# a) Finalizar script
script$stop()
# ------------------------------------------------------------------------------
