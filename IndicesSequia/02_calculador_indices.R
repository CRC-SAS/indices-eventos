# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("ADGofTest", "caret", "dplyr", "fitdistrplus", "lmomco", "logspline",
                      "lubridate", "magrittr", "mgcv", "purrr", "SCI", "sirad", "SPEI", 
                      "stats", "stringr", "utils", "WRS2", "yaml", "yardstick", "feather",
                      "R6", "futile.logger", "mgcv", "doSNOW", "foreach", "snow", "parallel",
                      "RPostgres")
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

# a) YAML de configuracion del cálculo de índices de sequia
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_calculador_indices.yml")
}
if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuración ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuración ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
  config$dir <- normalize_dirnames(config$dir)
}

# b) YAML de parametros del cálculo de índices de sequia
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parametros_calculador_indices.yml")
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
source(glue::glue("{config$dir$base}/lib/funciones_ajuste.R"), echo = FALSE)
source(glue::glue("{config$dir$base}/lib/funciones_calculo.R"), echo = FALSE)
source(glue::glue("{config$dir$base}/lib/funciones_test_ajuste.R"), echo = FALSE)
source(glue::glue("{config$dir$base}/lib/funciones_worker.R"), echo = FALSE)

# c) Chequear que no este corriendo el script de estadisticas.
#    Si esta corriendo, la ejecucion debe cancelarse.
script.estadisticas <- Script$new(run.dir = config$dir$estadisticas$run,
                                   name = "EstadisticaMovil")
script.estadisticas$assertNotRunning()
rm(script.estadisticas)

# d) Iniciar script y obtener fecha de ejecucion
script <- Script$new(run.dir = config$dir$run,
                     name = "IndicesGenerador")
script$start()

# e) Obtener configuraciones para el cálculo de los indices de sequía
script$info("Buscando configuraciones para los índices a ser calculados")
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$configuraciones}")
configuraciones.indice <- feather::read_feather(archivo)
rm(archivo)

# f) Buscar las estadisticas moviles 
script$info("Buscando estadísticas móviles para calcular indices de sequia")
archivo <- glue::glue("{config$dir$data}/{config$files$estadisticas_moviles$resultados}")
estadisticas.moviles <- feather::read_feather(archivo)
rm(archivo)

# g) Buscar ubicaciones a las cuales se aplicara el calculo de indices de sequia
script$info("Obtener ubicaciones para el cálculo de los índices de sequia")
points_filename <- glue::glue("{config$dir$data}/{config$files$puntos_a_extraer}")
ubicaciones_a_procesar <- readRDS(points_filename) %>%
  dplyr::select(dplyr::ends_with("_id"), longitude, latitude) %>%
  sf::st_drop_geometry() %>% tibble::as_tibble() %>% dplyr::distinct()
script$info("Obtención finalizada")

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Calcular indices de sequia ----
# -----------------------------------------------------------------------------#

# Crear tarea distribuida y ejecutarla
task.indices.sequia <- Task$new(parent.script = script,
                                func.name = "CalcularIndicesSequiaUbicacion",
                                packages = list.of.packages)

# Ejecutar tarea distribuida
script$info("Calculando indices de sequia")
resultados.indices.sequia <- task.indices.sequia$run(number.of.processes = config$max.procesos, 
                                                     config = config, input.values = ubicaciones_a_procesar, 
                                                     configuraciones.indice, estadisticas.moviles)

# Transformar resultados a un objeto de tipo tibble
resultados.indices.sequia.tibble <- resultados.indices.sequia %>% purrr::map_dfr(~.x)

# Guardar resultados en un archivo fácil de compartir
feather::write_feather(resultados.indices.sequia.tibble, 
                       glue::glue("{config$dir$data}/config$files$indices_sequia$resultados"))

# Si hay errores, terminar ejecucion
task.indices.sequia.errors <- task.indices.sequia$getErrors()
if (length(task.indices.sequia.errors) > 0) {
  for (error.obj in task.indices.sequia.errors) {
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
