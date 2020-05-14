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

replace_run_identifier <- function(filenames, identifier) {
  if (is.atomic(filenames) && grepl("<\\*idc>", filenames)) 
    filenames <- base::sub('<\\*idc>', identifier, filenames)
  if (!is.atomic(filenames))
    for (nm in names(filenames)) 
      filenames[[nm]] <- replace_run_identifier(filenames[[nm]], identifier)
  return (filenames)
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
  config$files <- replace_run_identifier(config$files, config$files$identificador_corrida)
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


# d.1) Definir nombre del script
script_name <- "IdentificarEventos"
script_logfile <- glue::glue("{config$dir$run}/{script_name}.log")

# d.2) borrar archivo .log de corridas anteriores
if (file.exists(script_logfile))
  file.remove(script_logfile)

# d.3) Iniciar script
script <- Script$new(run.dir = config$dir$run, name = script_name, create.appender = T)
script$start()


# e) Buscar los resultados del cálculo de índices de sequía
script$info(glue::glue("Buscando resultados del cálculo de índices de sequía, ",
                       "archivo: {config$files$indices_sequia$resultados}"))
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$resultados}")
resultados.indices.sequia <- data.table::fread(archivo, nThread = config$files$avbl_cores) %>% 
  dplyr::mutate(referencia_comienzo = as.Date(referencia_comienzo), referencia_fin = as.Date(referencia_fin)) %>%
  dplyr::as_tibble()
base::remove(archivo); base::invisible(base::gc())

# f) Obtener configuraciones para el cálculo de los indices de sequía
script$info(glue::glue("Buscando configuraciones para los índices a ser calculados, ",
                       "archivo: {config$files$indices_sequia$configuraciones}"))
archivo <- glue::glue("{config$dir$data}/{config$files$indices_sequia$configuraciones}")
configuraciones.indices <- data.table::fread(archivo, nThread = config$files$avbl_cores) %>% 
  dplyr::mutate(referencia_comienzo = as.Date(referencia_comienzo), referencia_fin = as.Date(referencia_fin)) %>%
  dplyr::as_tibble()
base::remove(archivo); base::invisible(base::gc())
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
script$info("Leyendo csv con datos de entrada")
csv_filename <- glue::glue("{config$dir$data}/{config$files$clima_generado}")
datos_climaticos_generados <- data.table::fread(csv_filename, nThread = config$files$avbl_cores) %>%
  dplyr::rename(prcp = prcp_amt) %>% dplyr::select(-prcp_occ) %>% dplyr::mutate(date = as.Date(date)) %>% 
  tibble::as_tibble()
script$info("Lectura del csv finalizada")
# h.x) Reducción de trabajo (solo para pruebas)
# datos_climaticos_generados <- datos_climaticos_generados %>%
#   dplyr::filter( realization %in% c(1, 2, 3), dplyr::between(date, as.Date('1991-01-01'), as.Date('2000-12-31')) )
# h.2) Generar tibble con ubicaciones sobre las cuales iterar
script$info("Obtener ubicaciones sobre las cuales iterar")
ubicaciones_a_procesar <- datos_climaticos_generados %>%
  dplyr::select(dplyr::ends_with("_id"), longitude, latitude) %>%
  dplyr::mutate(x = longitude, y = latitude) %>%
  sf::st_as_sf(coords = c('x', 'y'), crs = sf::st_crs(22185)) %>%
  sf::st_transform(crs = sf::st_crs(4326)) %>%
  dplyr::mutate(lon_dec = sf::st_coordinates(geometry)[,'X'],
                lat_dec = sf::st_coordinates(geometry)[,'Y']) %>%
  sf::st_drop_geometry() %>% tibble::as_tibble() %>% dplyr::distinct()
script$info("Obtención de ubicaciones a iterar finalizada")

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Calcular indices de sequia de manera distriuída ----
# -----------------------------------------------------------------------------#

# Definir nombre de la función a ser distribuida y nombre de archivos .log y .out de corridas anteriores
function_name <- "IdentificarEventos"
task_logfile <- glue::glue("{config$dir$run}/{script_name}-{function_name}.log")
task_outfile <- glue::glue("{config$dir$run}/{script_name}-{function_name}.out")

# Borrar archivos .log y .out de corridas anteriores
if (file.exists(task_logfile))
  file.remove(task_logfile)
if (file.exists(task_outfile))
  file.remove(task_outfile)

# Crear tarea distribuida y ejecutarla
task.identificar.eventos <- Task$new(parent.script = script,
                                     func.name = function_name,
                                     packages = list.of.packages)

# Ejecutar tarea distribuida
script$info("Identificando Eventos")
resultados.identificar.eventos <- task.identificar.eventos$run(number.of.processes = config$max.procesos, 
                                                               config = config, input.values = ubicaciones_a_procesar,
                                                               configuraciones.indices, resultados.indices.sequia,
                                                               interpolar_aislados = TRUE)

# Agregar log de la tarea al log del script
file.append(script_logfile, task_logfile)

# Si hay errores, terminar ejecucion
task.identificar.eventos.errors <- task.identificar.eventos$getErrors()
if (length(task.identificar.eventos.errors) > 0) {
  for (error.obj in task.identificar.eventos.errors) {
    id_column <- IdentificarIdColumn(ubicaciones_a_procesar %>% dplyr::top_n(1))
    script$warn(glue::glue("({id_column}={error.obj$input.value[[id_column]]}): {error.obj$error}"))
  }
  script$error("Finalizando script de forma ANORMAL")
}

# Transformar resultados a un objeto de tipo tibble
resultados.identificar.eventos.tibble <- resultados.identificar.eventos %>% purrr::map_dfr(~.x)

# Guardar resultados en un archivo fácil de compartir
results_filename <- glue::glue("{config$dir$data}/{config$files$eventos$resultados}")
script$info(glue::glue("Guardando resultados en el archivo {results_filename}"))
data.table::fwrite(resultados.identificar.eventos.tibble, file = results_filename, nThread = config$files$avbl_cores)

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 5. Finalizar script ----
# -----------------------------------------------------------------------------#

# a) Finalizar script
script$stop()

# b) Crear archivo .info
info_filename <- glue::glue("{config$dir$data}/{config$files$eventos$info_corrida}")
if (file.exists(info_filename))
  file.remove(info_filename)
file.copy(from = script_logfile, to = info_filename)

# ------------------------------------------------------------------------------
