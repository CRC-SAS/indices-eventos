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
# a) Leer YML de configuracion
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos.
  # Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_identificar_eventos.yml")
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
  archivo.params <- paste0(getwd(), "/parametros_identificar_eventos.yml")
}
if (! file.exists(archivo.params)) {
  stop(paste0("El archivo de parametros de ", archivo.params, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de parametros ", archivo.params, "...\n"))
  config$params <- yaml::yaml.load_file(archivo.params)
}

rm(archivo.config, archivo.params, args); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 3. Cargar librerias propias e iniciar script ----
# -----------------------------------------------------------------------------#

# a) Carga de clases de uso general
source(paste0(config$dir$lib, "FechaUtils.R"), echo = FALSE)
source(paste0(config$dir$lib, "Script.R"), echo = FALSE)
source(paste0(config$dir$lib, "Task.R"), echo = FALSE)
source(paste0(config$dir$lib, "IndiceSequia"), echo = FALSE)

# b. Carga de codigo para ajuste de distribuciones, calculo de indices y ejecucion distribuida
source(paste0(config$dir$base, "lib/", "funciones_eventos.R"), echo = FALSE)

# c) Chequear que no este corriendo el script de índices de sequia.
#    Si esta corriendo, la ejecucion debe cancelarse.
script.indices.sequia <- Script$new(run.dir = config$dir$indices.sequia$run,
                                    name = "IndicesSequia")
script.indices.sequia$assertNotRunning()
rm(script.indices.sequia)

# c) Iniciar script y obtener fecha de ejecucion
script <- Script$new(run.dir = config$dir$run,
                     name = "IdentificarEventos")
script$start()

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
resultados.identificar.eventos <- task.identificar.eventos$run(number.of.processes = config$max.procesos, 
                                                               config = config, input.values = list(NULL))

# Transformar resultados a un objeto de tipo tibble
resultados.identificar.eventos.tibble <- resultados.identificar.eventos %>% purrr::map_dfr(~.x)

# Guardar resultados en un archivo fácil de compartir
feather::write_feather(resultados.identificar.eventos.tibble, glue::glue("{sub('/$', '', config$dir$data)}/identificar_eventos.feather"))

# Si hay errores, terminar ejecucion
task.identificar.eventos.errors <- task.identificar.eventos$getErrors()
if (length(task.identificar.eventos.errors) > 0) {
  for (error.obj in task.identificar.eventos.errors) {
    script$warn(paste0("(point_id=", error.obj$input.value, "): ", error.obj$error))
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
