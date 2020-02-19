# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("dplyr", "magrittr", "glue", "purrr",
                      "stringr", "yaml", "feather")
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

# a) YAML de configuracion del generador de config para los índices de sequía
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_generador_configuraciones.yml")
}
if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuración ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuración ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
  for (nm in names(config$dir)) # Normalize dir paths
    config$dir[nm] <- base::sub('/$', '', config$dir[[nm]])
}

# b) YAML de parametros de la generación de configuraciones
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parametros_generador_configuraciones.yml")
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

rm(archivo.config, archivo.nombres, args, nm); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 3. Generar configuraciones ----
# -----------------------------------------------------------------------------#
configuraciones <- purrr::map_dfr(
  .x = names(config$params$indices),
  .f = function(indice, config) {
    indice.data <- config$params$indices[[indice]]
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
  config = config
) %>%
  dplyr::mutate(referencia_comienzo = sprintf("%s-01-01", referencia_ano_desde)) %>%
  dplyr::mutate(referencia_fin = sprintf("%s-12-31", referencia_ano_hasta)) %>%
  dplyr::mutate(id = dplyr::row_number()) %>%
  dplyr::select(id, indice, escala, distribucion, metodo_ajuste, referencia_comienzo, referencia_fin)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Guardar datos y salir ----
# -----------------------------------------------------------------------------#
# a) Guardar datos
filename <- glue::glue("{config$dir$data}/{config$files$indices_sequia$configuraciones}")
feather::write_feather(configuraciones, filename)
# ------------------------------------------------------------------------------
