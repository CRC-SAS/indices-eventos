
# -----------------------------------------------------------------------------#
# --- Funciones para la identificación de eventos ----
# -----------------------------------------------------------------------------#


# Función para transformar a longitudes de cadenas
transformarALongitudesCadenas = function(x) {
  # 1. Calcular RLE para la cadena de booleanos x
  base::stopifnot(! is.null(x) && ! any(is.na(x)) && is.logical(x))
  x.rle <- base::rle(x)
  
  # 2. Transformar valores TRUE/FALSE a las longitudes de cadenas
  x.rle$values <- x.rle$lengths
  
  # 3. Invertir RLE y regenerar vector
  y <- base::inverse.rle(x.rle)
  stopifnot(length(x) == length(y))
  return (y)
}


# Función para identificar longitudes de cadenas
identificarLongitudesCadenas = function(x) {
  # 1. Calcular RLE para la cadena de duraciones x
  base::stopifnot(! is.null(x) && ! any(is.na(x)) && is.integer(x))
  x.rle <- base::rle(x)
  
  # 2. Transformar duraciones a secuencias de 1 a N
  x.rle$values <- seq(from = 1, to = length(x.rle$lengths))
  
  # 3. Invertir RLE y regenerar vector
  y <- base::inverse.rle(x.rle)
  stopifnot(length(x) == length(y))
  return (y)
}


# Función para la identificación de eventos
identificarEventosConfigUbicacion = function(indice_configuracion_id, ubicacion, tipo_evento = c("seco", "humedo"), 
                                             umbral_indice, duracion_minima, valores.indices.completos, 
                                             interpolar_aislados = TRUE, metodo_imputacion_id = 0) {
  # 0. Validar tipo de evento
  base::match.arg(tipo_evento)
  
  # 0. Identificar la columna con el id de la ubicación (usualmente station_id, o point_id)
  id_column <- IdentificarIdColumn(ubicacion)
  
  # 1. Buscar valores de indices para la estacion. Si hay valores faltantes (NA) aislados
  #    (un NA con un predecesor y un sucesor no NA), interpolar linealmente para evitar "perder" eventos.
  eventos         <- NULL
  valores.indices <- valores.indices.completos %>%
    dplyr::filter(!!rlang::sym(id_column) == dplyr::pull(ubicacion, !!id_column) & 
                    metodo_imputacion_id == !!metodo_imputacion_id & 
                    indice_configuracion_id == !!indice_configuracion_id)
  if (nrow(valores.indices) > 0) {
    valores.indices <- valores.indices %>%
      dplyr::arrange(!!rlang::sym(id_column), ano, pentada_fin)
  }
  
  # 2. Si hay valores faltantes (NA) aislados (un NA con un predecesor y un sucesor no NA), 
  #    interpolar linealmente para evitar "perder" eventos.
  if (nrow(valores.indices) > 0) {
    if (interpolar_aislados) {
      eventos <- valores.indices %>%
        dplyr::mutate(valor_indice_anterior = dplyr::lag(valor_indice),
                      valor_indice_siguiente = dplyr::lead(valor_indice)) %>%
        dplyr::mutate(interpolar = (is.na(valor_indice) && ! is.na(valor_indice_anterior) && ! is.na(valor_indice_siguiente))) %>%
        dplyr::mutate(valor_indice_ajustado = dplyr::if_else(interpolar, (valor_indice_anterior + valor_indice_siguiente) / 2, valor_indice)) %>%
        dplyr::select(!!id_column, ano, pentada_fin, valor_indice_ajustado)  
    } else {
      eventos <- valores.indices %>%
        dplyr::mutate(valor_indice_ajustado = valor_indice) %>%
        dplyr::select(!!id_column, ano, pentada_fin, valor_indice_ajustado)  
    }
  }
  
  # 3. Identificar pentadas donde se cumple que el indice esta por debajo/encima del umbral.        
  if (! is.null(eventos)) {
    if (tipo_evento == "seco") {
      eventos <- eventos %>%
        dplyr::mutate(cumple_condicion = ! is.na(valor_indice_ajustado) & 
                        ! is.nan(valor_indice_ajustado) &
                        valor_indice_ajustado <= umbral_indice)
    } else {
      eventos <- eventos %>%
        dplyr::mutate(cumple_condicion = ! is.na(valor_indice_ajustado) & 
                        ! is.nan(valor_indice_ajustado) &
                        valor_indice_ajustado >= umbral_indice)
    }
  }
  
  # 4. Calcular duracion de eventos y filtrar solamente aquellas filas que:
  #    a. Cumplen con la condicion de evento
  #    b. Cumplen con la condicion de duracion
  if (! is.null(eventos)) {
    if (all(! eventos$cumple_condicion)) {
      # Ningun evento cumple la condicion. No calcular nada mas.
      eventos <- NULL
    } else {
      eventos <- eventos %>%
        dplyr::mutate(duracion_evento = transformarALongitudesCadenas(cumple_condicion)) %>%
        dplyr::filter(cumple_condicion & (duracion_evento >= duracion_minima))
      if (nrow(eventos) > 0) {
        eventos <- eventos %>%
          dplyr::mutate(numero_evento = identificarLongitudesCadenas(duracion_evento)) %>%
          dplyr::select(-cumple_condicion)
      } else {
        # No hay eventos que cumplan con la condicion de duracion minima
        eventos <- NULL
      }
    }
  }
  
  # 5. Agregar y sumarizar:
  #     i. Fecha de comienzo de evento
  #    ii. Fecha de fin de evento
  #   iii. Intensidad de evento
  #    iv. Magnitud de evento
  #     v. Valores extremos
  if (! is.null(eventos)) {
    eventos <- eventos %>%
      dplyr::mutate(fecha_inicio_pentada = pentada.ano.a.fecha.inicio(pentada_fin, ano),
                    fecha_fin_pentada = fecha.fin.pentada(pentada.ano.a.fecha.inicio(pentada_fin, ano))) %>%
      dplyr::group_by(!!rlang::sym(id_column), numero_evento) %>%
      dplyr::summarise(fecha_inicio = min(fecha_inicio_pentada), fecha_fin = max(fecha_fin_pentada),
                       intensidad = mean(valor_indice_ajustado), magnitud = sum(valor_indice_ajustado), 
                       duracion = min(duracion_evento), minimo = min(valor_indice_ajustado), 
                       maximo = max(valor_indice_ajustado))
  }
  
  return (eventos %>% dplyr::mutate(tipo_evento = !!tipo_evento, 
                                    indice_configuracion_id = !!indice_configuracion_id))
}


# --- Función responsable de iniciar la identificación de eventos
IdentificarEventos <- function(input.value, script, config, configuraciones.indices, resultados.indices.sequia) {
  # Obtener la ubicación para la cual se calcularán los índices
  ubicacion <- input.value
  
  # Identificar la columna con el id de la ubicación (usualmente station_id, o point_id)
  id_column <- IdentificarIdColumn(ubicacion)
  
  # Informar estado de la ejecución
  script$info(glue::glue("Identificando eventos secos para la ubicación con ",
                         "{id_column} = {ubicacion %>% dplyr::pull(!!id_column)} ",
                         "(lon: {ubicacion$longitude}, lat: {ubicacion$latitude})"))
  
  eventos_secos <- purrr::pmap_dfr(
    .l = configuraciones.indices,
    .f = function(...) {
      conf_indice <- tibble::tibble(...)
      eventos <- purrr::map_dfr(
        .x = unique(resultados.indices.sequia$realizacion),
        .f = function(r) {
          eventos_x_realizacion <- 
            identificarEventosConfigUbicacion(indice_configuracion_id = conf_indice$id, 
                                              ubicacion = ubicacion, tipo_evento = "seco", 
                                              umbral_indice = config$params$eventos$secos$umbral, 
                                              duracion_minima = config$params$eventos$secos$duracion_minima, 
                                              valores.indices.completos = resultados.indices.sequia %>% 
                                                dplyr::filter(realizacion == r)) %>%
            dplyr::mutate(realizacion = r)
        })
      eventos <- eventos %>%
        dplyr::mutate(indice = conf_indice$indice, escala = conf_indice$escala, 
                      distribucion = conf_indice$distribucion, 
                      metodo_ajuste = conf_indice$metodo_ajuste)
      return (eventos %>% dplyr::select(realizacion, dplyr::everything()))
    })
  
  # Informar estado de la ejecución
  script$info(glue::glue("Identificando eventos humedos para la ubicación con ",
                         "{id_column} = {ubicacion %>% dplyr::pull(!!id_column)} ",
                         "(lon: {ubicacion$longitude}, lat: {ubicacion$latitude})"))
  
  eventos_humedos <- purrr::pmap_dfr(
    .l = configuraciones.indices,
    .f = function(...) {
      conf_indice <- tibble::tibble(...)
      eventos <- purrr::map_dfr(
        .x = unique(resultados.indices.sequia$realizacion),
        .f = function(r) {
          eventos_x_realizacion <- 
            identificarEventosConfigUbicacion(indice_configuracion_id = conf_indice$id, 
                                              ubicacion = ubicacion, tipo_evento = "humedo", 
                                              umbral_indice = config$params$eventos$humedos$umbral, 
                                              duracion_minima = config$params$eventos$humedos$duracion_minima, 
                                              valores.indices.completos = resultados.indices.sequia %>% 
                                                dplyr::filter(realizacion == r)) %>%
            dplyr::mutate(realizacion = r)
        })
      eventos <- eventos %>%
        dplyr::mutate(indice = conf_indice$indice, escala = conf_indice$escala, 
                      distribucion = conf_indice$distribucion, 
                      metodo_ajuste = conf_indice$metodo_ajuste)
      return (eventos %>% dplyr::select(realizacion, dplyr::everything()))
    })
  
  return (dplyr::bind_rows(eventos_secos, eventos_humedos))
  
}
