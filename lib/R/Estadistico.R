require(R6)
require(dplyr)
require(dbplyr)
require(purrr)

EstadisticoFacade <- R6Class("EstadisticoFacade",
	inherit = Facade,
	public = list(
	  Estadisticos = list(
	    'Suma', 'Media', 'Mediana', 'DesviacionEstandar', 'MAD', 'NFaltantes', 'NDisponibles', 'Ocurrencia'
	  ),
	  
		initialize = function(con) {
			private$con <- con
		},
  
		buscar = function(omm_id = NULL, variable_id = NULL, estadistico = NULL, 
		                  ancho_ventana_pentadas = NULL, fecha.desde = NULL, fecha.hasta = NULL, 
				  rango_fecha_inicio = NULL, rango_fecha_fin = NULL,
		                  metodo.imputacion.id = NULL, pentada.mes.inicio = NULL) {
		  filtros      <- list(omm_id = omm_id, variable_id = variable_id, estadistico = estadistico, 
		                       ancho_ventana_pentadas = ancho_ventana_pentadas, fecha_desde = fecha.desde, 
		                       fecha_hasta = fecha.hasta, metodo_imputacion_id = metodo.imputacion.id)
		  estadisticos <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'estadistico')) , filtros)
		  
		  # Ordenar por red_id, estacion_id, variable_id, estadistico y fecha_desde
		  # Recolectar datos.
		  estadisticos %<>% 
		    dplyr::arrange(omm_id, variable_id, estadistico, fecha_desde, ancho_ventana_pentadas) %>%
		    dplyr::collect()
		  
		  # Se indica que la fecha de inicio corresponda a una pentada del mes determinada.
		  # Mediante este parametro es posible buscar las estadisticas mensuales (pentada.mes.inicio = 1)
		  if (! is.null(pentada.mes.inicio)) {
		    estadisticos %<>% dplyr::mutate(pentada_mes = fecha.a.pentada.mes(fecha_desde)) %>%
		      dplyr::filter(pentada_mes == pentada.mes.inicio) %>%
		      dplyr::select(-pentada_mes)
		  }

		  # Rango de fechas: fecha_desde >= FECHA_INICIO and fecha_hasta <= FECHA_FIN
		  if (! is.null(rango_fecha_inicio)) {
		    estadisticos %<>% dplyr::filter(fecha_desde >= rango_fecha_inicio)
		  }
		  if (! is.null(rango_fecha_fin)) {
		    estadisticos %<>% dplyr::filter(fecha_hasta <= rango_fecha_fin)
		  }
		  
		  return (estadisticos)
		},
		
		buscarEstadisticasDiferenciasPrediccionEspacial = function(omm_id = NULL, variable_id = NULL, estadistico = NULL, mes = NULL) {
		  filtros      <- list(omm_id = omm_id, variable_id = variable_id, estadistico = estadistico, mes = mes)
		  estadisticos <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'estadistico_diferencias_predicciones_espaciales')) , filtros)
		  
		  # Recolectar datos y ordenarlos.
		  estadisticos %<>% 
		    dplyr::arrange(omm_id, mes, variable_id, estadistico) %>%
		    dplyr::collect()
		  return (estadisticos)
		},
		
		calcularNormal = function(omm_id = NULL, variable_id = NULL, 
		                          min.proporcion.datos.disponibles = 0.8,
		                          referencia.ano.desde, referencia.ano.hasta) {
		  # Calcula la normal climatologica para cualquiera de estas 3 variables: tmax, tmin, prcp
		  # En el caso de la precipitacion, interesa la precipitacion acumulada, por lo que se devuelve
		  # el promedio de precipitacion acumulada en un lapse de 6 pentadas dentro del periodo de referencia.
		  # Para el caso de las temperaturas, importa el promedio.
		  # TODO: Agregar mas documentacion de la OMM
		  
		  # Validar datos de entrada
		  base::stopifnot(is.null(omm_id) || is.numeric(omm_id))
		  base::stopifnot(is.null(variable_id) || is.character(variable_id))
		  base::stopifnot(is.numeric(referencia.ano.desde) && is.numeric(referencia.ano.hasta) &&
		                  (referencia.ano.desde < referencia.ano.hasta))
		  
		  # Definicion de periodo de referencia
		  # Ej: si elige entre 1981 y 2010, entonces se tomaran todos los datos
		  #     estadisticos cuya fecha_hasta este comprendida entre el 01-01-1981 
		  #     y el 31-12-2010
		  fecha.hasta.inicio    <- as.Date(sprintf("%d-01-01", referencia.ano.desde))
		  fecha.hasta.fin       <- as.Date(sprintf("%d-12-31", referencia.ano.hasta))
		  max.datos.disponibles <- referencia.ano.hasta - referencia.ano.desde + 1
		  
		  # Definicion de estadisticos para cada variable
		  estadisticos <- list(tmax = 'Media', tmin = 'Media', prcp = 'Suma')
		  if (is.null(variable_id)) {
		    variable_id <- c('tmax', 'tmin', 'prcp')
		  }
		  
		  # Calcular normales para cada variable
		  normales <- purrr::map_dfr(
		    .x = variable_id,
		    .f = function(var_id) {
		      # Buscar estadisticas moviles para un ancho mensual (6 pentadas)
		      normales.variable <- self$buscar(omm_id = omm_id, variable_id = var_id, 
		                                       estadistico = estadisticos[[var_id]],
		                                       ancho_ventana_pentadas = 6) %>%
		      # Seleccionar datos que esten dentro del periodo de referencia
		      dplyr::filter(fecha_hasta >= fecha.hasta.inicio & fecha_hasta <= fecha.hasta.fin) %>%
		      # Calcular pentada de fin (en base a fecha_hasta) para poder luego agrupar los datos
		      dplyr::mutate(pentada_fin = fecha.a.pentada.ano(fecha_hasta)) %>%
		      # Agrupar por estacion, variable y pentada de fin
		      dplyr::group_by(omm_id, variable_id, pentada_fin) %>%
		      # Calcular promedio y proporcion de datos disponibles por grupo
		      dplyr::summarise(normal = mean(valor, na.rm = TRUE),
		                       proporcion_datos_disponibles = length(which(! is.na(valor))) / max.datos.disponibles) %>%
		      # Si la proporcion de datos disponibles es menor a la minima, informar como NA
		      dplyr::mutate(normal = ifelse(proporcion_datos_disponibles >= min.proporcion.datos.disponibles,
		                                    normal, NA))    
		      return (normales.variable)
		    }
		  )
		  
		  return (normales)
		},
		
		primeraFechaDesde = function(omm_id = NULL) {
		  filtros      <- list(omm_id = omm_id)
		  estadisticos <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'estadistico')), filtros) %>%
		    dplyr::summarise(ultima.fecha.desde = min(fecha_desde, na.rm = TRUE)) %>%
		    dplyr::collect()
		  return (estadisticos$ultima.fecha.desde)
		},
		
		ultimaFechaDesde = function(omm_id = NULL) {
		  filtros      <- list(omm_id = omm_id)
		  estadisticos <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'estadistico')), filtros) %>%
		      dplyr::summarise(ultima.fecha.desde = max(fecha_desde, na.rm = TRUE)) %>%
		      dplyr::collect()
		  return (estadisticos$ultima.fecha.desde)
		},
		
		actualizarEstacion = function(omm_id, datos) {
		  # a. Iniciar transaccion
		  DBI::dbBegin(private$con)
		  
		  tryCatch({
		    # b. Insertar datos
   		  query <- paste0("INSERT INTO estadistico (omm_id, variable_id, estadistico, fecha_desde, fecha_hasta, metodo_imputacion_id, ancho_ventana_pentadas, valor) ",
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (omm_id, variable_id, estadistico, fecha_desde, fecha_hasta, metodo_imputacion_id) ",
                        "DO UPDATE SET ancho_ventana_pentadas = EXCLUDED.ancho_ventana_pentadas, valor = EXCLUDED.valor")
  		  for (seq_index in seq(from = 1, to = nrow(datos))) {
  	      fila <- datos[seq_index,]
  	      res  <- DBI::dbSendStatement(private$con, query)
  	      DBI::dbBind(res, unname(fila))
  	      DBI::dbClearResult(res)
  		  }
  		  
  		  # c. Finalizar transaccion
  		  DBI::dbCommit(private$con)
		  }, error = function(e) {
		    # Error en la transaccion. Hacer rollback y luego mostrar el error.
		    DBI::dbRollback(conn = private$con)
		    stop(e)
		  })
		}
	)
)
