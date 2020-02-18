require(R6)
require(dplyr)
require(dbplyr)
require(magrittr)
require(tidyr)

RegistroDiarioFacade <- R6Class("RegistroDiarioFacade",
	inherit = Facade,
	public = list(
		initialize = function(con) {
			private$con <- con
		},

		buscar = function(omm_id = NULL, variable_id = NULL, estado.registro = NULL,
		                  fecha.desde = NULL, fecha.hasta = NULL) {
		  # a. Buscar registros por estacion y estado de registro
		  filtros.ancho   <- list(omm_id = omm_id, estado = estado.registro)
		  registros.ancho <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'estacion_registro_diario')), 
		                                     filtros.ancho)
		  
		  # b. Filtrar por fecha desde y hasta de ser necesario
		  if (! is.null(fecha.desde)) {
		    registros.ancho %<>% dplyr::filter(fecha >= fecha.desde)
		  }
		  if (! is.null(fecha.hasta)) {
		    registros.ancho %<>% dplyr::filter(fecha <= fecha.hasta)
		  }
		  registros.ancho %<>% dplyr::collect()
		  
		  # c. Pasar a tabla larga y filtrar por variable si es necesario
		  filtros.largo   <- list(variable_id = variable_id)
		  registros.largo <- tidyr::gather(data = registros.ancho, key = 'variable_id', value = valor, 
		                                   -omm_id, -fecha, -estado, -num_observaciones) %>%
		    private$filtrar(., filtros.largo)

		  # d. Ordenar por omm_id, fecha
		  registros.largo %<>% dplyr::arrange(omm_id, fecha)

		  return (dplyr::collect(registros.largo))
		},

		# buscarResultadosTests = function(red_id = NULL, estacion_id = NULL, variable_id = NULL,
		#                                  fecha.desde = NULL, fecha.hasta = NULL, resultado.test = NULL) {
		#   filtros    <- list(red_id = red_id, estacion_id = as.character(estacion_id), variable_id = variable_id,
		#                      fecha.desde = fecha.desde, fecha.hasta = fecha.hasta, resultado.test = resultado.test)
		#   resultados <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'registro_diario_test')), filtros)
		#   return (resultados)
		# },

		primeraFechaRegistro = function(omm_id = NULL, estado.registro = NULL) {
		  filtros   <- list(omm_id = omm_id, estado = estado.registro)
		  registros <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'estacion_registro_diario')), 
		                               filtros) %>%
		    dplyr::summarise(primera.fecha = min(fecha, na.rm = TRUE)) %>%
		    dplyr::collect()
		  return (registros$primera.fecha)
		},

		ultimaFechaRegistro = function(omm_id = NULL, estado.registro = NULL) {
		  filtros   <- list(omm_id = omm_id, estado = estado.registro)
		  registros <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'estacion_registro_diario')), 
		                               filtros) %>%
	      dplyr::summarise(ultima.fecha = max(fecha, na.rm = TRUE)) %>%
	      dplyr::collect()
		  return (registros$ultima.fecha)
		},

		# registros.nuevos = (omm_id, fecha, <variables>, estado, num_observaciones)
		agregarRegistrosNuevos = function(omm_id, registros.nuevos) {
		  # i. Se inicia una transaccion y se realizan las operaciones en un bloque tryCatch
		  DBI::dbBegin(conn = private$con)
		  
		  tryCatch({
		    # ii. Insertar nuevos datos en estacion_registro_diario actualizando los datos ya existentes
		    query <- paste0("INSERT INTO estacion_registro_diario(omm_id, fecha, tmax, tmin, tmed, td, pres_est, pres_nm, prcp, hr, helio, nub, vmax_d, vmax_f, vmed, estado, num_observaciones) ",
		                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (omm_id, fecha) ",
		                    "DO UPDATE SET tmax = EXCLUDED.tmax, tmin = EXCLUDED.tmin, tmed = EXCLUDED.tmed, td = EXCLUDED.td, pres_est = EXCLUDED.pres_est, pres_nm = EXCLUDED.pres_nm, ",
		                    "prcp = EXCLUDED.prcp, hr = EXCLUDED.hr, helio = EXCLUDED.helio, nub = EXCLUDED.nub, vmax_d = EXCLUDED.vmax_d, vmax_f = EXCLUDED.vmax_f, vmed = EXCLUDED.vmed, ",
		                    "estado = EXCLUDED.estado, num_observaciones = EXCLUDED.num_observaciones")
		    for (seq_index in seq(from = 1, to = nrow(registros.nuevos))) {
		      fila <- registros.nuevos[seq_index,]
		      res  <- DBI::dbSendStatement(private$con, query)
		      DBI::dbBind(res, unname(fila))
		      DBI::dbClearResult(res)
		    }
		    
		    # iii. Hacer commit
		    DBI::dbCommit(conn = private$con)
		  }, error = function(e) {
		    # Error en la transaccion. Hacer rollback y luego mostrar el error.
		    DBI::dbRollback(conn = private$con)
		    stop(e)
		  })
		},
		
		# omm_id = ID OMM de la estacion cuyo datos ser van a modificar
		# registros.pendientes = (omm_id, fecha, variable_id, valor, pasa_test.*, dictamen)
		# fecha.control = fecha de ejecucion del script
 		actualizarEstacion = function(omm_id, registros.pendientes, fecha.control) {
 		  # i. Generar registros para estacion_variable_estado
 		  #    estacion_variable_estado(omm_id, fecha, variable, valor, estado)
 		  #    No se guardan los aprobados 
 		  estacion.variable.estado <- registros.pendientes %>%
 		    dplyr::filter(dictamen != 'A') %>%
 		    dplyr::rename(estado = dictamen, variable = variable_id) %>%
 		    dplyr::select(omm_id, fecha, variable, valor, estado)
 		  
 		  # ii. Generar registros para estacion_variable_test
 		  #     estacion_variable_test = (omm_id, fecha, variable, test, valor, fecha_control) 
 		  #     Solo se guardan los tests fallidos
 		  estacion.variable.test <- registros.pendientes %>%
 		    dplyr::select(-dictamen) %>%
 		    dplyr::rename(variable = variable_id) %>%
 		    tidyr::gather(key = pasa_test, value = resultado_test, -omm_id, -fecha, -variable, -valor) %>%
 		    dplyr::filter(! is.na(resultado_test) & ! resultado_test) %>%
 		    tidyr::separate(col = pasa_test, into = c('dummy', 'test'), sep = '\\.') %>%
 		    dplyr::select(omm_id, fecha, variable, test, valor) %>%
 		    dplyr::mutate(fecha_control = fecha.control)
 		  
 		  # iii. Generar registros para estacion_registro_diario
 		  #      Las variables que figuran con estado N deben guardarse como faltantes
 		  #      Se debe generar el dictamen a nivel de fecha.
 		  #      Si hay al menos un dictamen S => el estado es D
 		  #      Sino, si hay al menos un dictamen N => el estado es R
 		  #      Sino, el estado es V
 		  fechas    <- registros.pendientes %>%
 		    dplyr::distinct(fecha) %>%
 		    dplyr::arrange(fecha) %>%
 		    dplyr::pull(fecha)
 		  variables <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'variable')) %>%
 		    dplyr::arrange(orden_columna) %>%
 		    dplyr::collect() %>%
 		    dplyr::pull(id)
 		  
 		  # Primero genero los dictamenes para cada fecha
 		  estacion.registro.diario.estado <- purrr::map_dfr(
 		    .x = fechas,
 		    .f = function(una.fecha) {
 		      estados.variable <- registros.pendientes %>%
 		        dplyr::filter(fecha == una.fecha) %>%
 		        dplyr::pull(dictamen)
 		      if (length(estados.variable) > 0) {
 		        if (any('S' == estados.variable, na.rm = TRUE)) {
 		          estado.registro <- 'D'
 		        } else if (any('N' == estados.variable, na.rm = TRUE)) {
 		          estado.registro <- 'R'
 		        } else {
 		          estado.registro <- 'V'
 		        }
 		      } else {
 		        estado.registro <- 'V'
 		      }
 		      
 		      return (data.frame(omm_id = omm_id, fecha = una.fecha, estado = estado.registro, stringsAsFactors = FALSE))
 		    }
 		  ) %>% as.data.frame()
 		  
 		  # Ahora busco las variables con estado N para reemplazar su valor por NA
 		  estacion.registro.diario.faltantes <- registros.pendientes %>%
 		    dplyr::filter(dictamen == 'N') %>%
 		    dplyr::select(omm_id, fecha, variable_id) %>%
        as.data.frame()
 		  
 		  # iv. Ya se tiene toda la informacion necesaria preparada para la actualizacion
 		  #     Se inicia una transaccion y se realizan las operaciones en un bloque tryCatch
		  DBI::dbBegin(conn = private$con)
		  
		  tryCatch({
		    # v. Insertar datos de estaciones_variable_estado
		    if (nrow(estacion.variable.estado) > 0) {
  		    DBI::dbWriteTable(conn = private$con, name = "estacion_variable_estado", 
  		                      value = estacion.variable.estado, append = TRUE, row.names = FALSE)
		    }
		    
		    # vi. Insertar datos de estaciones_variable_test
		    if (nrow(estacion.variable.test) > 0) {
		      DBI::dbWriteTable(conn = private$con, name = "estacion_variable_test", 
		                        value = estacion.variable.test, append = TRUE, row.names = FALSE)
		    }
		    
		    # vii. Actualizar estados de registros diarios
		    for (i in seq_len(nrow(estacion.registro.diario.estado))) {
		      omm_id <- estacion.registro.diario.estado[i, "omm_id"]
		      fecha  <- estacion.registro.diario.estado[i, "fecha"]
		      estado <- estacion.registro.diario.estado[i, "estado"]
		      self$executeStatement(query = "UPDATE estacion_registro_diario SET estado = $1 WHERE omm_id = $2 and fecha = $3",
		                            parameters = list(estado, omm_id, fecha))  
		    }
		    
		    # viii. Actualizar estados de registros diarios
		    for (i in seq_len(nrow(estacion.registro.diario.faltantes))) {
		      omm_id      <- estacion.registro.diario.faltantes[i, "omm_id"]
		      fecha       <- estacion.registro.diario.faltantes[i, "fecha"]
		      variable_id <- estacion.registro.diario.faltantes[i, "variable_id"]
		      self$executeStatement(query = paste0("UPDATE estacion_registro_diario SET ", variable_id, " = $1 where omm_id = $2 and fecha = $3"),
		                            parameters = list(NA, omm_id, fecha)) 
		    }

  		  # ix. Finalizar transaccion
  		  DBI::dbCommit(conn = private$con)
		  }, error = function(e) {
		    # Error en la transaccion. Hacer rollback y luego mostrar el error.
		    DBI::dbRollback(conn = private$con)
		    stop(e)
		  })
 		},
		
		cantidadDiasSecos = function(omm_id, fecha, max_dias_previos, umbral_prcp_dia_seco = 0.1) {
		  # 1. Buscar datos de precipitacion hasta desde fecha - max_dias_previos hasta fecha inclusive
		  #    Aquellos dias con precipitacion menor al umbral, marcarlos como secos
		  fecha.hasta  <- fecha
		  fecha.desde  <- fecha - lubridate::days(max_dias_previos)
		  seq.fechas   <- data.frame(fecha = seq(from = fecha.desde, to = fecha.hasta, by = 'days'))
		  diasSecosInc <- self$buscar(omm_id = omm_id, variable_id = 'prcp',
		                              fecha.desde = fecha.desde, fecha.hasta = fecha.hasta) %>%
		    dplyr::mutate(seco = (valor < umbral_prcp_dia_seco)) %>%
		    dplyr::select(omm_id, fecha, seco)
		  diasSecos    <- purrr::map_dfr(
		    .x = unique(dplyr::pull(diasSecosInc, omm_id)),
		    .f = function(omm.id) {
		      diasSecosCompletos <- diasSecosInc %>%
		        dplyr::filter(omm_id == omm.id) %>%
		        dplyr::right_join(seq.fechas, by = c("fecha")) %>%
		        dplyr::mutate(omm_id = omm.id)
		      return (diasSecosCompletos)
		    }
		  )
		  
		  # 2. Para cada estacion, contar la cantidad de dias secos
		  cantDiasSecos <- purrr::map_dfr(
		    .x = omm_id,
		    .f = function(omm.id) {
		      # Por defualt, asumo que no hay ningun dato para la estacion
		      cantidad <- NA
		      exacto   <- NA
		      
		      # Realizo la evaluacion de dias secos/humedos
		      diasSecosEstacion <- dplyr::filter(diasSecos, omm_id == omm.id)
		      if (! is.null(diasSecosEstacion) && (nrow(diasSecosEstacion) > 0)) {
		        secuencias <- base::rle(diasSecosEstacion$seco)
		        eventos    <- secuencias$values
		        duraciones <- secuencias$lengths
		        if (length(eventos) > 0) {
		          ultimo.evento.es.seco <- eventos[length(eventos)]
		          if (! is.na(ultimo.evento.es.seco)) {
		            if (ultimo.evento.es.seco) {
		              if (length(eventos) == 1) {
		                # Todos los dias fueron secos. Como se le puso un limite a la busqueda, no podemos asegurar
		                # la cantidad de dias secos que hubo. Se pone como cantidad el limite y se indica que 
		                # el valor retornado no es exacto (lo que si se puede asegurar que hay AL MENOS max_dias_previos secos)
		                cantidad <- max_dias_previos
		                exacto   <- FALSE
		              } else {
		                # Si el evento anterior fue humedo, entonces la cantidad de dias humedos es exacta.
		                # Sino (el evento anterior fue NA), entonces la cantidad de dias humedos no es exacta.
		                anterior.evento.es.seco <- eventos[length(eventos)-1]
		                cantidad                <- duraciones[length(duraciones)]
		                exacto                  <- ! is.na(anterior.evento.es.seco)
		              }
		            } else {
		              # Los ultimos dias llovio. Hay 0 dias secos
		              cantidad <- 0
		              exacto   <- TRUE
		            }
		          }
		        }
		      }
		      
		      return (data.frame(omm_id = omm.id, cantidad = cantidad, exacto = exacto))
		    }
		  )
		  return (cantDiasSecos)
		}
	)
)
