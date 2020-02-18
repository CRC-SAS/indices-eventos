require(R6)
require(dplyr)
require(dbplyr)

IndiceSequiaFacade <- R6Class("IndiceSequiaFacade",
	inherit = Facade,
	private = list(
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
	  },
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
	),
	public = list(
	  Indice = list("SPI" = "SPI", "SPEI" = "SPEI", "CategoriaINMET" = "CategoriaINMET", "Decil" = "Decil", "PPN" = "PPN"),
	  
	  initialize = function(con) {
			private$con <- con
		},
  
		buscarConfiguraciones = function(indice = NULL, escala = NULL, distribucion = NULL, metodo_ajuste = NULL,
		                                 referencia_comienzo = NULL, referencia_fin = NULL, procesable = 1) {
		  filtros         <- list(indice = indice, escala = escala, distribucion = distribucion, metodo_ajuste = metodo_ajuste,
		                          referencia_comienzo = referencia_comienzo, referencia_fin = referencia_fin, procesable = procesable)
		  configuraciones <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'indice_configuracion')) , filtros)
		  
		  # Ordenar por indice, escala, distribucion, metodo de ajuste, periodo de referencia (comienzo/fin)
		  # Recolectar datos.
		  configuraciones %<>% 
		    dplyr::arrange(indice, escala, distribucion, metodo_ajuste, referencia_comienzo, referencia_fin) %>%
		    dplyr::collect()
		  
		  return (configuraciones)
		},
		
		buscarParametros = function(indice_configuracion_id = NULL, omm_id = NULL, pentada_fin = NULL, mes = NULL, 
		                            parametro = NULL, metodo_imputacion_id = NULL) {
		  filtros         <- list(indice_configuracion_id = indice_configuracion_id, omm_id = omm_id,
		                          parametro = parametro, metodo_imputacion_id = metodo_imputacion_id)
		  if (! is.null(pentada_fin)) {
		    filtros$pentada_fin <- pentada_fin
		  } else if (! is.null(mes)) {
		    filtros$pentada_fin <- 6 * mes
		  }
		  parametros <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'indice_parametro')) , filtros)
		  
		  return (dplyr::collect(parametros))
		},
		
		buscarResultadosTests = function(indice_configuracion_id = NULL, omm_id = NULL, pentada_fin = NULL, mes = NULL, 
		                                 test = NULL, parametro = NULL, metodo_imputacion_id = NULL) {
		  filtros         <- list(indice_configuracion_id = indice_configuracion_id, omm_id = omm_id,
		                          test = test, parametro = parametro, metodo_imputacion_id = metodo_imputacion_id)
		  if (! is.null(pentada_fin)) {
		    filtros$pentada_fin <- pentada_fin
		  } else if (! is.null(mes)) {
		    filtros$pentada_fin <- 6 * mes
		  }
		  resultados <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'indice_resultado_test')) , filtros)
		  
		  return (dplyr::collect(resultados))
		},
		
		buscarValores = function(indice_configuracion_id = NULL, omm_id = NULL, pentada_fin = NULL, mes = NULL, ano = NULL, 
		                         metodo_imputacion_id = NULL, fecha_desde = NULL, fecha_hasta = NULL) {
		  filtros         <- list(indice_configuracion_id = indice_configuracion_id, omm_id = omm_id,
		                          ano = ano, metodo_imputacion_id = metodo_imputacion_id)
		  if (! is.null(pentada_fin)) {
		    filtros$pentada_fin <- pentada_fin
		  } else if (! is.null(mes)) {
		    filtros$pentada_fin <- 6 * mes
		  }
		  valores <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'indice_valor')) , filtros)
		  
		  # Ordenar por omm_id, ano, pentada de fin, y metodo de imputacion
		  # Recolectar datos.
		  valores %<>% 
		    dplyr::arrange(omm_id, ano, pentada_fin, metodo_imputacion_id) %>%
		    dplyr::collect()
		  
		  # Se se especifico fecha de inicio y/o fin, convertir a pentada/ano y filtrar de la siguiente manera
		  # ano_pentada_fin_desde <= ano_pentada_fin <= ano_pentada_fin_hasta
		  if (! is.null(fecha_desde)) {
		    pentada.fin.desde <- fecha.a.pentada.ano(fecha_desde)
		    ano.desde         <- lubridate::year(fecha_desde)
		    valores           %<>%
		      dplyr::filter((ano == ano.desde & pentada_fin >= pentada.fin.desde) |
		                    (ano > ano.desde))
		  }
		  if (! is.null(fecha_hasta)) {
		    pentada.fin.hasta <- fecha.a.pentada.ano(fecha_hasta)
		    ano.hasta         <- lubridate::year(fecha_hasta)
		    valores           %<>%
		      dplyr::filter((ano == ano.hasta & pentada_fin <= pentada.fin.hasta) |
		                      (ano < ano.hasta))
		  }
		  
		  return (valores)
		},
		
		primerAnoPentadaFin = function(indice_configuracion_id, omm_id = NULL, metodo_imputacion_id = NULL) {
		  # Construir query
		  query      <- "select ano, pentada_fin from indice_valor where indice_configuracion_id = $1"
		  parametros <- list(indice_configuracion_id)
		  if (! is.null(omm_id)) {
		    omm.id <- paste0(omm_id, collapse = ", ")
		    query  <- glue::glue("{query} and omm_id in ({omm.id})")
		  }
		  if (! is.null(metodo_imputacion_id)) {
		    query <- glue::glue("{query} and metodo_imputacion_id = {metodo_imputacion_id}")
		  }
		  query <- glue::glue("{query} order by ano, pentada_fin limit 1")
		  
		  # Ejecutar query
		  periodos <- self$executeSelectStatement(query = query, parameters = parametros)
		  if (nrow(periodos) > 0) {
		    return (periodos[1,])
		  } else {
		    return (NA)
		  }
		},
		
		ultimoAnoPentadaFin = function(indice_configuracion_id, omm_id = NULL, metodo_imputacion_id = NULL) {
		  # Construir query
		  query      <- "select ano, pentada_fin from indice_valor where indice_configuracion_id = $1"
		  parametros <- list(indice_configuracion_id)
		  if (! is.null(omm_id)) {
		    omm.id <- paste0(omm_id, collapse = ", ")
		    query  <- glue::glue("{query} and omm_id in ({omm.id})")
		  }
		  if (! is.null(metodo_imputacion_id)) {
		    query <- glue::glue("{query} and metodo_imputacion_id = {metodo_imputacion_id}")
		  }
		  query <- glue::glue("{query} order by ano desc, pentada_fin desc limit 1")
		  
		  # Ejecutar query
		  periodos <- self$executeSelectStatement(query = query, parameters = parametros)
		  if (nrow(periodos) > 0) {
		    return (periodos[1,])
		  } else {
		    return (NA)
		  }
		},
		
		ultimoAnoPentadaFinEstacion = function(omm_id) {
		  query    <- "select ano, pentada_fin from indice_valor where omm_id = $1 order by ano desc, pentada_fin desc limit 1"
		  params   <- list(omm_id)
		  periodos <- self$executeSelectStatement(query = query, parameters = params)
		  if (nrow(periodos) > 0) {
		    return (periodos[1,])
		  } else {
		    return (NA)
		  }
		},
		
		rangoDatosConfiguracion = function(indice_configuracion_id, metodo_imputacion_id = 0) {
		  # Validacion de parametros
		  base::stopifnot(is.numeric(indice_configuracion_id) && (length(indice_configuracion_id) > 1))
		  base::stopifnot(is.numeric(metodo_imputacion_id) && (length(metodo_imputacion_id) == 1))
		  
		  # Definicion de queries
		  queries <- list(
		    minimo = paste0("select ano, pentada_fin from indice_valor where metodo_imputacion_id = ", metodo_imputacion_id, " and indice_configuracion_id in (", paste0(indice_configuracion_id, collapse = ", "), ") order by ano, pentada_fin limit 1"),
		    maximo = paste0("select ano, pentada_fin from indice_valor where metodo_imputacion_id = ", metodo_imputacion_id, " and indice_configuracion_id in (", paste0(indice_configuracion_id, collapse = ", "), ") order by ano desc , pentada_fin desc limit 1")
		  )

		  # Ejecucion de queries
      rango <- purrr::map(
        .x = names(queries),
        .f = function(tipo) {
          query <- queries[[tipo]]
          res.query <- DBI::dbSendQuery(private$con, query)
          return (DBI::dbFetch(res.query, n = 1))
        }
      )
      names(rango) <- names(queries)
      return (rango)
		},
		
		actualizarResultadoTestsAjusteEstacion = function(resultado.tests) {
		  # a. Iniciar transaccion
		  DBI::dbBegin(private$con)
		  
		  tryCatch({
		    # b. Bloquear tabla
		    DBI::dbSendQuery(private$con, "LOCK TABLE indice_resultado_test IN EXCLUSIVE MODE")
		    
		    # c. Insertar parametros de ajuste
		    query <- paste0("INSERT INTO indice_resultado_test (indice_configuracion_id, omm_id, pentada_fin, test, parametro, metodo_imputacion_id, valor) ",
		                    "VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (indice_configuracion_id, omm_id, pentada_fin, test, parametro, metodo_imputacion_id) ",
		                    "DO UPDATE SET valor = EXCLUDED.valor")
		    for (seq_index in seq(from = 1, to = nrow(resultado.tests))) {
		      fila <- resultado.tests[seq_index,]
		      res  <- DBI::dbSendStatement(private$con, query)
		      DBI::dbBind(res, unname(as.list(fila)))
		      DBI::dbClearResult(res)
		    }
		    
		    # d. Finalizar transaccion
		    DBI::dbCommit(private$con)
		  }, error = function(e) {
		    # Error en la transaccion. Hacer rollback y luego mostrar el error.
		    DBI::dbRollback(conn = private$con)
		    stop(e)
		  })
		},
		
		actualizarValoresParametrosEstacion = function(parametros.ajuste) {
		  # a. Iniciar transaccion
		  DBI::dbBegin(private$con)
		  
		  tryCatch({
		    # b. Bloquear tabla
		    DBI::dbSendQuery(private$con, "LOCK TABLE indice_parametro IN EXCLUSIVE MODE")
		    
  		  # c. Insertar parametros de ajuste
  		  query <- paste0("INSERT INTO indice_parametro (indice_configuracion_id, omm_id, pentada_fin, parametro, metodo_imputacion_id, valor) ",
  		                  "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (indice_configuracion_id, omm_id, pentada_fin, parametro, metodo_imputacion_id) ",
  		                  "DO UPDATE SET valor = EXCLUDED.valor")
  		  for (seq_index in seq(from = 1, to = nrow(parametros.ajuste))) {
  		    fila <- parametros.ajuste[seq_index,]
  		    res  <- DBI::dbSendStatement(private$con, query)
  		    DBI::dbBind(res, unname(as.list(fila)))
  		    DBI::dbClearResult(res)
  		  }
  		  
  		  # d. Finalizar transaccion
  		  DBI::dbCommit(private$con)
		  }, error = function(e) {
		    # Error en la transaccion. Hacer rollback y luego mostrar el error.
		    DBI::dbRollback(conn = private$con)
		    stop(e)
		  })
		},
		
		actualizarValoresIndiceEstacion = function(valores) {
		  # a. Iniciar transaccion
		  DBI::dbBegin(private$con)
		  
		  tryCatch({
		    # b. Bloquear tabla
		    DBI::dbSendQuery(private$con, "LOCK TABLE indice_valor IN EXCLUSIVE MODE")
		    
		    # c. Insertar valores de indices
		    query <- paste0("INSERT INTO indice_valor (indice_configuracion_id, omm_id, pentada_fin, ano, metodo_imputacion_id, valor_dato, valor_indice, percentil_dato) ",
		                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (indice_configuracion_id, omm_id, pentada_fin, ano, metodo_imputacion_id) ",
		                    "DO UPDATE SET valor_dato = EXCLUDED.valor_dato, valor_indice = EXCLUDED.valor_indice, percentil_dato = EXCLUDED.percentil_dato")
		    for (seq_index in seq(from = 1, to = nrow(valores))) {
		      fila <- valores[seq_index,]
		      res  <- DBI::dbSendStatement(private$con, query)
		      DBI::dbBind(res, unname(as.list(fila)))
		      DBI::dbClearResult(res)
		    }
		    
		    # d. Finalizar transaccion
		    DBI::dbCommit(private$con)
		  }, error = function(e) {
		    # Error en la transaccion. Hacer rollback y luego mostrar el error.
		    DBI::dbRollback(conn = private$con)
		    stop(e)
		  })
		},
		
		identificarEventos = function(indice_configuracion_id, omm_id, tipo_evento = c("seco", "humedo"), 
		                              umbral_indice, duracion_minima, interpolar_aislados = TRUE, metodo_imputacion_id = 0) {
		  # 0. Validar tipo de evento
		  base::match.arg(tipo_evento)
		  
		  # 1. Buscar valores de indices para la estacion. Si hay valores faltantes (NA) aislados
		  #    (un NA con un predecesor y un sucesor no NA), interpolar linealmente para evitar "perder" eventos.
		  eventos         <- NULL
		  valores.indices <- self$buscarValores(omm_id = omm_id, metodo_imputacion_id = metodo_imputacion_id,
		                                        indice_configuracion_id = indice_configuracion_id)
		  if (nrow(valores.indices) > 0) {
		    valores.indices <- valores.indices %>%
		      dplyr::arrange(omm_id, ano, pentada_fin)
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
		        dplyr::select(omm_id, ano, pentada_fin, valor_indice_ajustado)  
		    } else {
		      eventos <- valores.indices %>%
		        dplyr::mutate(valor_indice_ajustado = valor_indice) %>%
		        dplyr::select(omm_id, ano, pentada_fin, valor_indice_ajustado)  
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
  		      dplyr::mutate(duracion_evento = private$transformarALongitudesCadenas(cumple_condicion)) %>%
  		      dplyr::filter(cumple_condicion & (duracion_evento >= duracion_minima))
  		    if (nrow(eventos) > 0) {
  		      eventos <- eventos %>%
    		      dplyr::mutate(numero_evento = private$identificarLongitudesCadenas(duracion_evento)) %>%
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
		      dplyr::group_by(omm_id, numero_evento) %>%
		      dplyr::summarise(fecha_inicio = min(fecha_inicio_pentada), fecha_fin = max(fecha_fin_pentada),
		                       intensidad = mean(valor_indice_ajustado), magnitud = sum(valor_indice_ajustado), 
		                       duracion = min(duracion_evento), minimo = min(valor_indice_ajustado), 
		                       maximo = max(valor_indice_ajustado))
		  }
		  
		  return (eventos)
		},
		
		identificarEventosActuales = function(indice_configuracion_id, tipo_evento = c("seco", "humedo"), umbral_indice, 
		                                     duracion_maxima, interpolar_aislados = TRUE, metodo_imputacion_id = 0,
		                                     agregar.ultimos.datos = FALSE) {
		  # 0. Validar tipo de evento
		  base::match.arg(tipo_evento)
		  
		  # 1. Definir extension de pentadas (agrego una pentada mas por si hay que interpolar)
		  fecha.actual                              <- Sys.Date()
		  fecha.inicio.pentada.actual               <- fecha.inicio.pentada(fecha.actual)
		  fecha.inicio.pentada.limite               <- sumar.pentadas(fecha.inicio.pentada.actual, -duracion_maxima)
		  fecha.inicio.pentada.limite.interpolacion <- sumar.pentadas(fecha.inicio.pentada.limite, -1)
		  
		  # 2. Buscar valores de indices para la configuracion seleccionada y rango de fechas.
		  valores.indices <- self$buscarValores(metodo_imputacion_id = metodo_imputacion_id,
		                                        indice_configuracion_id = indice_configuracion_id) %>%
		    dplyr::mutate(fecha_inicio_pentada_fin = pentada.ano.a.fecha.inicio(pentada_fin, ano)) %>%
		    dplyr::filter(fecha_inicio_pentada_fin >= fecha.inicio.pentada.limite.interpolacion)
		  
		  # 3. Eliminar estaciones cuyo ultimo valor de indice sea NA o no cumpla la condicion del umbral
		  estaciones.totales <- valores.indices %>%
		    dplyr::distinct(omm_id)
		  valores.indices.estaciones.candidatas <- purrr::pmap_dfr(
		    .l = estaciones.totales,
		    .f = function(omm_id) {
		      omm.id                  <- omm_id
		      valores.indice.estacion <- valores.indices %>%
		        dplyr::filter(omm_id == omm.id) %>%
		        dplyr::arrange(desc(fecha_inicio_pentada_fin))
		      
		      valores      <- dplyr::pull(valores.indice.estacion, valor_indice)
		      es.candidata <- FALSE
		      if ((length(valores) > 0) && ! is.na(valores[1])) {
		        es.candidata <- ifelse(tipo_evento == "seco", valores[1] <= umbral_indice, valores[1] >= umbral_indice)
		      }
		      if (es.candidata) {
		        return (valores.indice.estacion)
		      } else {
		        return (NULL)
		      }
		    }
		  )
		  
		  # 4. Generar eventos por estacion
		  if (nrow(valores.indices.estaciones.candidatas) > 0) {
		    eventos <- purrr::pmap_dfr(
		      .l <- dplyr::distinct(valores.indices.estaciones.candidatas, omm_id),
		      .f <- function(omm_id) {
		        # a. Filtrar valores de indice para estacion
		        omm.id                  <- omm_id
		        valores.indice.estacion <- valores.indices %>%
		          dplyr::filter(omm_id == omm.id) %>%
		          dplyr::arrange(fecha_inicio_pentada_fin)  
		        
		        # b. Interpolar si es aplicable
		        if (interpolar_aislados) {
		          eventos.estacion <- valores.indice.estacion %>%
		            dplyr::mutate(valor_indice_anterior = dplyr::lag(valor_indice),
		                          valor_indice_siguiente = dplyr::lead(valor_indice)) %>%
		            dplyr::mutate(interpolar = (is.na(valor_indice) && ! is.na(valor_indice_anterior) && ! is.na(valor_indice_siguiente))) %>%
		            dplyr::mutate(valor_indice_ajustado = dplyr::if_else(interpolar, (valor_indice_anterior + valor_indice_siguiente) / 2, valor_indice)) %>%
		            dplyr::select(omm_id, ano, pentada_fin, fecha_inicio_pentada_fin, valor_indice_ajustado)
		        } else {
		          eventos.estacion <- valores.indice.estacion %>%
		            dplyr::mutate(valor_indice_ajustado = valor_indice) %>%
		            dplyr::select(omm_id, ano, pentada_fin, fecha_inicio_pentada_fin, valor_indice_ajustado)
		        }
		        
		        # c. Fijar maximo de pentadas y ordener en formar ascendente
		        eventos.estacion <- eventos.estacion %>%
		          dplyr::arrange(fecha_inicio_pentada_fin) %>%
		          dplyr::filter(fecha_inicio_pentada_fin >= fecha.inicio.pentada.limite)
		        
		        # d. Determinar en que pentadas se cumple de condicion respecto al umbral.
		        if (tipo_evento == "seco") {
		          eventos.estacion <- eventos.estacion %>%
		            dplyr::mutate(cumple_condicion = ! is.na(valor_indice_ajustado) & 
		                            ! is.nan(valor_indice_ajustado) &
		                            valor_indice_ajustado <= umbral_indice)
		        } else {
		          eventos.estacion <- eventos.estacion %>%
		            dplyr::mutate(cumple_condicion = ! is.na(valor_indice_ajustado) & 
		                            ! is.nan(valor_indice_ajustado) &
		                            valor_indice_ajustado >= umbral_indice)
		        }
		        
		        # e. Identificar la ultima pentada donde no se cumple la condicion. 
		        #    Seleccionar desde la pentada siguiente en adelante
		        condiciones <- dplyr::pull(eventos.estacion, cumple_condicion)
		        no.cumplen  <- which(! condiciones)
		        if (length(no.cumplen) > 0) {
		          posicion.inicio  <- max(no.cumplen) + 1
		          posicion.fin     <- nrow(eventos.estacion)
		          eventos.estacion <- eventos.estacion[seq(posicion.inicio, posicion.fin), ]
		        }
		        
		        # f. Calcular metricas de evento
		        evento.estacion <- eventos.estacion %>%
		          dplyr::rename(fecha_inicio_pentada = fecha_inicio_pentada_fin) %>%
		          dplyr::mutate(fecha_fin_pentada = fecha.fin.pentada(fecha_inicio_pentada)) %>%
		          dplyr::group_by(omm_id) %>%
		          dplyr::summarise(fecha_inicio = min(fecha_inicio_pentada), fecha_fin = max(fecha_fin_pentada),
		                           intensidad = mean(valor_indice_ajustado), magnitud = sum(valor_indice_ajustado), 
		                           duracion = n(), minimo = min(valor_indice_ajustado), 
		                           maximo = max(valor_indice_ajustado))
		        return (evento.estacion)
		      }
		    )
		    
		    # g. Agregar ultimos datos de ser necesario
		    if (agregar.ultimos.datos) {
		      ultimos.indices <- valores.indices %>%
		        dplyr::group_by(omm_id) %>%
		        dplyr::top_n(n = 1, wt = fecha_inicio_pentada_fin) %>%
		        dplyr::filter(! is.na(valor_indice)) %>%
		        dplyr::mutate(ultima_fecha_datos = fecha.fin.pentada(fecha_inicio_pentada_fin)) %>%
		        dplyr::rename(ultimo_valor_indice = valor_indice, ultimo_percentil_dato = percentil_dato) %>%
		        dplyr::select(omm_id, ultima_fecha_datos, ultimo_valor_indice, ultimo_percentil_dato)
		      
		      eventos <- eventos %>%
		        dplyr::inner_join(ultimos.indices, by = c("omm_id"))
		    }
		    
		    return (eventos)
		  } else {
		    return (NULL)
		  }
		}
	)
)