# -----------------------------------------------------------------------------#
# --- Funciones a ejecutar por parte de cada worker ----
# -----------------------------------------------------------------------------#

CalcularIndicesSequiaUbicacion <- function(input.value, script, config, configuraciones.indices, estadisticas.moviles) {
  # Obtener la ubicación para la cual se calcularán los índices
  ubicacion <- input.value
  
  # Identificar la columna con el id de la ubicación (usualmente station_id, o point_id)
  id_column <- IdentificarIdColumn(ubicacion)
  
  # Informar estado de la ejecución
  script$info(glue::glue("Procesando estadisticas para la ubicación con ",
                         "{id_column} = {ubicacion %>% dplyr::pull(!!id_column)} ",
                         "(lon: {ubicacion$longitude}, lat: {ubicacion$latitude})"))
  
  # Asignar funcion rgenlog a Global Environment (sino la ejecucion en procesos hijos no funciona bien)
  assign("rgenlog", rgenlog, .GlobalEnv)
  
  
  #############################################################################################
  ## Definición de estadisticas.variables.completas, fecha.primeros.datos y fecha.ultimos.datos
  
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
  
  rm(estadisticas.moviles.ubicacion); invisible(gc())
  
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
    dplyr::mutate(et0 = SPEI::hargreaves(Tmin = tmin, Tmax = tmax, Pre = prcp, Ra = srad, na.rm = TRUE)[,"ET0_har"]) %>%
    dplyr::select(-srad, -tmin, -tmax)
  
  fecha.primeros.datos   <- min(estadisticas.variables$fecha_desde)
  fecha.ultimos.datos    <- max(estadisticas.variables$fecha_hasta)
  
  # Se borran datos que ya no será utilizados
  rm(estadisticas.precipitacion, estadisticas.temp.max, estadisticas.temp.min); invisible(gc())
  
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
  rm(et0, pentada, estadisticas.mensuales.variables, estadisticas.variables); invisible(gc())
  
  
  #########################################################################
  ## Definición de fechas.procesables, fechas.pentada.ano y pentadas.unicas
  
  # Procesar desde la primera pentada con datos estadisticos.
  fechas.procesables <- seq.pentadas(fecha.primeros.datos, fecha.ultimos.datos)  %>%
    purrr::flatten_dbl(.) %>%
    as_date(., origin = lubridate::origin)
  
  # Determinar que fechas pertenecen a la misma pentada del ano.
  # Esas fechas tiene los mismos parametros. De este modo, optimizamos calculos.
  fechas.pentada.ano <- fecha.a.pentada.ano(fechas.procesables)
  pentadas.unicas    <- sort(unique(fechas.pentada.ano))
  
  
  ################################
  ## Inicio del cálculo de índices
  
  # Ejecutar calculo para cada configuracion.
  # Cada configuracion es una combinacion unica de indice, escala, distribucion,
  # metodo de ajuste y periodo de referencia.
  resultado <- purrr::map_dfr(
    .x = seq(from = 1, to = nrow(configuraciones.indices)),
    .f = function(row_index) {
      # Obtener configuracion de calculo
      configuracion.indice <- configuraciones.indices[row_index, ]
      
      # Informar estado de la ejecución
      script$info(glue::glue("Procesando estadisticas para la ubicación: ",
                             "{ubicacion %>% dplyr::pull(!!id_column)}, config: {configuracion.indice$id} ",
                             "(indice = {configuracion.indice$indice}, escala = {configuracion.indice$escala}, ",
                             "distribucion = {configuracion.indice$distribucion})"))
      
      # Obtener estadisticas para esa escala de tiempo
      estadisticas.variables <- estadisticas.variables.completas %>%
        dplyr::filter(ancho_ventana_pentadas == (configuracion.indice$escala * 6))
      
      #### ----------------------------------------------------------------------#
      #### Para cada fecha procesable:
      #### 1. Buscar parametros de ajuste para esta configuracion, red, ubicacion,
      ####    y pentada de fin (correspondiente a la fecha procesable)
      #### 2. Si no exiten parametros:
      ####    a. Determinar periodo de agregacion. El final del periodo esta dado
      ####       por la fecha procesable. El inicio, de acuerdo a la longitud de la escala
      ####    b. Agregar valores para el periodo de referencia. Cuando haya periodos que caigan
      ####       parte en un ano y en otro, siempre la extension es hacia el fin del periodo
      ####       de referencia asumiendo que es mas probable que haya datos mas recientes.
      ####    c. Ajustar distribucion segun parametros de configuracion. En este paso deben
      ####       considerarse la cantidad de faltantes. Si hay demasiados faltantes (de acuerdo
      ####       a unbrales definidos), entonces todos los parametros devueltos son NA.
      ####    d. Aplicar tests de bondad de ajuste (solo para el caso de ajustes parametricos
      ####       con distribucion definina). Si alguno de los tests falla, entonces considerar 
      ####       que el ajuste no es bueno. En ese caso, reemplazar todos los valores de parametros por NA.
      ####    e. Guardar parametros en base de datos. En el caso de ser un objeto de ajuste, 
      ####       no se guarda en la base de datos. Ir directamente al paso 3.
      #### 3. Una vez obtenidos los parametros de ajuste:
      ####    i. Si alguno de los parametros es NA, tanto el valor del indice como su
      ####       percentil asociado son NA. Ir directamente al paso iii.
      ####    ii. Calcular valor de indice y percentil asociado.
      ####    iii. Guardar valor de indice y percentil asociado en base de datos.
      #### ----------------------------------------------------------------------#
      
      #script$info(glue::glue("Calculando indices de sequia de la ubicación {ubicacion$point_id} ",
      #                       "para la configuración {configuracion.indice$id}"))
      
      resultados.indice.configuracion <- purrr::map_dfr(
        .x = pentadas.unicas,
        .f = function(pentada.ano) {
          # a. Buscar fechas procesables correspondientes a esa pentada
          fechas.procesables.pentada <- fechas.procesables[which(fechas.pentada.ano == pentada.ano)]
          
          # b. Tomando la primera fecha como ejemplo (daria lo mismo tomar cualquiera),
          #    obtener parametros de ajuste.
          parametros.ajuste <- AjustarParametrosUbicacionFecha(ubicacion, fechas.procesables.pentada[1], configuracion.indice,
                                                               script, config, estadisticas.variables, id_column)
          
          # c. Calcular indices para fechas correspondientes a esa pentada (los parametros son los mismos)
          resultados.indice.configuracion.pentada <- purrr::map_dfr(
            .x = fechas.procesables.pentada,
            .f = function(fecha.procesable) {
              purrr::map_dfr(
                .x = unique(estadisticas.variables$realizacion),
                .f = function(r) {
                  estadisticas.variables.realizacion <- estadisticas.variables %>% dplyr::filter(realizacion == r)
                  return (tibble::tibble(realizacion = r) %>% tidyr::crossing(
                    CalcularIndicesSequiaUbicacionFecha(ubicacion, fecha.procesable, parametros.ajuste, configuracion.indice, 
                                                        script, config,estadisticas.variables.realizacion)))
                }
              )
            }
          )
          return (resultados.indice.configuracion.pentada)
        }
      )
      
      # iii. Guardar valor de indice y percentil asociado en base de datos.
      if (nrow(resultados.indice.configuracion) > 0) {
        script$info(glue::glue("Retornando indices de sequia de la ubicación {ubicacion %>% dplyr::pull(!!id_column)} ",
                               "para la configuración {configuracion.indice$id}"))
        valores.indice <- resultados.indice.configuracion %>%
          dplyr::mutate(conf_id = configuracion.indice$id, indice = configuracion.indice$indice, 
                        escala = configuracion.indice$escala, distribucion = configuracion.indice$distribucion, 
                        metodo_ajuste = configuracion.indice$metodo_ajuste, 
                        referencia_comienzo = as.Date(configuracion.indice$referencia_comienzo), 
                        referencia_fin = as.Date(configuracion.indice$referencia_fin),
                        metodo_imputacion_id = 0, !!id_column := dplyr::pull(ubicacion, !!id_column)) %>%
          dplyr::select(!!id_column, realizacion, pentada_fin, ano, metodo_imputacion_id, conf_id, indice, escala, distribucion,
                        metodo_ajuste, referencia_comienzo, referencia_fin, valor_dato, valor_indice, percentil_dato)
        return(valores.indice)
      } else {
        type_of_id_col <- typeof(dplyr::pull(ubicacion,!!id_column))
        valores.indice <- tibble::tibble(!!id_column := if(type_of_id_col == "integer") integer() else 
                                                        if(type_of_id_col == "numeric") double() else 
                                                        if(type_of_id_col == "logical") logical() else 
                                                        if(type_of_id_col == "character") character() else
                                                          character(), 
                                         realizacion = integer(), pentada_fin = double(), ano = double(), metodo_imputacion_id = double(), 
                                         conf_id = integer(), 
                                         indice = character(), escala = integer(), distribucion = character(), 
                                         metodo_ajuste = character(), referencia_comienzo = as.Date(character()), 
                                         referencia_fin = as.Date(character()),
                                         valor_dato = double(), valor_indice = double(), percentil_dato = double())
        return(valores.indice)
      }
    }
  )
  
  
  # Leer todos los archivos con resultados de tests, crear un único tibble
  # con todos los datos en esos archivos y guardarlos en solo dos archivos
  script$info("Leyendo y borrando los archivos temporales con los resultados de los tests")
  indice_resultados_tests <- purrr::map_dfr(
    .x = seq(from = 1, to = nrow(configuraciones.indices)),
    .f = function(row_index) {
      configuracion.indice <- configuraciones.indices[row_index, ]
      resultados_tests_conf <- purrr::map_dfr(
        .x = pentadas.unicas,
        .f = function(pentada.ano) { 
          resultados_tests_conf_pent <- NULL
          pentada.fin <- fecha.a.pentada.ano(fechas.procesables[which(fechas.pentada.ano == pentada.ano)][1])
          file_name <- glue::glue("{config$dir$data}/control/resultados_tests/",
                                  "resultados_tests_{ubicacion %>% dplyr::pull(!!id_column)}_",
                                  "{configuracion.indice$id}_{pentada.fin}.feather")
          resultados_tests_conf_pent <- feather::read_feather(file_name) %>%
            dplyr::mutate_if(is.factor, as.character)
          base::file.remove(file_name); base::remove(file_name)
          return(resultados_tests_conf_pent)
        })
      return(resultados_tests_conf)
    })
  script$info(glue::glue("Generando archivo con los resultados de los tests: {config$files$indices_sequia$result_tst}"))
  filename = glue::glue("{config$dir$data}/{config$files$indices_sequia$result_tst}")
  data.table::fwrite(indice_resultados_tests, file = filename, nThread = config$files$avbl_cores)
  base::remove(filename); base::invisible(base::gc())
  
  # Leer todos los archivos con parametros de indices, crear un único tibble
  # con todos los datos en esos archivos y guardarlos en un solo archivo
  script$info("Leyendo y borrando los archivos temporales con los parámetros de índices")
  indice_parametros <- purrr::map_dfr(
    .x = seq(from = 1, to = nrow(configuraciones.indices)),
    .f = function(row_index) {
      configuracion.indice <- configuraciones.indices[row_index, ]
      parametros_conf <- purrr::map_dfr(
        .x = pentadas.unicas,
        .f = function(pentada.ano) { 
          parametros_conf_pent <- NULL
          pentada.fin <- fecha.a.pentada.ano(fechas.procesables[which(fechas.pentada.ano == pentada.ano)][1])
          file_name <- glue::glue("{config$dir$data}/control/parametros/",
                                  "parametros_{ubicacion %>% dplyr::pull(!!id_column)}_",
                                  "{configuracion.indice$id}_{pentada.fin}.feather")
          if(base::file.exists(file_name)) {
            parametros_conf_pent <- feather::read_feather(file_name) %>%
              dplyr::mutate_if(is.factor, as.character)
            base::file.remove(file_name); base::remove(file_name)
          }
          return(parametros_conf_pent)
        })
      return(parametros_conf)
    })
  script$info(glue::glue("Generando archivo con los parámetros de los índices: {config$files$indices_sequia$parametros}"))
  filename = glue::glue("{config$dir$data}/{config$files$indices_sequia$parametros}")
  data.table::fwrite(indice_parametros, file = filename, nThread = config$files$avbl_cores)
  base::remove(filename); base::invisible(base::gc())
  
  return (resultado)
}

AjustarParametrosUbicacionFecha <- function(ubicacion, fecha.procesable, configuracion.indice,
                                            script, config, estadisticas.variables, id_column) {
  # 1. Buscar parametros de ajuste para esta configuracion, ubicacion,
  #    y pentada de fin (correspondiente a la fecha procesable)
  ano.fin     <- lubridate::year(fecha.procesable)
  pentada.fin <- fecha.a.pentada.ano(fecha.procesable)
  
  script$info(glue::glue("Ajustando parametros para pentada {pentada.fin} de la ubicación ", 
                         "{ubicacion %>% dplyr::pull(!!id_column)} y la configuración {configuracion.indice$id}"))
  
  # a. Buscar valores para el periodo de referencia. Cuando el indice es SPEI,
  #    el valor buscado es (prcp - et0), sino es (prcp)
  anos.periodo.referencia <- seq(from = lubridate::year(configuracion.indice$referencia_comienzo),
                                 to = lubridate::year(configuracion.indice$referencia_fin))
  fechas.fin.pentada.fin  <- fecha.fin.pentada(pentada.ano.a.fecha.inicio(pentada.ano = pentada.fin, ano = anos.periodo.referencia))
  datos.referencia        <- estadisticas.variables %>%
    dplyr::filter(fecha_hasta %in% fechas.fin.pentada.fin)
  if (configuracion.indice$indice == "SPEI") {
    variable.acumulada <- datos.referencia %>%
      dplyr::mutate(diferencia = prcp - et0) %>%
      dplyr::filter(! is.na(diferencia)) %>% 
      dplyr::pull(diferencia)
  } else {
    variable.acumulada <- datos.referencia %>%
      dplyr::filter(! is.na(prcp)) %>% 
      dplyr::pull(prcp)
  }
  rm(datos.referencia)
  
  # c. Ajustar distribucion segun parametros de configuracion. En este paso deben
  #    considerarse la cantidad de faltantes. Si hay demasiados faltantes (de acuerdo
  #    a unbrales definidos), entonces todos los parametros devueltos son NA.
  min.valores.necesarios <- round(length(anos.periodo.referencia) * config$params$min.proporcion.disponibles.referencia)
  configuracion.ajuste   <- config$params$ajuste.general[[configuracion.indice$metodo_ajuste]]
  funcion.ajuste         <- configuracion.ajuste$funcion
  argumentos.ajuste      <- configuracion.ajuste$parametros
  argumentos.indice      <- config$params$ajuste.particular[[configuracion.indice$metodo_ajuste]]
  if (! is.null(argumentos.indice)) {
    argumentos.indice    <- argumentos.indice[[configuracion.indice$indice]]
    if (! is.null(argumentos.indice)) {
      argumentos.ajuste  <- append(argumentos.ajuste, argumentos.indice)
    }
  }
  if ("distribucion" %in% names(formals(funcion.ajuste))) {
    argumentos.ajuste$distribucion <- configuracion.indice$distribucion
  }
  if (length(variable.acumulada) >= min.valores.necesarios) {
    argumentos.ajuste$x  <- variable.acumulada
  } else {
    script$warn(paste("Verificar los resultados, los parámetros parecen no haberse generado correctamente!!",
                      "No se pudo completar 30 observaciones, probablemente la combinación de años y",
                      "realizaciones no llega a 30 observaciones!!"))
  }
  parametros.ajuste      <- do.call(what = funcion.ajuste, args = argumentos.ajuste)
  
  # d. Aplicar tests de bondad de ajuste (solo para el caso de ajustes parametricos
  #    con distribucion definina). Si alguno de los tests falla, entonces considerar 
  #    que el ajuste no es bueno. En ese caso, reemplazar todos los valores de parametros por NA.
  if (! is.na(configuracion.indice$distribucion) || 
      (configuracion.indice$metodo_ajuste %in% c("NoParametrico", "Empirico"))) {
    script$info(glue::glue("Verificando bondad de ajuste para pentada {pentada.fin} de la ubicación ", 
                           "{ubicacion %>% dplyr::pull(!!id_column)} y la configuración {configuracion.indice$id}"))
    
    parametros.test <- list(x = variable.acumulada, config = config)
    if (configuracion.indice$metodo_ajuste == "NoParametrico") {
      parametros.test$objeto.ajuste <- parametros.ajuste  
    } else if (configuracion.indice$metodo_ajuste == "Empirico") {
      parametros.test$percentiles.ajuste <- unlist(parametros.ajuste)
    } else {
      parametros.test$distribucion      <- configuracion.indice$distribucion
      parametros.test$parametros.ajuste <- parametros.ajuste
    }
    resultado.tests <- do.call(what = "TestearBondadAjuste", args = parametros.test)
    estadisticos    <- resultado.tests$estadisticos
    if (! resultado.tests$pasa.tests) {
      # Agregar los parametros originales a la tabla de resultados de tests.
      # Solamente aplicable para ajuste parametrico
      if (configuracion.indice$metodo_ajuste != "NoParametrico") {
        estadisticos <- rbind(estadisticos, ParametrosADataFrame(parametros.ajuste) %>%
                                dplyr::mutate(test = "") %>%
                                dplyr::select(test, parametro, valor)
        )
      }
      
      # Luego, poner todos los parametros originales en NA
      parametros.na        <- as.list(rep(NA, length(parametros.ajuste)))
      names(parametros.na) <- names(parametros.ajuste)
      parametros.ajuste    <- parametros.na
    }
    
    # Guardar resultados de tests en base de datos
    if (! is.null(estadisticos)) {
      resultado.tests <- estadisticos %>%
        dplyr::mutate(!!id_column := dplyr::pull(ubicacion, !!id_column), 
                      indice_configuracion_id = configuracion.indice$id, 
                      metodo_imputacion_id = 0, pentada_fin = pentada.fin) %>%  
        dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, test, parametro, metodo_imputacion_id, valor)
      feather::write_feather(resultado.tests, glue::glue("{config$dir$data}/control/resultados_tests/",
                                                         "resultados_tests_{ubicacion %>% dplyr::pull(!!id_column)}_",
                                                         "{configuracion.indice$id}_{pentada.fin}.feather"))
    }
  }
  
  # e. Guardar parametros en base de datos. En el caso de ser un objeto de ajuste, 
  #    no se guarda en la base de datos. Ir directamente al paso 3.
  if (is.list(parametros.ajuste) && (class(parametros.ajuste) != "logspline")) {
    parametros.ajuste <- ParametrosADataFrame(parametros.ajuste) %>%
      dplyr::mutate(!!id_column := dplyr::pull(ubicacion, !!id_column),  
                    indice_configuracion_id = configuracion.indice$id, 
                    metodo_imputacion_id = 0, pentada_fin = pentada.fin) %>%
      dplyr::select(indice_configuracion_id, !!id_column, pentada_fin, parametro, metodo_imputacion_id, valor)
    feather::write_feather(resultado.tests, glue::glue("{config$dir$data}/control/parametros/",
                                                       "parametros_{ubicacion %>% dplyr::pull(!!id_column)}_",
                                                       "{configuracion.indice$id}_{pentada.fin}.feather"))
  }
  
  return (parametros.ajuste)
}

CalcularIndicesSequiaUbicacionFecha <- function(ubicacion, fecha.procesable, parametros.ajuste, configuracion.indice, 
                                                script, config, estadisticas.variables) {
  # Calculo de pentadas de inicio y fin
  # 1. Buscar parametros de ajuste para esta configuracion, ubicacion,
  #    y pentada de fin (correspondiente a la fecha procesable)
  ano.fin               <- lubridate::year(fecha.procesable)
  pentada.fin           <- fecha.a.pentada.ano(fecha.procesable)
  fecha.fin.pentada.fin <- fecha.fin.pentada(fecha.procesable)
  
  # 2. Una vez obtenidos los parametros de ajuste:
  #    i. Si alguno de los parametros es NA, tanto el valor del indice como su
  #       percentil asociado son NA. Ir directamente al paso iii.
  calcular.indice <- TRUE
  if (is.data.frame(parametros.ajuste)) {
    # Si no hay ningun parametro nulo, pasar los parametros a lista.
    calcular.indice   <- ! any(is.na(parametros.ajuste$valor))
    parametros.ajuste <- ParametrosALista(parametros.ajuste)
  } else {
    # Es el objeto de ajuste devuelto por "logspline"
    calcular.indice <- (class(parametros.ajuste) == "logspline")
  }
  
  # ii. Calcular valor de indice y percentil asociado.
  #     Buscar primero el valor de correspondiente a la fecha procesable.
  estadisticas.calculo  <- estadisticas.variables %>%
    dplyr::filter(fecha_hasta == fecha.fin.pentada.fin)
  var.acumulada.calculo <- NA  
  if (nrow(estadisticas.calculo) == length(unique(estadisticas.variables$realizacion))) {
    if (configuracion.indice$indice == "SPEI") {
      # Restarle la et0
      var.acumulada.calculo <- estadisticas.calculo %>%
        dplyr::mutate(diferencia = prcp - et0) %>%
        dplyr::pull(diferencia)
    } else {
      var.acumulada.calculo <- estadisticas.calculo %>%
        dplyr::pull(prcp)
    }
  }
  rm(estadisticas.calculo)
  
  # Ejecutar calculo si los parametros son validos y el valor de entrada no es NA
  if (calcular.indice && ! is.na(var.acumulada.calculo)) {
    configuracion.calculo <- config$params$calculo[[configuracion.indice$indice]]
    funcion.calculo       <- configuracion.calculo$funcion
    parametros.calculo    <- configuracion.calculo$parametros.adicionales
    parametros.calculo$x  <- var.acumulada.calculo
    if (class(parametros.ajuste) == "logspline") {
      if (! is.null(configuracion.calculo$parametro.logspline)) {
        parametros.calculo[[configuracion.calculo$parametro.logspline]] <- parametros.ajuste
      }
    } else if (is.list(parametros.ajuste)) {
      if (! is.null(configuracion.calculo$parametro.lista)) {
        parametros.calculo[[configuracion.calculo$parametro.lista]] <- parametros.ajuste
      } else if (! is.null(configuracion.calculo$parametro.vector)) {
        parametros.calculo[[configuracion.calculo$parametro.vector]] <- unname(unlist(parametros.ajuste))
      }
    }
    
    resultado.calculo <- do.call(what = funcion.calculo, args = parametros.calculo)
    return (data.frame(ano = ano.fin, pentada_fin = pentada.fin, valor_dato = resultado.calculo$valor_dato, 
                       valor_indice = resultado.calculo$valor_indice, percentil_dato = resultado.calculo$percentil_dato))
  } else {
    return (data.frame(ano = ano.fin, pentada_fin = pentada.fin, valor_dato = var.acumulada.calculo, valor_indice = NA, percentil_dato = NA))
  }
}
# ------------------------------------------------------------------------------