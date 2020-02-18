# -----------------------------------------------------------------------------#
# --- Funciones para testear la bondad del ajuste ----
# -----------------------------------------------------------------------------#

# --- Funciones para estimar la bondad del ajuste
TestKS <- function(x, distribucion, parametros.ajuste) {
  resultados <- NULL
  if (distribucion == "Gamma") {
    # Primero se deben eliminar los ceros
    x              <- x[which(x > 0)]
    resultado.test <- stats::ks.test(x = x, y = "pgamma",
                                     shape = parametros.ajuste$alpha,
                                     scale = parametros.ajuste$beta)
    resultados     <- list(p.value = unname(resultado.test$p.value),
                           statistic = unname(resultado.test$statistic))
  } else if (distribucion == "Log-Logistica") {
    resultado.test <- stats::ks.test(x = x, y = "pgenlog",
                                     scale = parametros.ajuste$alpha,
                                     shape = parametros.ajuste$kappa,
                                     location = parametros.ajuste$xi)
    resultados     <- list(p.value = unname(resultado.test$p.value),
                           statistic = unname(resultado.test$statistic))
  }
  
  return (resultados)
}
TestAD <- function(x, distribucion, parametros.ajuste) {
  resultados <- NULL
  if (distribucion == "Gamma") {
    # Primero se deben eliminar los ceros
    x              <- x[which(x > 0)]
    resultado.test <- ADGofTest::ad.test(x = x, distr.fun = pgamma,
                                         shape = parametros.ajuste$alpha,
                                         scale = parametros.ajuste$beta)
    resultados     <- list(p.value = unname(resultado.test$p.value),
                           statistic = unname(resultado.test$statistic))
  } else if (distribucion == "Log-Logistica") {
    resultado.test <- ADGofTest::ad.test(x = x, distr.fun = pgenlog,
                                         scale = parametros.ajuste$alpha,
                                         shape = parametros.ajuste$kappa,
                                         location = parametros.ajuste$xi)
    resultados     <- list(p.value = unname(resultado.test$p.value),
                           statistic = unname(resultado.test$statistic))
  }
  
  return (resultados)
}
TestCVM <- function(x, distribucion, parametros.ajuste) {
  resultados <- NULL
  if (distribucion == "Gamma") {
    # Primero se deben eliminar los ceros
    x              <- x[which(x > 0)]
    resultado.test <- goftest::cvm.test(x = x, null = pgamma,
                                        shape = parametros.ajuste$alpha,
                                        scale = parametros.ajuste$beta)
    resultados     <- list(p.value = unname(resultado.test$p.value),
                           statistic = unname(resultado.test$statistic))
  } else if (distribucion == "Log-Logistica") {
    resultado.test <- goftest::cvm.test(x = x, null = pgenlog,
                                        scale = parametros.ajuste$alpha,
                                        shape = parametros.ajuste$kappa,
                                        location = parametros.ajuste$xi)
    resultados     <- list(p.value = unname(resultado.test$p.value),
                           statistic = unname(resultado.test$statistic))
  }
  
  return (resultados)
}

# Funciones para estimar bondad de ajuste en base a metricas de cuantiles
CalcularCuantilesAjustados <- function(probs, distribucion, parametros.ajuste = NULL, objeto.ajuste = NULL) {
  if (! is.null(objeto.ajuste)) {
    # Ajuste no parametrico  
    cuant.ajustados <- logspline::qlogspline(p = probs, fit = objeto.ajuste)
  } else if (distribucion == "Gamma") {
    # Ajuste parametrico de distribucion Gamma
    cuant.ajustados <- stats::qgamma(p = probs, shape = parametros.ajuste$alpha, scale = parametros.ajuste$beta)
  } else if (distribucion == "Log-Logistica") {
    # Ajuste parametrico de distribucion Log-logistica
    cuant.ajustados <- SCI::qgenlog(p = probs, scale = parametros.ajuste$alpha, shape = parametros.ajuste$kappa,
                                    location = parametros.ajuste$xi)
  }
  
  return (cuant.ajustados)
}
TestRMSEIQR <- function(x, distribucion = c("Gamma", "Log-Logistica"), probs, parametros.ajuste = NULL, objeto.ajuste = NULL) {
  # Calculo del RMSE entre los cuantiles observados y los estimados a partir de una distribucion
  # Validar parametros
  distribucion <- base::match.arg(distribucion)
  base::stopifnot(is.numeric(x))
  base::stopifnot(all(probs >= 0.0 && probs <= 1.0))
  base::stopifnot(
    (! is.null(parametros.ajuste) && (class(parametros.ajuste) == "list")) || 
    (! is.null(objeto.ajuste) && (is.na(objeto.ajuste) || (class(objeto.ajuste) == "logspline")))
  ) 
  
  # Devolver NA si no hubo ajuste
  if ((! is.null(objeto.ajuste) && is.na(objeto.ajuste)) || 
      (! is.null(parametros.ajuste) && any(is.na(parametros.ajuste)))) {
    return (NA)
  }
  
  # Calcular los cuantiles empiricos
  cuant.observados <- quantile(x, probs = probs)
  
  # Calcular los cuantiles de la PDF ajustada
  cuant.ajustados <- CalcularCuantilesAjustados(probs, distribucion, parametros.ajuste, objeto.ajuste)
    
  # Calcular RMSE entre cuantiles empiricos y teoricos
  rmse <- caret::RMSE(pred = cuant.ajustados, obs = cuant.observados)
  
  # Calcular IQR de la muestra
  iqr <- stats::IQR(x)
  
  return (rmse/iqr)
}
TestCCC <- function(x, distribucion = c("Gamma", "Log-Logistica"), probs, parametros.ajuste = NULL, objeto.ajuste = NULL) {
  # Coeficiente de correlacion de concordancia es una medida de correlacion, consistencia y precisión basada en Lin, L. (1989). 
  # A concordance correlation coefficient to evaluate reproducibility. Biometrics, 45 (1), 255???268.
  
  # Validar parametros
  distribucion <- base::match.arg(distribucion)
  base::stopifnot(is.numeric(x))
  base::stopifnot(all(probs >= 0.0 && probs <= 1.0))
  base::stopifnot(
    (! is.null(parametros.ajuste) && (class(parametros.ajuste) == "list")) || 
    (! is.null(objeto.ajuste) && (is.na(objeto.ajuste) || (class(objeto.ajuste) == "logspline")))
  ) 
  
  # Devolver NA si no hubo ajuste
  if ((! is.null(objeto.ajuste) && is.na(objeto.ajuste)) || 
      (! is.null(parametros.ajuste) && any(is.na(parametros.ajuste)))) {
    return (NA)
  }
  
  # Calcular los cuantiles empiricos
  cuant.observados <- quantile(x, probs = probs)
  
  # Calcular los cuantiles de la PDF ajustada
  cuant.ajustados <- CalcularCuantilesAjustados(probs, distribucion, parametros.ajuste, objeto.ajuste)
  
  # Crear objeto para el calculo del RMSE
  cuantiles <- data.frame(observados = cuant.observados, ajustados = cuant.ajustados)
  
  # Calcular RMSE entre cuantiles empiricos y teoricos
  ccc <- yardstick::ccc(cuantiles, truth = observados, estimate = ajustados)
  if (is.tbl(ccc)) {
    return (dplyr::pull(ccc, .estimate))
  } else {
    return (ccc)
  }
}
TestQCOMHD <- function(x, distribucion = c("Gamma", "Log-Logistica"), probs, numero.muestras, parametros.ajuste = NULL, objeto.ajuste = NULL) {
  # Prueba de comparación de cuartiles basada en Rand R. Wilcox, David M. Erceg-Hurn, Florence Clark & Michael Carlson (2014) Comparing 
  # two independent groups via the lower and upper quantiles, Journal of Statistical Computation and Simulation, 84:7, 1543-1551, 
  # DOI: 10.1080/00949655.2012.754026
  
  # Validar parametros
  distribucion <- base::match.arg(distribucion)
  base::stopifnot(is.numeric(x))
  base::stopifnot(all(probs >= 0.0 && probs <= 1.0))
  base::stopifnot(is.integer(numero.muestras))
  base::stopifnot(
    (! is.null(parametros.ajuste) && (class(parametros.ajuste) == "list")) || 
    (! is.null(objeto.ajuste) && (is.na(objeto.ajuste) || (class(objeto.ajuste) == "logspline")))
  )  
  
  # Devolver NA si no hubo ajuste
  if ((! is.null(objeto.ajuste) && is.na(objeto.ajuste)) || 
      (! is.null(parametros.ajuste) && any(is.na(parametros.ajuste)))) {
    return (data.frame(q = probs, p.crit = rep(NA, length(probs)), p.value = rep(NA, length(probs))))
  }
  
  # Generar un remuestreo del tamaño de la muestra original
  if (! is.null(objeto.ajuste)) {
    # Ajuste no parametrico  
    muestra <- logspline::rlogspline(n = length(x), fit = objeto.ajuste)
  } else if (distribucion == "Gamma") {
    # Ajuste parametrico de distribucion Gamma
    muestra <- rgamma(n = length(x), shape = parametros.ajuste$alpha, scale = parametros.ajuste$beta)
  } else if (distribucion == "Log-Logistica") {
    # Ajuste parametrico de distribucion Log-logistica
    muestra <- rgenlog(n = length(x), scale = parametros.ajuste$alpha, shape = parametros.ajuste$kappa,
                       location = parametros.ajuste$xi)
  }
  
  # Generar objeto para realizar calculo
  datos <- data.frame(observados = x, remuestreados = muestra)
  
  # Calcular t-student sobre cada percentil definido en probs entre la muestra 
  # original y una serie sintetica a partir del ajuste de una distribucion
  resultado <- WRS2::qcomhd(observados~remuestreados, data = datos, q = probs, nboot = numero.muestras)
  qcomhd    <- resultado$partable %>%
    dplyr::select(q, p.crit, p.value)
  return(qcomhd)
}

# --- Consolidacion de tests
TestearBondadAjuste <- function(x, distribucion = c("Gamma", "Log-Logistica"), config, 
                                parametros.ajuste = NULL, objeto.ajuste = NULL, percentiles.ajuste = NULL) {
  # 1. Validar datos de entrada
  # Validar parametros
  distribucion <- base::match.arg(distribucion)
  base::stopifnot(is.numeric(x))
  base::stopifnot(
    (! is.null(parametros.ajuste) && (class(parametros.ajuste) == "list")) || 
    (! is.null(objeto.ajuste) && (is.na(objeto.ajuste) || (class(objeto.ajuste) == "logspline"))) || 
    (! is.null(percentiles.ajuste) && (base::is.numeric(percentiles.ajuste) || all(is.na(percentiles.ajuste))))
  )  
  
  # 2. Inicializar objeto a devolver
  resultados.tests      <- list(pasa.tests = TRUE, estadisticos = NULL)
  ajuste.parametrico    <- ! is.null(parametros.ajuste)
  ajuste.no.parametrico <- ! is.null(objeto.ajuste)
  ajuste.empirico       <- ! is.null(percentiles.ajuste)
  
  # 3. Aplicar tests exclusivos para caso parametrico
  #    Si alguno de los tests devuelve NA o un valor de p-value menor al umbral,
  #    interpretar el resultado del test como un fallo. Luego, si alguno de los tests falla, entonces
  #    interpretar como malo el ajuste y devolver todos los parametros en NA.
  falla.ajuste.parametrico <- FALSE
  if (ajuste.parametrico) {
    falla.ajuste.parametrico <- any(is.na(parametros.ajuste))
    if (! falla.ajuste.parametrico) {
      estadisticos <- purrr::map_dfr(
        .x = c("KS", "AD", "CVM"),
        .f = function(test.name) {
          func.name <- paste0("Test", test.name)
          tryCatch({
            estadisticos.test <- ParametrosADataFrame(do.call(what = func.name, args = list(x = x, distribucion = distribucion, parametros.ajuste = parametros.ajuste))) %>%
              dplyr::mutate(test = test.name) %>%
              dplyr::select(test, parametro, valor)
          }, error = function(e) {
            cat(e$message, "\n")
            return (NULL)
          })
        }
      )
      resultados.tests$estadisticos <- estadisticos
      
      # Determinar si pasan los tests o no
      p.values <- estadisticos %>%
        dplyr::filter(parametro == "p.value") %>%
        dplyr::pull(valor)
      if (any(is.na(p.values)) || any(p.values < config$tests$umbral.p.valor)) {
        resultados.tests$pasa.tests <- FALSE  
      } else {
        resultados.tests$pasa.tests <- TRUE 
      }
    } else {
      resultados.tests$pasa.tests <- FALSE
    }
  }
  
  # 4. Test para ajuste empirico (de percentiles)
  #    Se verifica que los cuantiles devueltos no contengan valores duplicados
  if (ajuste.empirico) {
    resultados.tests$pasa.tests <- ! any(duplicated(percentiles.ajuste)) 
  }
  
  # 5. Aplicar tests de RMST, CCC y QCOMHD (aplicables a ajustes parametricos como no parametricos)
  #    Guardar los valores resultantes, pero no dictaminar en base a esos tests.
  if (ajuste.no.parametrico || (ajuste.parametrico && ! falla.ajuste.parametrico)) {
    # i. RMSE / IQR
    rmseiqr <- TestRMSEIQR(x = x, distribucion = distribucion, probs = do.call("seq", config$params$tests$probs$ccc),
                           parametros.ajuste = parametros.ajuste, objeto.ajuste = objeto.ajuste)
    resultados.tests$estadisticos <- rbind(resultados.tests$estadisticos, data.frame(test = 'RMSEIQR', parametro = 'rmseiqr', valor = rmseiqr))
    
    # ii. CCC
    ccc <- TestCCC(x = x, distribucion = distribucion, probs = do.call("seq", config$params$tests$probs$ccc),
                   parametros.ajuste = parametros.ajuste, objeto.ajuste = objeto.ajuste)
    resultados.tests$estadisticos <- rbind(resultados.tests$estadisticos, data.frame(test = 'CCC', parametro = 'ccc', valor = ccc))
    
    # iii. QCOMHD. Este test puede fallar si hay muchos valores repetidos
    tryCatch({
      qcomhd <- TestQCOMHD(x = x, distribucion = distribucion, probs = do.call("seq", config$params$tests$probs$qcomhd),
                           numero.muestras = config$params$tests$qcomhd.muestras, parametros.ajuste = parametros.ajuste,
                           objeto.ajuste = objeto.ajuste)
      resultados.tests$estadisticos <- rbind(resultados.tests$estadisticos, qcomhd %>%
                                               tidyr::gather(key = para, value = valor, -q) %>%
                                               dplyr::mutate(test = 'QCOMHD', parametro = paste0(para, '-', q)) %>%
                                               dplyr::select(test, parametro, valor))  
    }, error = function(e) {
      cat(e$message, "\n")  
    })
  }

  # 6. Devolver resultados
  return (resultados.tests)
}
# -----------------------------------------------------------------------------
