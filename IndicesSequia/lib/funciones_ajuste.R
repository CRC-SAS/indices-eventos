# -----------------------------------------------------------------------------#
# --- Funciones para ajustar distribuciones segun distintas metodologias ----
# -----------------------------------------------------------------------------#

# --- Funcion para generar muestras aleatorias de Logistica Generalizada
# Dado que la funcion rgenlog no existe en el paquete SCI, necesitamos definirla 
# para poder realizar el remuestreo para SPEI. A tal efecto, se implementa un 
# envoltorio utilizando la funcion lmomco::rlmomco como base de calculo.
rgenlog <- function(n, shape, scale, location) {
  obj.parglo <- list(
    type = "glo",
    para = c("xi" = location, "alpha" = scale, "kappa" = shape),
    source = "parglo"
  )
  return (lmomco::rlmomco(n, para = obj.parglo))
}

# --- Funciones para pasar parametros de lista a data.frame y viceversa
ParametrosADataFrame <- function(parametros.ajuste) {
  parametros <- tibble::enframe(parametros.ajuste) %>%
    tidyr::unnest(cols = c(name, value)) %>% # union(c(name, value), colnames(.))) %>%
    dplyr::rename(parametro = name, valor = value)
  return (parametros)
}
ParametrosALista <- function(parametros.ajuste) {
  parametros.lista        <- as.list(parametros.ajuste$valor)
  names(parametros.lista) <- parametros.ajuste$parametro
  return (parametros.lista)
}

# --- Funciones de ajuste propiamente dichas
AjustarLMomentosGamma <- function(x, submetodo, min.tasa.valores.positivos) {
  # Ajuste de la distribución Gamma por metodo de L-Momentos
  # Eliminar los valores = 0 porque está fuera del dominio de la distribución Gamma
  # Verificar si el vector de entrada tiene ceros 
  prob.cero             <- 0 # Probabilidad de valores = 0 segun (Stagge et. al, 2015), eq [2]
  prob.media.mult.ceros <- 0 # Probabilidad de valores = 0 segun (Stagge et. al, 2015), eq [3]
  longitud.inicial      <- length(x) # Cantidad de elementos antes de eliminar 0s
  if (any(x == 0)) {
    # Calcular probabilidad de valores = 0
    n.ceros         <- as.integer(length(x[x == 0])) 		                 # Numero de ceros en ESTA serie
    largo.con.ceros <- as.integer(length(x))                             # Largo serie ajustada (con ceros)
    prob.cero       <- (n.ceros / (largo.con.ceros + 1))                 # (Stagge et. al, 2015, eq 2)
    prob.media.mult.ceros <- (n.ceros + 1) / (2 * (largo.con.ceros + 1)) # (Stagge et. al, 2015, eq 3)
    x               <- x[which(x > 0)]                                   # Eliminar los ceros presentes
    rm(n.ceros, largo.con.ceros)
  }
  
  # Verificar si hay un minimo numero de valores para estimar parametros
  parametros.ajuste <- list(alpha = NA, beta = NA, prob.0 = prob.cero, prob.media.0 = prob.media.mult.ceros)
  if (length(x) >= (min.tasa.valores.positivos * longitud.inicial)) {
    # Estimar Probability-Weighted Moments (PWM)
    if (submetodo == 'PP-PWM') {
      pwm.result <- lmomco::pwm.pp(x)
    } else if (submetodo == 'UB-PWM') {
      pwm.result <- lmomco::pwm.ub(x)
    }
    
    # Convertir Probability-Weighted Moments (PWM) a L-moments
    lmom.fit <- lmomco::pwm2lmom(pwm.result)
    if (lmomco::are.lmom.valid(lmom.fit) && !is.na(sum(lmom.fit[[1]])) && !is.nan(sum(lmom.fit[[1]]))) {
      # Extraer parametros
      gam.pars                       <- lmomco::pargam(lmom.fit, checklmom = TRUE)[["para"]]
      parametros.ajuste$alpha        <- unname(gam.pars[1])
      parametros.ajuste$beta         <- unname(gam.pars[2])
    } 
  }
  
  return (parametros.ajuste)
}
AjustarLMomentosLogLogistica <- function(x, submetodo) {
  # Estimar Probability-Weighted Moments (PWM)
  if (submetodo == 'PP-PWM') {
    pwm.result <- lmomco::pwm.pp(x)
  } else if (submetodo == 'UB-PWM') {
    pwm.result <- lmomco::pwm.ub(x)
  }
    
  # Convertir Probability-Weighted Moments (PWM) a L-moments
  parametros.ajuste <- list(alpha = NA, kappa = NA, xi = NA)
  lmom.fit <- lmomco::pwm2lmom(pwm.result)
  if (lmomco::are.lmom.valid(lmom.fit) && !is.na(sum(lmom.fit[[1]])) && !is.nan(sum(lmom.fit[[1]]))) {
    # Extraer parametros
    glo.pars                 <- lmomco::parglo(lmom.fit, checklmom = TRUE)[['para']]
    parametros.ajuste$alpha  <- unname(glo.pars[2])
    parametros.ajuste$kappa  <- unname(glo.pars[3])
    parametros.ajuste$xi     <- unname(glo.pars[1])
  } 
  
  return (parametros.ajuste)
}
AjustarLMomentos <- function(x = NULL, distribucion = c("Gamma", "Log-Logistica"), 
                             submetodo = c("PP-PWM", "UB-PWM"), min.tasa.valores.positivos) {
  # Verificar que los valores de distribucion y submetodo sean correctos
  distribucion <- base::match.arg(distribucion)
  submetodo    <- base::match.arg(submetodo)
  
  # Verificar que x sea un vector numerico. 
  # Si es NULL, todos los valores de parametros deben ser NA.
  if (is.null(x)) {
    if (distribucion == "Gamma") {
      return (list(alpha = NA, beta = NA))    
    } else {
      return (list(alpha = NA, kappa = NA, xi = NA))    
    }
  } else {
    base::stopifnot(is.numeric(x))
  }
  
  # Verificar la tasa minima de valores positivos
  base::stopifnot(is.null(min.tasa.valores.positivos) || is.numeric(min.tasa.valores.positivos))
  
  # Realizar ajuste
  tryCatch({
    if (distribucion == "Gamma") {
      return (AjustarLMomentosGamma(x = x, submetodo = submetodo, min.tasa.valores.positivos = min.tasa.valores.positivos))
    } else if (distribucion == "Log-Logistica") {
      return (AjustarLMomentosLogLogistica(x = x, submetodo = submetodo))
    }  
  }, error = function(e) {
    cat(e$message, "\n")
    if (distribucion == "Gamma") {
      return (list(alpha = NA, beta = NA))    
    } else {
      return (list(alpha = NA, kappa = NA, xi = NA))    
    }
  })
}
AjustarMaximaVerosimilitudGamma <- function(x, numero.muestras = NULL, min.tasa.valores.positivos) {
  # Ajuste de la distribución Gamma por metodo de maxima verosimilitud
  # Eliminar los valores = 0 porque está fuera del dominio de la distribución Gamma
  # Verificar si el vector de entrada tiene ceros 
  prob.cero             <- 0 # Probabilidad de valores = 0 segun (Stagge et. al, 2015), eq [2]
  prob.media.mult.ceros <- 0 # Probabilidad de valores = 0 segun (Stagge et. al, 2015), eq [3]
  longitud.inicial      <- length(x) # Cantidad de elementos antes de eliminar 0s
  if (any(x == 0)) {
    # Calcular probabilidad de valores = 0
    n.ceros         <- as.integer(length(x[x == 0])) 		                 # Numero de ceros en ESTA serie
    largo.con.ceros <- as.integer(length(x))                             # Largo serie ajustada (con ceros)
    prob.cero       <- (n.ceros / (largo.con.ceros + 1))                 # (Stagge et. al, 2015, eq 2)
    prob.media.mult.ceros <- (n.ceros + 1) / (2 * (largo.con.ceros + 1)) # (Stagge et. al, 2015, eq 3)
    x               <- x[which(x > 0)]                                   # Eliminar los ceros presentes
    rm(n.ceros, largo.con.ceros)
  }
  
  # Verificar si hay un minimo numero de valores para estimar parametros
  parametros.ajuste <- list(alpha = NA, beta = NA, prob.0 = prob.cero, prob.media.0 = prob.media.mult.ceros)
  if (length(x) >= (min.tasa.valores.positivos * longitud.inicial)) {
    # Estimar L-momentos para usar como valores iniciales
    lmomco.fit     <- lmomco::pwm2lmom(lmomco::pwm.ub(x))
    gam.pars.guess <- lmomco::pargam(lmomco.fit, checklmom = TRUE)
    start.params   <- list(shape = unname(gam.pars.guess$para['alpha']), 
                           scale = unname(gam.pars.guess$para['beta']))
    
    # Realizar estimacion por metodo ML
    gam.mle.fit       <- fitdistrplus::fitdist(data = x,
                                               distr = 'gamma',
                                               method = 'mle',
                                               keepdata = FALSE,
                                               start = start.params)
    if (is.null(numero.muestras)) {
      # Estimar parametros sin remuestreo
      parametros.ajuste$alpha <- unname(gam.mle.fit$estimate['shape'])
      parametros.ajuste$beta  <- unname(gam.mle.fit$estimate['scale'])
    } else {
      # Realizar bootstrap parametrico para los parametros estimados
      bootstrap <- fitdistrplus::bootdist(f = gam.mle.fit,
                                          bootmethod = "param",
                                          niter = numero.muestras,
                                          silent = TRUE)
      
      # Calcular la mediana de cada parametro
      parametros.remuestreo   <- apply(X = bootstrap$estim, MARGIN = 2, FUN = median)
      parametros.ajuste$alpha <- unname(parametros.remuestreo['shape'])
      parametros.ajuste$beta  <- unname(parametros.remuestreo['scale'])
    }
  }
  
  return (parametros.ajuste)
}
AjustarMaximaVerosimilitudLogLogistica <- function(x, numero.muestras = NULL) {
  # Estimar L-momentos para usar como valores iniciales
  lmomco.fit     <- lmomco::pwm2lmom(lmomco::pwm.ub(x))
  glo.pars.guess <- lmomco::parglo(lmomco.fit, checklmom = TRUE)
  start.params   <- list(scale = unname(glo.pars.guess$para['alpha']), 
                         shape = unname(glo.pars.guess$para['kappa']),
                         location = unname(glo.pars.guess$para['xi']))
  
  # Realizar estimacion por metodo ML
  glo.mle.fit       <- fitdistrplus::fitdist(data = x,
                                             distr = 'genlog',
                                             method = 'mle',
                                             keepdata = FALSE,
                                             start = start.params)
  parametros.ajuste <- list(alpha = NA, kappa = NA, xi = NA)
  if (is.null(numero.muestras)) {
    # Estimar parametros sin remuestreo
    parametros.ajuste$alpha <- unname(glo.mle.fit$estimate['scale'])
    parametros.ajuste$kappa <- unname(glo.mle.fit$estimate['shape'])
    parametros.ajuste$xi    <- unname(glo.mle.fit$estimate['location'])
  } else {
    # Realizar bootstrap parametrico para los parametros estimados
    bootstrap.obj <- fitdistrplus::bootdist(f = glo.mle.fit,
                                            bootmethod = "param",
                                            niter = numero.muestras,
                                            silent = TRUE)
    
    # Calcular la mediana de cada parametro
    parametros.remuestreo   <- apply(X = bootstrap.obj$estim, MARGIN = 2, FUN = median, na.rm = TRUE)
    parametros.ajuste$alpha <- unname(parametros.remuestreo['scale'])
    parametros.ajuste$kappa <- unname(parametros.remuestreo['shape'])
    parametros.ajuste$xi    <- unname(parametros.remuestreo['location'])
  }
  
  return (parametros.ajuste)
}
AjustarMaximaVerosimilitud <- function(x = NULL, distribucion = c("Gamma", "Log-Logistica"),
                                       numero.muestras = NULL, min.tasa.valores.positivos = NULL) {
  # Verificar que los valores de distribucion
  distribucion <- base::match.arg(distribucion)
  
  # Verificar que x sea un vector numerico. 
  # Si es NULL, todos los valores de parametros deben ser NA.
  if (is.null(x)) {
    if (distribucion == "Gamma") {
      return (list(alpha = NA, beta = NA))    
    } else {
      return (list(alpha = NA, kappa = NA, xi = NA))    
    }
  } else {
    base::stopifnot(is.numeric(x))
  }
  
  # Verificar que numero.muestras sea valido
  # Si numero.muestras es NULL, entonces el ajuste es SIN remuestreo.
  # Sino, el ajuste es CON remuestreo y con el numero de muestras indicado.
  base::stopifnot(is.null(numero.muestras) || is.integer(numero.muestras))
  
  # Verificar la tasa minima de valores positivos
  base::stopifnot(is.null(min.tasa.valores.positivos) || is.numeric(min.tasa.valores.positivos))
  
  # Realizar ajuste
  tryCatch({
    if (distribucion == "Gamma") {
      return (AjustarMaximaVerosimilitudGamma(x = x, numero.muestras = numero.muestras, min.tasa.valores.positivos = min.tasa.valores.positivos))
    } else if (distribucion == "Log-Logistica") {
      return (AjustarMaximaVerosimilitudLogLogistica(x = x, numero.muestras = numero.muestras))
    }  
  }, error = function(e) {
    cat(e$message, "\n")
    if (distribucion == "Gamma") {
      return (list(alpha = NA, beta = NA))    
    } else {
      return (list(alpha = NA, kappa = NA, xi = NA))    
    }
  })
}
AjustarNoParametrico <- function(x = NULL, percentiles = NULL, lbound = NULL, min.tasa.valores.positivos = NULL) {
  # Verificar que percentiles sea valido
  # Si percentiles es NULL, entonces se debe hacer ajuste y devolver objeto ajustado
  # Sino, se debe hacer el ajuste y devolver los valores de los percentiles indicados
  base::stopifnot(is.null(percentiles) || (is.numeric(percentiles) && all(percentiles >= 0 & percentiles <= 1)))
  base::stopifnot(is.null(lbound) || is.numeric(lbound))
  base::stopifnot(is.null(min.tasa.valores.positivos) || is.numeric(min.tasa.valores.positivos))
  
  # Verificar que x sea un vector numerico
  if (is.null(x)) {
    if (is.null(percentiles)) {
      # Devolver un objeto de ajuste con valor NA
      return (NA)
    } else {
      parametros        <- as.list(rep(NA, length(percentiles)))
      names(parametros) <- paste0('p', percentiles * 100)
      return (parametros)
    }
  } else {
    base::stopifnot(is.numeric(x))
  }
  
  # En caso de estar definida una tasa minima de valores positivos, verificar tal condicion.
  # Si la condicion no se cumple, devolver NA
  longitud.inicial <- length(x) # Cantidad de elementos antes de eliminar 0s
  if (! is.null(min.tasa.valores.positivos) && any(x == 0)) {
    # Eliminar ceros
    x <- x[which(x > 0)]
    
    # Verificar condicion de minima tasa de valores positivos
    if (length(x) < (min.tasa.valores.positivos * longitud.inicial)) {
      # No se cumple la condicion. Devolver NA
      return (NA)
    }
  }
  rm(longitud.inicial)
  
  # Realizar ajuste
  tryCatch({
    objeto.ajuste <- logspline::logspline(x, lbound = ifelse(is.null(lbound), min(x), lbound),
                                          ubound = max(x), silent = TRUE)
    
    # ATENCION: Hay casos en donde se devuelve objeto de ajuste erroneo.
    # Ej: Error in objeto.ajuste$logl[, 2] : incorrect number of dimensions
    # Para ello se controla que el objeto objeto.ajuste$logl sea una matriz.
    # Si esto no se cumple se asume que no hubo ajuste
    if ((class(objeto.ajuste) != "logspline") || ! is.matrix(objeto.ajuste$logl)) {
      return (NA)
    }
    
    # Devolver parametros como data frame
    if (is.null(percentiles)) {
      # Devolver directamente el objeto ajustado
      return (objeto.ajuste)
    } else {
      # Devolver los percentiles indicados
      parametros.percentiles <- logspline::qlogspline(p = percentiles, fit = objeto.ajuste)
      parametros             <- as.list(parametros.percentiles)
      names(parametros)      <- paste0('p', percentiles * 100)
      return (parametros)
    }
  }, error = function(e) {
    cat(e$message, "\n")
    return (NA)
  })
}
AjustarEmpiricamente <- function(x = NULL, percentiles = NULL, media = FALSE) {
  # Verificar que percentiles y media sean validos
  # Si percentiles es NULL, media debe ser TRUE
  # Si percentiles no es NULL, media debe ser FALSE
  if (is.null(percentiles)) {
    base::stopifnot(media)
  } else {
    base::stopifnot(!media && is.numeric(percentiles) && all(percentiles >= 0 & percentiles <= 1))
  }
  
  # Verificar que x sea un vector numerico
  if (is.null(x)) {
    if (is.null(percentiles)) {
      return (list("mean" = NA))
    } else {
      parametros        <- as.list(rep(NA, length(percentiles)))
      names(parametros) <- paste0('p', percentiles * 100)
      return (parametros)
    }
  } else {
    base::stopifnot(is.numeric(x))
  }
  
  # Realizar ajuste
  if (media) {
    # Calcular media y devolverla
    return (list('mean' = mean(x, na.rm = TRUE)))
  } else {
    # Calcular percentiles y devolverlos
    parametros.percentiles <- unname(stats::quantile(x = x, probs = percentiles))
    parametros             <- as.list(parametros.percentiles)
    names(parametros)      <- paste0('p', percentiles * 100)
    return (parametros)
  }
}
# -----------------------------------------------------------------------------