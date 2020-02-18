# -----------------------------------------------------------------------------#
# --- Funciones para calcular indices y sus percentiles asociados ----
# -----------------------------------------------------------------------------#

ResultadoADataFrame <- function(valor.dato, valor.indice, percentil.dato) {
  return (data.frame(valor_dato = valor.dato, valor_indice = valor.indice, percentil_dato = percentil.dato))
}

NormalizarProbabilidad <- function(prob) {
  if (is.na(prob) || is.nan(prob)) {
    return (NA)
  } else if (prob > 1) {
    return (1)
  } else if (prob < 0) {
    return (0)
  } else {
    return (prob)
  }
}
NormalizarIndice <- function(valor, limites = NULL) {
  if (! is.na(valor) && ! is.nan(valor)) {
    if (! is.null(limites)) {
      if (valor > limites$max) {
        return (limites$max)
      } else if (valor < limites$min) {
        return (limites$min)
      } else {
        return (valor)
      }
    } else {
      return (valor)
    }    
  } else {
    return (NA)
  }
}

CalcularRadiacionSolarExtraterrestre <- function(fecha_desde, fecha_hasta, latitud) {
  # 1. Calcular radiacion solar extraterrestre para cada dia al anio. sirad::radians(latitud)
  rad.ext <- (sirad::extrat(seq(from = 1, to = 366), sirad::radians(latitud)))$ExtraTerrestrialSolarRadiationDaily
  
  # 2. Calcular secuencia de fechas
  fecha.inicio <- min(fecha_desde)
  fecha.fin    <- max(fecha_hasta)
  fechas.seq   <- seq(from = fecha.inicio, to = fecha.fin, by = 'days')
  ydays.seq    <- lubridate::yday(fechas.seq)
  
  # 3. Para cada rango de fechas, calcular la radiacion diaria promedio
  radiacion <- purrr::map(
    .x = seq(from = 1, to = length(fecha_desde)),
    .f = function(seq_index) {
      pos.desde <- which(fechas.seq == fecha_desde[seq_index])
      pos.hasta <- which(fechas.seq == fecha_hasta[seq_index])
      pos.seq   <- seq(from = pos.desde, to = pos.hasta, by = 1)
      return (mean(rad.ext[ydays.seq[pos.seq]]))
    }
  ) %>% unlist()
  
  return (radiacion)
}

AgregarET0 <- function(fecha.desde, fecha.hasta, ancho.ventana, et0.original) {
  pos.ancho.mensual <- which(ancho.ventana == 6)
  et0.completo      <- purrr::imap(
    .x = ancho.ventana,
    .f = function(ancho_ventana, seq_index) {
      if (ancho_ventana > 6) {
        fecha_desde         <- fecha.desde[seq_index]
        fecha_hasta         <- fecha.hasta[seq_index]
        vector.fechas.desde <- sumar.pentadas(fecha_desde, cantidad = seq(from = 0, to = ancho_ventana - 6, by = 6))
        pos.fechas.desde    <- which(fecha.desde %in% vector.fechas.desde)
        pos.interseccion    <- pos.fechas.desde[which(pos.fechas.desde %in% pos.ancho.mensual)]
        return (sum(et0.original[pos.interseccion], na.rm = FALSE))
      } else {
        return (et0.original[seq_index])
      }
    }
  )
  
  return (unlist(et0.completo))
}

CalcularSPI <- function(x, objeto.ajuste = NULL, parametros.gamma = NULL, limites = NULL) {
  prob.gamma <- NA
  if (! is.null(objeto.ajuste)) {
    prob.gamma <- logspline::plogspline(x, objeto.ajuste)
  } else if (! is.null(parametros.gamma)) {
    if (parametros.gamma$prob.0 != 0) {
      prob.gamma <- stats::pgamma(x, shape = parametros.gamma$alpha, scale = parametros.gamma$beta)
      prob.gamma <- ifelse(prob.gamma == 0,
                           parametros.gamma$prob.media.0,
                           parametros.gamma$prob.0 + ((1 - parametros.gamma$prob.0) * prob.gamma))
    } else {
      # No hay ceros en el periodo de referencia
      # Si x = 0, se hace el chanchullo de tomar como valor 0.01
      y          <- ifelse(x == 0, 0.01, x)
      prob.gamma <- stats::pgamma(y, shape = parametros.gamma$alpha, scale = parametros.gamma$beta)
    }
  }
  prob.gamma <- NormalizarProbabilidad(prob.gamma)
  if (! is.na(prob.gamma)) {
    spi       <- NormalizarIndice(stats::qnorm(prob.gamma, mean = 0, sd = 1), limites)
    percentil <- 100 * prob.gamma
  } else {
    spi       <- NA
    percentil <- NA
  }
  
  return (ResultadoADataFrame(x, spi, percentil))
}

CalcularSPEI <- function(x, objeto.ajuste = NULL, parametros.log.logistica = NULL, limites = NULL) {
  prob.glogis <- NA
  if (! is.null(objeto.ajuste)) {
    prob.glogis <- logspline::plogspline(x, objeto.ajuste)
  } else if (! is.null(parametros.log.logistica)) {
    prob.glogis <- SCI::pgenlog(x, location = parametros.log.logistica$xi,
                                   scale = parametros.log.logistica$alpha,
                                   shape = parametros.log.logistica$kappa) 
  }
  prob.glogis <- NormalizarProbabilidad(prob.glogis)
  if (! is.na(prob.glogis)) {
    spei      <- NormalizarIndice(stats::qnorm(prob.glogis, mean = 0, sd = 1), limites)
    percentil <- 100 * prob.glogis
  } else {
    spei      <- NA
    percentil <- NA
  }
  
  return (ResultadoADataFrame(x, spei, percentil)) 
}

CalcularDecil <- function(x, objeto.ajuste = NULL, percentiles = NULL) {
  # Verificar que x sea numerico
  base::stopifnot(is.numeric(x))
  
  # Verificar que haya enviado el objeto de ajuste o los percentiles
  if (is.null(objeto.ajuste)) {
    base::stopifnot(is.numeric(percentiles))
  } else {
    base::stopifnot(class(objeto.ajuste) == "logspline")
  }
  
  # Calcular decil y percentil correspondiente (solo si envio objeto de ajuste)
  if (! is.null(objeto.ajuste)) {
    percentil <- 100 * logspline::plogspline(x, objeto.ajuste)
    decil     <- base::cut(x = percentil,
                           breaks = c(-Inf, seq(from = 10, to = 90, by = 10), Inf),
                           labels = seq(from = 1, to = 10),
                           right = FALSE,
                           include.lowest = TRUE)
  } else {
    decil     <- base::cut(x = x,
                           breaks = c(-Inf, sort(percentiles), Inf),
                           labels = seq(from = 1, to = 10),
                           right = FALSE,
                           include.lowest = TRUE)
    percentil <- NA
  }
  
  # Devolver resultados como data frame
  return (ResultadoADataFrame(x, decil, percentil))
}

CalcularPPN <- function(x, media) {
  # Verificar que x sea numerico
  base::stopifnot(is.numeric(x))
  
  # Verificar que media sea numerico
  base::stopifnot(is.numeric(media))
  
  # Calcular PPN. En ese caso no se calcula el percentil porque no tenemos datos suficientes.
  ppn       <- 100 * (x / media)
  percentil <- NA
  
  # Devolver resultados como data frame
  return (ResultadoADataFrame(x, ppn, percentil))
}

# -----------------------------------------------------------------------------