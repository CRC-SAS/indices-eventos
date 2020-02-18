#--- Funciones para pasar fechas a pentadas y decadas en un sentido y otro ---#
require(lubridate)
require(purrr)

# --- 
# Tríadas: períodos de 3 días. Hay 122 tríadas comprendidas en un año calendario.
# La primera tríada comienza el 1 de Enero, la segunda el 4 y así sucesivamente.
# Si el año es bisiesto, hay 366 días, por lo que las 122 tríadas quedan completamente
# incluidas dentro de ese año. Si el año no es bisiesto, la última tríada comprende
# los días 30 y 31 de Diciembre y el 1 de Enero del añó siguiente. Esto implica que
# la última tríada de un añó no bisiesto y la primera del año siguiente comparten 
# el 1 de Enero.
# ---
fecha.a.triada.ano <- function(fecha) {
  triada <- ((lubridate::yday(fecha) - 1) %/% 3) + 1
  return (triada)
}
triada.ano.a.fecha.inicio <- function(triada.ano, ano) {
  dia.ano <- (triada.ano - 1) * 3 + 1
  fecha   <- as.Date(dia.ano - 1, origin = as.Date(sprintf("%d-01-01", ano)))
  return (fecha)
}
fecha.inicio.triada <- function(fecha) {
  triada.ano <- fecha.a.triada.ano(fecha)
  return (triada.ano.a.fecha.inicio(triada.ano, lubridate::year(fecha)))
}
fecha.fin.triada <- function(fecha) {
  fecha.inicio <- fecha.inicio.triada(fecha)
  return (fecha.inicio + lubridate::days(2))
}
sumar.triadas <- function(fecha.inicio.triada, cantidad.triadas) {
  triada.ano       <- fecha.a.triada.ano(fecha.inicio.triada)
  nueva.triada.ano <- triada.ano + cantidad.triadas
  anos.agregados   <- (nueva.triada.ano - 1) %/% 122
  triada.ano       <- ((nueva.triada.ano - 1) %% 122) + 1
  return (triada.ano.a.fecha.inicio(triada.ano, lubridate::year(fecha.inicio.triada) + anos.agregados))
}
seq.triadas <- function(fecha.inicio.triada.1, fecha.inicio.triada.2) {
  # Validar que la fecha 1 sea menor o igual a la fecha 2
  if (fecha.inicio.triada.1 > fecha.inicio.triada.2) {
    return (NULL)
  }
  ano.1 <- lubridate::year(fecha.inicio.triada.1)
  ano.2 <- lubridate::year(fecha.inicio.triada.2)
  
  # 1. Generar secuencia de anos
  seq.anos <- seq(from = ano.1, to = ano.2, by = 1)
  
  # 2 .Para cada ano, generar secuencia de todas las pentadas
  seq.fecha.inicio <- purrr::map(
    .x = seq.anos,
    .f = function(ano) {
      if (ano.1 == ano.2) {
        # Hay un solo ano a procesar
        p.ano <- seq(from = fecha.a.triada.ano(fecha.inicio.triada.1), to = fecha.a.triada.ano(fecha.inicio.triada.2), by = 1)
      } else if (ano == ano.1) {
        # Comienzo del primer ano
        p.ano <- seq(from = fecha.a.triada.ano(fecha.inicio.triada.1), to = 122, by = 1)
      } else if (ano == ano.2) {
        # Fin del ultimo an0
        p.ano <- seq(from = 1, to = fecha.a.triada.ano(fecha.inicio.triada.2), by = 1)
      } else {
        # Ano intermedio (todo entero)
        p.ano <- seq(from = 1, to = 122, by = 1)
      }
      
      return (purrr::map(
        .x = p.ano,
        .f = function(triada) {
          return (triada.ano.a.fecha.inicio(triada, ano))
        }
      )) %>% do.call(what = "c", args = .)
    }
  ) %>% do.call(what = "c", args = .)
  
  return (seq.fecha.inicio)
}

#--- Pentadas ---#
fecha.a.pentada.ano <- function(fecha) {
  dia         <- lubridate::day(fecha)
  mes         <- lubridate::month(fecha)
  pentada.mes <- ifelse(dia > 25, 6, ((dia - 1) %/% 5) + 1)
  return (pentada.mes + 6 * (mes - 1))
}
fecha.a.pentada.mes <- function(fecha) {
  pentada.ano <- fecha.a.pentada.ano(fecha)
  return (((pentada.ano - 1) %% 6) + 1)
}
pentada.ano.a.fecha.inicio <- function(pentada.ano, ano) {
  pentada.mes <- ((pentada.ano - 1) %% 6) + 1
  dia         <- 1 + 5 * (pentada.mes - 1)
  mes         <- ((pentada.ano - 1) %/% 6) + 1
  return (as.Date(sprintf("%d-%d-%d", ano, mes, dia)))
}
fecha.inicio.pentada <- function(fecha) {
  pentada.mes <- fecha.a.pentada.mes(fecha)
  dia.inicio  <- 1 + 5 * (pentada.mes - 1)
  return (as.Date(sprintf("%d-%d-%d", lubridate::year(fecha), lubridate::month(fecha), dia.inicio)))
}
fecha.fin.pentada <- function(fecha) {
  pentada.mes <- fecha.a.pentada.mes(fecha)
  dia.fin     <- ifelse(pentada.mes < 6, 5 + 5 * (pentada.mes - 1), lubridate::days_in_month(fecha))
  return (as.Date(sprintf("%d-%d-%d", lubridate::year(fecha), lubridate::month(fecha), dia.fin)))
}
sumar.pentadas <- function(fecha.inicio.pentada, cantidad.pentadas) {
  pentada.ano       <- fecha.a.pentada.ano(fecha.inicio.pentada)
  nueva.pentada.ano <- pentada.ano + cantidad.pentadas
  anos.agregados    <- (nueva.pentada.ano - 1) %/% 72
  pentada.ano       <- ((nueva.pentada.ano - 1) %% 72) + 1
  return (pentada.ano.a.fecha.inicio(pentada.ano, lubridate::year(fecha.inicio.pentada) + anos.agregados))
}
seq.pentadas <- function(fecha.inicio.pentada.1, fecha.inicio.pentada.2) {
  # Validar que la fecha 1 sea menor o igual a la fecha 2
  if (fecha.inicio.pentada.1 > fecha.inicio.pentada.2) {
    return (NULL)
  }
  ano.1 <- lubridate::year(fecha.inicio.pentada.1)
  ano.2 <- lubridate::year(fecha.inicio.pentada.2)
  
  # 1. Generar secuencia de anos
  seq.anos <- seq(from = ano.1, to = ano.2, by = 1)
  
  # 2 .Para cada ano, generar secuencia de todas las pentadas
  seq.fecha.inicio <- purrr::map(
    .x = seq.anos,
    .f = function(ano) {
      if (ano.1 == ano.2) {
        # Hay un solo ano a procesar
        p.ano <- seq(from = fecha.a.pentada.ano(fecha.inicio.pentada.1), to = fecha.a.pentada.ano(fecha.inicio.pentada.2), by = 1)
      } else if (ano == ano.1) {
        # Comienzo del primer ano
        p.ano <- seq(from = fecha.a.pentada.ano(fecha.inicio.pentada.1), to = 72, by = 1)
      } else if (ano == ano.2) {
        # Fin del ultimo an0
        p.ano <- seq(from = 1, to = fecha.a.pentada.ano(fecha.inicio.pentada.2), by = 1)
      } else {
        # Ano intermedio (todo entero)
        p.ano <- seq(from = 1, to = 72, by = 1)
      }
      
      return (purrr::map(
        .x = p.ano,
        .f = function(pentada) {
          return (pentada.ano.a.fecha.inicio(pentada, ano))
        }
      )) %>% do.call(what = "c", args = .)
    }
  ) %>% do.call(what = "c", args = .)
  
  return (seq.fecha.inicio)
}

#--- Decadas ---#
fecha.a.decada.ano <- function(fecha) {
  pentada.ano <- fecha.a.pentada.ano(fecha)
  decada.ano  <- ((pentada.ano - 1) %/% 2) + 1
  return (decada.ano)
}
fecha.a.decada.mes <- function(fecha) {
  decada.ano <- fecha.a.decada.ano(fecha)
  return (((decada.ano - 1) %% 3) + 1)
}
decada.ano.a.fecha.inicio <- function(decada.ano, ano) {
  decada.mes <- ((decada.ano - 1) %% 3) + 1
  dia        <- 1 + 10 * (decada.mes - 1)
  mes        <- ((decada.ano - 1) %/% 3) + 1
  return (as.Date(sprintf("%d-%d-%d", ano, mes, dia)))
}
fecha.inicio.decada <- function(fecha) {
  decada.mes <- fecha.a.decada.mes(fecha)
  dia.inicio <- 1 + 10 * (decada.mes - 1)
  return (as.Date(sprintf("%d-%d-%d", lubridate::year(fecha), lubridate::month(fecha), dia.inicio)))
}
fecha.fin.decada <- function(fecha) {
  decada.mes <- fecha.a.decada.mes(fecha)
  dia.fin    <- ifelse(decada.mes < 3, 10 + 10 * (decada.mes - 1), lubridate::days_in_month(fecha))
  return (as.Date(sprintf("%d-%d-%d", lubridate::year(fecha), lubridate::month(fecha), dia.fin)))
}
sumar.decadas <- function(fecha.inicio.decada, cantidad.decadas) {
  decada.ano       <- fecha.a.decada.ano(fecha.inicio.decada)
  nueva.decada.ano <- decada.ano + cantidad.decadas
  anos.agregados   <- (nueva.decada.ano - 1) %/% 36
  decada.ano       <- ((nueva.decada.ano - 1) %% 36) + 1
  return (decada.ano.a.fecha.inicio(decada.ano, lubridate::year(fecha.inicio.decada) + anos.agregados))
}
seq.decadas <- function(fecha.inicio.decada.1, fecha.inicio.decada.2) {
  # Validar que la fecha 1 sea menor o igual a la fecha 2
  if (fecha.inicio.decada.1 > fecha.inicio.decada.2) {
    return (NULL)
  }
  ano.1 <- lubridate::year(fecha.inicio.decada.1)
  ano.2 <- lubridate::year(fecha.inicio.decada.2)
  
  # 1. Generar secuencia de anos
  seq.anos <- seq(from = ano.1, to = ano.2, by = 1)
  
  # 2 .Para cada ano, generar secuencia de todas las pentadas
  seq.fecha.inicio <- purrr::map(
    .x = seq.anos,
    .f = function(ano) {
      if (ano.1 == ano.2) {
        # Hay un solo ano a procesar
        d.ano <- seq(from = fecha.a.decada.ano(fecha.inicio.decada.1), to = fecha.a.decada.ano(fecha.inicio.decada.2), by = 1)
      } else if (ano == ano.1) {
        # Comienzo del primer ano
        d.ano <- seq(from = fecha.a.decada.ano(fecha.inicio.decada.1), to = 36, by = 1)
      } else if (ano == ano.2) {
        # Fin del ultimo ano
        d.ano <- seq(from = 1, to = fecha.a.decada.ano(fecha.inicio.decada.2), by = 1)
      } else {
        # Ano intermedio (todo entero)
        d.ano <- seq(from = 1, to = 36, by = 1)
      }
      
      return (purrr::map(
        .x = d.ano,
        .f = function(decada) {
          return (decada.ano.a.fecha.inicio(decada, ano))
        }
      )) %>% do.call(what = "c", args = .)
    }
  ) %>% do.call(what = "c", args = .)
  
  return (seq.fecha.inicio)
}