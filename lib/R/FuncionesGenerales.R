

# ---------------------------------------------------------------------------------------#
# --- Definición de función CalcularMesDia() ----

# --- Esta función recibe una fecha y retorne mes-dia como string

CalcularMesDia <- function(fechas) {
  return (as.character(format(fechas, "%m-%d")))
}

# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definición de función biweight() ----

# Esta función estima una media y desviación estandar para el vector x
# que es resistente a outliers. 

biweight <- function(x, cc = 7.5, na.rm = TRUE) {
  
  # --- The biweight estimate is a weighted average
  # --- such that the weighting decreases away from the centre of the
  # --- distribution. All values beyond a certain critical distance from
  # --- the centre (controlled by input parameter 'cc') are given zero weight.
  # --- A 'cc' value between 6 and 9 is recommended (Hoaglin et al., 1983).
  # --- For the Gaussian case, cc = 6 (9) censors values more than 4 (6) standard
  # --- deviations from the mean. The default 'cc' value used in the function
  # --- is 7.5, which censors values more than 5 standard deviations away.
  # --- Missing values are removed by default.
  
  # Referencias:
  # Lanzante, J.R., 1996, International Journal of Climatology 16: 1197-1226.
  #
  # Hoaglin, D., F. Mosteller and J. Tukey, 1983, Understanding robust
  # and exploratory data analysis, Wiley, New York, 447 p.
  
  x <- as.vector(x)
  wnas <- which(is.na(x))
  
  if (na.rm) {
    if (length(wnas)) {
      x <- x[ - wnas]  # Eliminate missing values
    }
  } else {
    if (length(wnas)) return(NA)
  }
  
  x2 <- x
  
  if (cc < 6 || cc > 9) {  
    warning("cc values recommended to be between 6 and 9...")
  }
  
  aa6 <- median(x2)
  aa7 <- x2 - aa6
  aa8 <- mad(x2, constant=1)
  aa9b <- aa7 / (cc * aa8)         # Eq. B4 in Lanzante, p.1219
  aa9 <- ifelse(abs(aa9b) >= 1, 1, aa9b)
  
  bb1 <- (1 - (aa9 * aa9)) ** 2
  bb2 <- sum(bb1)
  bb3 <- sum(aa7 * bb1)
  bb4 <- aa6 + (bb3/bb2)  		# Eq. B5 in Lanzante, biweight mean
  
  cc1 <- abs(sum((1 - (aa9 ** 2)) * (1 - (5 * (aa9 ** 2)))))
  cc2 <- (length(x2) * sum((aa7 ** 2) * ((1 - (aa9 ** 2)) ** 4))) ** 0.5
  cc3 <- cc2/cc1					# Eq. B6 in Lanzante, biweight std deviation
  
  return(data.frame("bwtmean" = bb4,"bwtstd" = cc3))
  
}	# End of function biweight()

# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definición de función AjustarCicloEstacional() ----
# --- El argumento es una serie temporal (objeto clase xts).
# --- La salida es un objeto xts con tres columnas:
# --- (a) el valor original de la variable de entrada;
# --- (b) el valor del ciclo estacional ajustado para cada día; y
# --- (c) el residuo con respecto al ciclo estacional (i.e., el valor original menos
# ---     el ciclo estacional).

AjustarCicloEstacional <- function(x, return.tibble = FALSE, plots = FALSE) {
  
  #### x <- variable.xts # para debug
  
  # --- El argumento de La función debe ser de clase xts
  if (!xts::is.xts(x)) {
    # --- si no es de clase xts, se intenta hacer una transformacion
    if (all( c("fecha","valor") %in% names(x) )) {
      x <- xts::xts(x=dplyr::pull(x, valor), order.by=dplyr::pull(x, fecha))
    } else {
      stop("Objeto de entrada debe ser de clase xts, o un tibble con fecha y valor entre sus columnas...")
    }
  }
  
  # --- Ajustar ciclo estacional usando un modelo General Aditivo (GAM).
  # --- Usamos paquete "mgcv". No se asume una forma funcional determinada para
  # --- el ciclo estacional, sino que se usa un ajuste por splines cíclicos.
  # --- Ver: Wood S.N.(2006) Generalized Additive Models: An Introduction with R.
  # --- Chapman and Hall/CRC Press.
  
  dia.del.anio <- lubridate::yday(zoo::index(x))  # Extraemos día del año (1-366) del objeto xts
  variable <- zoo::coredata(x)         # Extraemos datos del objeto xts
  
  tt1 <- mgcv::gam(variable ~ s(dia.del.anio, bs="cc"), method = "REML", na.action=na.omit)
  
  # --- Predecir valores usando modelo ajustado con GAMs
  
  tt2 <- mgcv::predict.gam(tt1,
                     newdata=data.frame(dia.del.anio=dia.del.anio, variable=variable),
                     na.action=na.pass)
  
  ciclo.estacional <- round(xts::xts(tt2, order.by=zoo::index(x)), 2) 
  colnames(ciclo.estacional) <- "ciclo.estacional"
  
  # --- Calcular residuos respecto al ciclo estacional
  
  tt3 <- (zoo::coredata(x) - zoo::coredata(ciclo.estacional))
  
  residuos <- round(xts::xts(tt3, order.by=zoo::index(x)), 2)
  colnames(residuos) <- "residuo"
  
  tt4 <- xts::merge.xts(x, ciclo.estacional, residuos, all=TRUE, fill=NA)
  colnames(tt4) <- c("variable","ciclo.estacional","residuo")
  
  # --- Si el argumento "plots" está definido, hacer algunos diagnósticos gráficos
  
  if (plots) {
    
    # Puede haber datos faltantes y al intentar graficar datos observados
    # los gráficos dan error. Por eso se trata de encontrar un periodo de
    # dos años con pocos valores faltantes. Esta seción encuentra un bloque de dos años
    # que tenga la mayor cantidad de datos posibles.
    
    uu1 <- floor_date(min(zoo::index(tt4)), unit="month")
    uu2 <- ceiling_date(max(zoo::index(tt4)), unit="month")
    uu3 <- seq(from=uu1, to=uu2, by="2 year")
    uu4 <- cut(zoo::index(tt4), breaks=uu3, include.lowest=TRUE)
    uu5 <- tapply(X=zoo::coredata(tt4$variable), INDEX=uu4, FUN=function(x){length(x[!is.na(x)])})	
    uu6 <- which(uu5 == max(uu5, na.rm=TRUE))
    uu7 <- ifelse(length(uu6) > 1, sample(uu6, size=1), uu6) 
    uu8 <- uu3[uu7]
    uu9 <- uu8 + years(2)    
    uu10 <- paste0(as.character(uu8),"/",as.character(uu9))
    
    series.xts <- tt4[uu10]
    rm(uu1,uu2,uu3,uu4,uu5,uu6,uu7,uu8,uu9,uu10)
    
    # Gráfico de dos ciclos estacionales ajustados
    
    xts::plot.xts(series.xts$ciclo.estacional,
             axes = TRUE, auto.grid = FALSE, col = "grey50",
             major.ticks = "auto", minor.ticks = FALSE, major.format = "%m",
             xlab = "Mes",
             ylab = "Ciclo estacional",
             main = "Ciclo estacional ajustado")
    
    # Gráfico de valores observados + ciclo estacional
    zoo::plot.zoo(series.xts$variable,
             col = "grey50",
             xlab = "Mes", ylab = "Ciclo estacional",
             main = "Ciclo estacional y valores")
    
    lines(series.xts$ciclo.estacional,
          col = "tomato", lwd = 3,
          xlab = "Mes", ylab = "Ciclo estacional",
          main = "Ciclo estacional ajustado")
    
    # Boxplots por mes de valores de variable analizada
    boxplot(zoo::coredata(tt4$variable) ~ month(zoo::index(tt4)),
            xlab = "Mes",
            ylab = variable.analizada,
            main = paste("Boxplots mensuales de", variable.analizada),
            col = "wheat")
    
    # Boxplots por mes de residuos respecto a ciclo estacional
    boxplot(zoo::coredata(tt4$residuo) ~ month(zoo::index(tt4)),
            xlab = "Mes",
            ylab = "Residuos del ciclo estacional",
            main = paste("Residuos de ciclo estacional de", variable.analizada),
            col = "wheat")     
    
  } # Fin de realizacion de diagnosticos graficos
  
  if (return.tibble) return(tt4 %>% tbl2xts::xts_tbl() %>% dplyr::rename(fecha = date))
  
  return(tt4)
  
} # Fin de función AjustarCicloEstacional 

# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definicion de funcion AgregarLagLead() ---- 

# Esta función agrega series desfasadas a la columna indicada en  el parámetro
# "columna", de acuerdo al parámetro "ancho.ventana".

AgregarLagLead <- function (datos.completos, columna, ancho.ventana) {
  
  # Se calcula cuantos lags y leads se debe agregar
  n_lag_lead <- abs(ancho.ventana %/% 2)
  
  # Si no se debe agregar nada se retornan los datos sin cambios
  if (n_lag_lead == 0) return (datos.completos)
  
  # Agregar series desfasadas de acuerdo al ancho de la ventana
  for (offset in seq(from = 1, to = n_lag_lead)) {
    field.name.lag  <- paste0(columna,'_lag_', offset)
    field.name.lead <- paste0(columna,'_lead_', offset)
    datos.completos <- datos.completos %>%
      dplyr::mutate(!! field.name.lag := dplyr::lag(!! rlang::sym(columna), offset),
                    !! field.name.lead := dplyr::lead(!! rlang::sym(columna), offset))
  }
  
  return (datos.completos)
  
}

# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definicion de funcion CalcularEstadisticasDescriptivas() ---- 

# Esta funcion se utiliza para calcular estadisticas descriptivas 
# por día del año utilizando una ventana para suavizar los datos y
# también para calcular estadisticas descriptivas por mes del año.

CalcularEstadisticasDescriptivas <- function(fechas, valores, ancho.ventana = 1, min.cantidad.datos = 30,
                                             agrupar.por = c("dia", "mes"), perc.inf = .1, perc.sup = .9) {
  # 0. si no se selecciono un modo de agruapción, se usa dia
  if (base::missing(agrupar.por)) agrupar.por <- "dia"
  
  # 1. Crear tibble con datos completos de fecha
  #    Calcular la combinacion mes-dia
  datos.completos  <- dplyr::as.tbl(data.frame(fecha = fechas, original = valores)) %>%
    tidyr::complete(fecha = base::seq.Date(min(fecha), max(fecha), by = "days")) %>%
    dplyr::mutate(mes_dia = CalcularMesDia(fecha)) %>%
    dplyr::mutate(mes_num = lubridate::month(fecha)) %>%
    dplyr::select(fecha, mes_dia, mes_num, original) %>%
    dplyr::arrange(fecha) 
  
  # 2. Agregar series desfasadas de acuerdo al ancho de la ventana
  if (agrupar.por == "dia" & ancho.ventana > 1) {
    datos.completos <- datos.completos %>% 
      AgregarLagLead(columna = "original", ancho.ventana = ancho.ventana)
  }
  
  # 3. Pasar a formado largo
  datos.completos <- datos.completos %>%
    tidyr::gather(key = serie, value = valor, -fecha, -mes_dia, -mes_num) %>%
    dplyr::arrange(fecha)
  
  # 4. Calcular estadisticas por dia
  estadisticas <- datos.completos %>%
    dplyr::group_by(!!as.name(dplyr::case_when(agrupar.por == "dia" ~ "mes_dia",
                                               agrupar.por == "mes" ~ "mes_num"))) %>%
    dplyr::summarise(cantidad_datos = sum(dplyr::if_else(! is.na(valor), 1, 0)),
                     p25 = quantile(x = valor, probs = 0.25, na.rm = TRUE),
                     mediana = quantile(x = valor, probs = 0.50, na.rm = TRUE),
                     p75 = quantile(x = valor, probs = 0.75, na.rm = TRUE),
                     media = mean(x = valor, na.rm = TRUE),
                     media_biweight = biweight(x = valor, na.rm = TRUE)$bwtmean,
                     desviacion_estandar = sd(x = valor, na.rm = TRUE),
                     desviacion_estandar_biweight = biweight(x = valor, na.rm = TRUE)$bwtstd,
                     desviacion_mediana_absoluta = mad(x = valor, na.rm = TRUE),
                     ext_inf = quantile(x = valor, probs = perc.inf, na.rm = TRUE),
                     ext_sup = quantile(x = valor, probs = perc.sup, na.rm = TRUE))
  
  # 5. Eliminar datos que no contengan una cantidad minima de datos
  if (agrupar.por == "dia") {
    # TODO: verificar si es correcto dividir en 4 la cantidad mínima de datos para el 29 de febrero
    # Por causa de los leads y lags formados para cada mes-dia, el 29 de febrero tiene 4 veces menos 
    # datos que los otros mes-dia, esto hace que algunas veces no haya resultados para el 29 de febrero
    # por falta de datos (cantidad_datos < min_cant_datos).
    # Una alternativa es agregar (inventar) un 29 de febrero para así tener cada anho un 29 de febrero 
    # (entes de retornar resultados estos 29 de febrero falsos deben ser eliminados). Otra alternativa 
    # es tomar para el análisis dias del año en lugar de dias mes, pero esto hace que, por ejemplo, el 
    # 1 de junio no sea siempre el mismo días! Además, este enfoque tiene otro problema. Al igual
    # que en el cod anterior, si consideramos que un anho tiene 366 días, entonces, a las ventanas que 
    # contengan el día 366 les va a faltar un día cuando el anho no sea viciesto! Por otro lado, si 
    # consideramos un anho de 365 días, entonces no se va a utilizar un dato (además de que los días 
    # posteriores al 29 de febrero no va a ser tomados correctamente).
    estadisticas <- estadisticas %>%
      dplyr::mutate(min_cant_datos = min.cantidad.datos * ancho.ventana) %>%
      dplyr::mutate(min_cant_datos = dplyr::if_else(mes_dia == "02-29", 
                                                    ceiling(min_cant_datos/4), 
                                                    ceiling(min_cant_datos)))
  } else {
    estadisticas <- estadisticas %>%
      dplyr::mutate(min_cant_datos = min.cantidad.datos)
  }
  estadisticas <- estadisticas %>%
    dplyr::mutate(es_valido = cantidad_datos >= min_cant_datos) %>%
    dplyr::mutate(p25 = ifelse(es_valido, p25, NA),
                  mediana = ifelse(es_valido, mediana, NA),
                  p75 = ifelse(es_valido, p75, NA),
                  media = ifelse(es_valido, media, NA),
                  media_biweight = ifelse(es_valido, media_biweight, NA),
                  desviacion_estandar = ifelse(es_valido, desviacion_estandar, NA),
                  desviacion_estandar_biweight = ifelse(es_valido, desviacion_estandar_biweight, NA),
                  desviacion_mediana_absoluta = ifelse(es_valido, desviacion_mediana_absoluta, NA),
                  ext_inf = ifelse(es_valido, ext_inf, NA),
                  ext_sup = ifelse(es_valido, ext_sup, NA)) %>%
    dplyr::select(-min_cant_datos, -es_valido)
  
  # 6. Calcular otros estadisticos a partir de los primeros
  estadisticas <- estadisticas %>%
    dplyr::mutate(pseudo_desviacion_estandar = (p75 - p25) / 1.349,
                  rango_intercuartil = p75 - p25)
  
  # 7. Devolver limites
  return (estadisticas)
  
}

# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definicion de funcion DetectarAnalizarSecuencias() ---- 

# Esta función detecta secuencias de valores repetidos y retorna una tabla con los indices 
# de comienzo y final, largo y duracion de ocurrencias consecutivas de los valores repetidos

DetectarAnalizarSecuencias <- function(fechas, valores) {
  
  # Detectar secuencias y computar fechas de inicio e fin, índices y largo de las secuencias detectadas
  sec.rep <- base::rle(valores)                           # Funcion rle() encuentra secuencias de valores repetidos
  largo.sec <- sec.rep$lengths                		        # Largo de las secuencias (puede ser 1)
  indice.fin <- as.integer(cumsum(largo.sec))	            # Indices de fin de secuencias
  indice.cmzo <- as.integer((indice.fin - largo.sec) + 1)	# Indices de comienzos de secuencias
  fecha.cmzo <- fechas[indice.cmzo]                       # Fecha de comienzo de secuencias
  fecha.fin <- fechas[indice.fin]                         # Fecha de fin de secuencias
  
  # Armar data frame con resultados
  secuencias <- tibble::tibble(
    fecha.cmzo = fecha.cmzo,
    fecha.fin = fecha.fin,
    cmzo = indice.cmzo,
    fin = indice.fin,
    largo = largo.sec,
    valor = valores[indice.cmzo]
  )
  
  return (secuencias)
  
}
# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definicion de funcion AgregarNdias() ---- 

# Esta función agrega n_dias (n registros), de un tibble a otro (al principio o al final)

AgregarNdias <- function(al_tibble, del_tibble, n_dias, 
                         lugar = c("solo.al.inicio", "solo.al.final")) {
  
  if (base::missing(lugar)) lugar <- "inicio.y.final"
  
  # Registros a ser agregados al inicio
  regs.al.inicio <- del_tibble %>% 
    dplyr::filter( fecha >= min(al_tibble$fecha) - lubridate::days(n_dias) & 
                     fecha <  min(al_tibble$fecha) ) 
  
  # Registros a ser agregados al final
  regs.al.final <- del_tibble %>% 
    dplyr::filter( fecha >  max(al_tibble$fecha) & 
                     fecha <= max(al_tibble$fecha) + lubridate::days(n_dias) ) 
  
  registros.a.agregar <- switch(lugar,
                               "solo.al.inicio" = regs.al.inicio,
                               "solo.al.final"  = regs.al.final,
                               "inicio.y.final" = dplyr::union(regs.al.inicio, regs.al.final))
  
  # Sino, se agregan los registros y se retorna el resultado
  return (al_tibble %>% dplyr::union(registros.a.agregar) %>% dplyr::arrange(fecha))
  
}

# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definicion de funcion AgregarNmeses() ---- 

# Esta función agrega n_meses (n registros), de un tibble a otro (al principio o al final)

AgregarNmeses <- function(al_tibble, del_tibble, n_meses, 
                          lugar = c("solo.al.inicio", "solo.al.final")) {
  
  if (base::missing(lugar)) lugar <- "inicio.y.final"
  
  # --- La fecha inferior es el primer día del mes anterior al primer mes pendiente
  fecha.inf <- lubridate::floor_date(min(al_tibble$fecha), unit="month") %m-% 
    base::months(n_meses)
  # --- La fecha superior es el último día del mes posterior al ultimo mes pendiente
  fecha.sup <- lubridate::ceiling_date(max(al_tibble$fecha), unit="month") %m+% 
    base::months(n_meses) - lubridate::days(1) # -1 para tomar el último día del mes
  
  # Registros a ser agregados al inicio
  regs.al.inicio <- del_tibble %>% 
    dplyr::filter( fecha >= fecha.inf & fecha < min(al_tibble$fecha) ) 
  
  # Registros a ser agregados al final
  regs.al.final <- del_tibble %>% 
    dplyr::filter( fecha > max(al_tibble$fecha) & fecha <= fecha.sup ) 
  
  registros.a.agregar <- switch(lugar,
                               "solo.al.inicio" = regs.al.inicio,
                               "solo.al.final"  = regs.al.final,
                               "inicio.y.final" = dplyr::union(regs.al.inicio, regs.al.final))
  
  # Sino, se agregan los registros y se retorna el resultado
  return (al_tibble %>% dplyr::union(registros.a.agregar) %>% dplyr::arrange(fecha))
  
}

# ----------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------#
# --- Definicion de funcion SmartBind() ---- 

# Hace una union sobre dos conjuntos de registros, pero teniendo cuidado de que no haya
# cuando dos resgistros tengan todo igual, salvo la columna objetivo 

SmartBind <- function(registros.preferentes, registros.complementarios, columna.objetivo) {
  
  # Se obtienen las columnas para el anti_join
  columnas_join <- names(registros.preferentes) %>% dplyr::setdiff(columna.objetivo)
  
  # Se excluyen de los registros complementarios, aquellos que ya estan en preferentes 
  registros.a.agregar <- registros.complementarios %>% 
    dplyr::anti_join(registros.preferentes, by = columnas_join)
  
  # Si no hay datos que agregar, se retornan los pendientes sin cambios
  if (nrow(registros.a.agregar) == 0) return (registros.preferentes)
  
  # Sino, se agregan los registros y se retorna el resultado
  return (registros.a.agregar %>% dplyr::union(registros.preferentes))
  
}

# ----------------------------------------------------------------------------------------

