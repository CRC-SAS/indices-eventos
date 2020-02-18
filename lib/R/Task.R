require(doSNOW)
require(foreach)
require(iterators)
require(purrr)
require(R6)
require(RPostgres)
require(snow)
require(utils)

Task <- R6Class("Task",
  private = list(
    parent.script = NULL,
    start.time    = NULL,
    end.time      = NULL,
    func.name     = NULL,
    db.config     = NULL,
    facades       = NULL,
    packages      = NULL,
    timeout.value = NULL,
    errors        = NULL,
    warnings      = NULL,
    log.warnings  = NULL
  ),
  public = list(
    initialize = function(parent.script,func.name, db.config = NULL, packages = NULL,
                          facades = NULL, timeout.value = NULL, log.warnings = FALSE) {
      if (is.null(parent.script) || (class(parent.script)[1] != "Script")) {
        stop("Script padre invalido")
      }
      if (is.null(func.name) || (class(func.name)[1] != "character")) {
        stop("Nombre de funcion invalida")
      }
      if ((is.null(facades) && ! is.null(db.config)) || (! is.null(facades) && is.null(db.config))) {
        stop("Para especificar facades, debe ingresar los datos de acceso a la base de datos")
      }

      private$parent.script <- parent.script
      private$func.name     <- func.name
      private$db.config     <- db.config
      private$packages      <- packages
      private$facades       <- facades
      private$timeout.value <- timeout.value
      private$errors        <- list()
      private$warnings      <- list()
      private$log.warnings  <- log.warnings
    },

    getFuncName = function() {
      return (private$func.name)
    },

    getStartTime = function() {
      return (private$start.time)
    },

    getEndTime = function() {
      return (private$end.time)
    },

    getErrors = function() {
      return (private$errors)
    },
    
    getWarnings = function() {
      return (private$warnings)
    },

    getDBConfig = function() {
      return (private$db.config)
    },

    getPackages = function() {
      return (private$packages)
    },

    getFacades = function() {
      return (private$facades)
    },

    getTimeoutValue = function() {
      return (private$timeout.value)
    },

    run = function(number.of.processes, input.values, ...) {
      # 1. Crear progress bar y convertir valores de input a iterator en caso de ser un data.frame
      number.of.values <- NULL
      if ("data.frame" %in% class(input.values)) {
        number.of.values <- nrow(input.values)
        input.values     <- iterators::iter(obj = input.values, by = 'row')
      } else if (is.vector(input.values) || is.list(input.values)) {
        number.of.values <- length(input.values)
      } else {
        stop("La variable input.values debe ser un data.frame, lista o vector")
      }
      progressBar <- utils::txtProgressBar(max = number.of.values, style = 3)
      progressBarFunction <- function(n) {
        setTxtProgressBar(progressBar, n)
      }
      snowOptions <- list(progress = progressBarFunction)

      # 2. Extraer funciones del global environment
      functions <- purrr::keep(
        .x = base::ls(envir = base::globalenv()),
        .p = function(obj.name) {
          obj <- base::get(obj.name, envir = base::globalenv())
          return ("function" %in% class(obj))
        }
      )

      # 3. Crear cluster
      output.file <- paste0(private$parent.script$getRunDir(), "/",
                            private$parent.script$getName(), "-",
                            private$func.name, ".out")
      cluster  <- snow::makeCluster(type = "SOCK",
                                    spec = rep('localhost', length.out = number.of.processes),
                                    outfile = output.file)
      doSNOW::registerDoSNOW(cluster)  # register cluster as backend for the %dopar% function
      
      # x. Configuración del logger (para que no se solapen las salidas al log)
      log.file <- paste0(private$parent.script$getRunDir(), "/", 
                         private$parent.script$getName(), "-",
                         private$func.name, ".log")
      loginit <- function(logfile) flog.appender(appender.file(logfile))
      foreach(input=rep(log.file, number.of.processes), .packages='futile.logger') %dopar% loginit(input)

      # 4. Ejecutar procesamiento paralelo e informar resultados
      private$start.time <- Sys.time()
      facades            <- private$facades
      rvs <- foreach::foreach(input.value = input.values, .packages = private$packages,
                              .options.snow = snowOptions, .errorhandling = 'pass',
                              .export = functions, .verbose = FALSE) %dopar% {
        params    <- list(...)
        child.con <- NULL
        tryCatch({
          # i. Crear conexion a base de datos y hacer setConnection en facades
          if (! is.null(facades)) {
            Sys.setenv(TZ = "UTC")
            child.con <- DBI::dbConnect(drv = RPostgres::Postgres(),
                                        dbname = private$db.config$name,
                                        user = private$db.config$user,
                                        password = private$db.config$pass,
                                        host = private$db.config$host)
            DBI::dbExecute(child.con, "SET TIME ZONE 'UTC'")

            for (facade.name in names(facades)) {
              a.facade <- facades[[facade.name]]
              a.facade$setConnection(child.con)
              params[[facade.name]] <- a.facade
            }
          }

          # ii. Agregar parametros default
          params$script      <- private$parent.script
          params$input.value <- input.value

          # iii. Ejecutar codigo
          return.value <<- NULL
          warnings.obj <<- NULL

          base::withCallingHandlers({
            if (! is.null(private$timeout.value)) {
              return.value <<- R.utils::withTimeout( {
                base::do.call(what = private$func.name, args = params, quote = TRUE)
              }, timeout = private$timeout.value, onTimeout = "warning")
            } else {
              return.value <<- base::do.call(what = private$func.name, args = params, quote = TRUE)
            }
          }, warning = function(w) {
            if (private$log.warnings) {
              private$parent.script$warn(paste0("Warning procesando objeto ", base::toString(input.value[1:2])))  
              private$parent.script$warn(paste0("- MESSAGE: ", w$message))
              private$parent.script$warn(paste0("- CALL: ", private$func.name, "(), ", w$call[1], "()"))
              warnings.obj <<- base::append(warnings.obj, w$message)
            }
          })

          # iv. Devolver resultado
          return (list(input.value = input.value, output.value = return.value, error = NULL, warnings = warnings.obj))

        }, error = function(e) {
          e <- base::gsub("\r?\n|\r", " ", e)
          m <- paste0("Error procesando para objeto ", base::toString(input.value[1:2]), ". Error: ", e)
          private$parent.script$error(m, abort.execution = FALSE)
          return (list(input.value = input.value, output.value = NULL, error = as.character(e), warnings = warnings.obj))
        }, finally = {
          if (! is.null(child.con)) {
            DBI::dbDisconnect(child.con)
          }
        })
      
      } # fin foreach
      foreach::registerDoSEQ()  # unregister cluster as backend for the %dopar% function
      snow::stopCluster(cluster)  # close/stop cluster
      private$end.time <- Sys.time()
      
      close(progressBar) # se finaliza/cierra la barra de progreso

      # 5. Separar resultados de errores y warnings. Guardar errores y warnings. Devolver resultados.
      results <- purrr::keep(
        .x = rvs,
        .p = function(x) { return (is.null(x$error)) }
      ) %>% purrr::map(
        .f = function(x) { return (x$output.value) }
      )
      private$errors <- purrr::keep(
        .x = rvs,
        .p = function(x) { return (! is.null(x$error)) }
      )
      private$warnings <- purrr::keep(
        .x = rvs,
        .p = function(x) { return (! is.null(x$warnings)) }
      )
      
      return (results)
    }
  )
)
