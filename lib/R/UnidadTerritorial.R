require(R6)
require(dplyr)
require(dbplyr)
require(magrittr)
require(purrr)
require(rgeos)
require(sf)
require(sp)

UnidadTerritorialFacade <- R6Class("UnidadTerritorialFacade",
	inherit = Facade,
	private = list(
	  proj4string = NULL,
	  as.SpatialPolygonsDataFrame = function(data) {
	    campo.poligono  <- "poligono_wkt"
	    datos           <- data %>%
	      dplyr::select(-dplyr::one_of(campo.poligono))
	    rownames(datos) <- data$id
	    poli.spdf <- purrr::map(
	      .x = seq(from = 1, to = nrow(data)),
	      .f = function(row_index) {
	        poligono.wkt <- data[row_index, campo.poligono]
	        poligono     <- rgeos::readWKT(text = poligono.wkt, id = data[row_index, "id"], p4s = private$proj4string)
	        return (sp::SpatialPolygonsDataFrame(Sr = poligono, data = datos[row_index, ]))
	      }
	    )
	    
	    spdf <- do.call(what = "rbind.SpatialPolygonsDataFrame", args = poli.spdf)
	    return (spdf)
	  },
	  as.SimpleFeature = function(data, agr) {
	    sf.object <- sf::st_as_sf(data, crs = private$proj4string, wkt = "poligono_wkt", agr = agr)
	    return (sf.object)
	  }
	),
	public = list(
	  Atributo = list(
	    "ZonaCRC" = 1
	  ),
	  AtributoOp = list("OR" = "OR", "AND" = "AND"),
		initialize = function(con, proj4string) {
			private$con         <- con
			private$proj4string <- proj4string
		},
		
		buscarUnidadesAdministrativas = function(id = NULL, pais_id = NULL, unidad_padre_id = NULL, nivel = NULL,
	                                           nombre = NULL, sin.poligonos = FALSE,
	                                           atributos.valor = NULL, atributos.op = NULL) {
	    # a. Buscar metadatos
	    if (! is.null(nivel)) {
	      nivel <- paste0('N', nivel)
	    }
	    filtros  <- list(id = id, pais_id = pais_id, unidad_padre_id = unidad_padre_id, nivel = nivel, nombre = nombre)
	    unidades <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'unidad_administrativa')), filtros) %>%
	      dplyr::collect()
	    
	    # b. Filtrar por atributos
	    if (! is.null(atributos.valor) && ! is.null(atributos.op)) {
	      filter.func <- NULL
	      if (atributos.op == self$AtributoOp$OR) {
	        filter.func <- (function(atributos.valor) {
	          return (function(atributos) {
	            return (purrr::map(
	              .x = atributos,
	              .f = function(atributo) {
	                return (any(bitwAnd(atributo, atributos.valor) != 0))
	              }
	            ) %>% unlist())  
	          })
	        })(as.integer(atributos.valor))
	      } else if (atributos.op == self$AtributoOp$AND) {
	        filter.func <- (function(atributos.valor) {
	          return (function(atributos) {
	            return (purrr::map(
	              .x = atributos,
	              .f = function(atributo) {
	                return (all(bitwAnd(atributo, atributos.valor) != 0))
	              }
	            ) %>% unlist())  
	          })
	        })(as.integer(atributos.valor))
	      }
	      
	      if (! is.null(filter.func)) {
	        unidades <- unidades %>%
	          dplyr::filter(filter.func(atributos))
	      } else {
	        # Operacion indefinida
	        warning(paste0("Operacion indefinida sobre atributos: ", atributos.op))
	        return (NULL)
	      }
	    }
	    
	    # c. Buscar poligonos (si es que es necesario)
	    if (sin.poligonos) {
	      return (unidades)
	    } else if (nrow(unidades) > 0) {
	      query     <- paste0("SELECT id, ST_AsText(poligono) poligono_wkt ",
	                          "FROM unidad_administrativa_poligono ",
	                          "WHERE id in (", paste0(unique(unidades$id), collapse = ", "), ")")
	      poligonos <- DBI::dbSendQuery(con = private$con, statement = query) %>%
	        DBI::dbFetch(n = -1) %>%
	        dplyr::inner_join(unidades, by = "id")
	      agr       <- c(
	        "id" = "identity",
	        "nivel" = "identity",
	        "pais_id" = "identity",
	        "unidad_padre_id" = "identity",
	        "nombre" = "identity",
	        "atributos" = "identity"
	      )
	      return (private$as.SimpleFeature(poligonos, agr))
	    } else {
	      return (NULL)
	    }
		},
		
		setAtributos = function(unidades.administrativas, atributos) {
		  query <- paste0("UPDATE unidad_administrativa set atributos = ", as.integer(atributos), " WHERE ",
		                  "id in (", paste0(unique(unidades.administrativas$id), collapse = ", "), ")")
		  return (DBI::dbSendQuery(private$con, query))
		}
	)
)