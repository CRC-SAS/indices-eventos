require(rlang)
require(R6)
require(dplyr)
require(dbplyr)

UsuarioFacade <- R6Class("UsuarioFacade",
	inherit = Facade,
	public = list(
		initialize = function(con) {
			private$con <- con
		},
  
	  buscar = function(id = NULL, login = NULL) {
	    filtros  <- list(id = id, login = login)
	    usuarios <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'usuario')), filtros)
	    return (dplyr::collect(usuarios))
	  },
		
		buscarServiciosPermitidos = function(id) {
		  servicios <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'servicio')) %>%
		    dplyr::inner_join(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'usuario_servicio')),
		                      by = c("id" = "servicio_id")) %>%
		    dplyr::filter(usuario_id == id) %>%
		    dplyr::select(-usuario_id)
		  return (dplyr::collect(servicios))
		},
		
		puedeAccederAServicio = function(usuario_id, url_servicio) {
		  servicios <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'servicio')) %>%
		    dplyr::inner_join(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'usuario_servicio')),
		                      by = c("id" = "servicio_id")) %>%
		    dplyr::filter(usuario_id == !!usuario_id & url == url_servicio) %>%
		    dplyr::collect() 
		  return (nrow(servicios) > 0)
		}
	)
)