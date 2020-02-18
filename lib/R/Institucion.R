require(rlang)
require(R6)
require(dplyr)
require(dbplyr)

InstitucionFacade <- R6Class("InstitucionFacade",
	inherit = Facade,
	public = list(
		initialize = function(con) {
			private$con <- con
		},
  
	  buscar = function(id = NULL, siglas = NULL) {
	    filtros       <- list(id = id, siglas = siglas)
	    instituciones <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'institucion')), filtros)
	    return (dplyr::collect(instituciones))
	  }
	)
)