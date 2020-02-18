require(rlang)
require(R6)
require(dplyr)
require(dbplyr)

PaisFacade <- R6Class("PaisFacade",
	inherit = Facade,
	public = list(
		initialize = function(con) {
			private$con <- con
		},
  
	  buscar = function(id = NULL) {
	    filtros <- list(id = id)
	    paises  <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'pais')), filtros)
	    return (dplyr::collect(paises))
	  }
	)
)