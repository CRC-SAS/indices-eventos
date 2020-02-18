require(rlang)
require(R6)
require(dplyr)
require(dbplyr)

VariableFacade <- R6Class("VariableFacade",
	inherit = Facade,
	public = list(
		initialize = function(con) {
			private$con <- con
		},
  
	  buscar = function() {
	    variables <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'variable')) %>%
	      dplyr::arrange(orden_columna)
	    return (dplyr::collect(variables))
	  }
	)
)