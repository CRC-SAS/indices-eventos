

IdentificarIdColumn <- function(ubicacion) {
  if ("station_id" %in% colnames(ubicacion)) {
    id_column <- "station_id"
  } else if ("point_id" %in% colnames(ubicacion)) {
    id_column <- "point_id"
  } else {
    id_column <- ubicacion %>% dplyr::select(dplyr::ends_with('_id')) %>% 
      base::colnames() %>% base::sort() %>% dplyr::first()
  }
  return (id_column)
}