# Scripts para la caracterización de la amenaza climática de sequías 


El presente repositorio contiene los cripts para realizar el análisis probabilista de amenaza de sequías desarrollado en el marco de la Cooperación Técnica RG-T3308, “Diseño e Implementación de un Sistema de Información sobre Sequías para el Sur de América del Sur (SISSA)”  

El repositorio contiene cuatro carpetas princiaples: 

* data: Ubicación donde se encuentran los datos de entradas
* EstadísticasMoviles: Contiene los scipts necesarios para la agregación en alta frecuencia. Además, se encuentra un documento que explica el mecanismo de agregación de variables (precipitación y temperaturas máxima y mínima) utilizando ventanas móviles basadas en péntadas.
* IndicesSequia: Contiene los scripts para el cálculo de índices de sequía en alta frecuencia. Además, se encuentra un documento que detalla los índices que se utilizan para el análisis de las sequías y caracterización de los eventos secos. Se describe cada uno de ellos, su forma de cálculo y las adaptaciones realizadas para ser usados con series climáticas sintéticas. Tambiién se explican las escalas temporales utilizadas y el uso de un período de referencia para determinar una climatología de precipitaciones y balances hídricos. 
* IdentificarEventos: Contiene los scripts para la identificación de eventos secos o humedos a partir de indices de sequía estandarizados. Además, se encuentra un documento que  define el concepto de evento seco, como identificarlos y las métricas que lo caracterizan.
