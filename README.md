# Scripts para la caracterización de la amenaza climática de sequías 


El presente repositorio contiene los scripts para realizar el análisis probabilista de amenaza de sequías desarrollado en el marco de la Cooperación Técnica RG-T3308, “Diseño e Implementación de un Sistema de Información sobre Sequías para el Sur de América del Sur (SISSA)”  

El repositorio contiene cuatro carpetas principales: 

* data: Ubicación donde se encuentran los datos de entrada.
* EstadísticasMoviles: Contiene los scripts necesarios para la agregación diferentes escalas temporales. Además, se encuentra un documento que explica el mecanismo de agregación de variables (precipitación y temperaturas máxima y mínima) utilizando ventanas móviles basadas en péntadas.
* IndicesSequia: Contiene los scripts para el cálculo de índices de sequía en alta frecuencia. Además, se incluye un documento: a) que detalla los índices que se utilizan para el análisis de las sequías y b) caracterización de los eventos secos. Se describe para cada uno de los índices: su forma de cálculo y las adaptaciones realizadas para el cálculo basado en series climáticas sintéticas. También se explican las escalas temporales utilizadas y el uso de un período de referencia para determinar una climatología de precipitaciones y balances hídricos. 
* IdentificarEventos: Contiene los scripts para la identificación de eventos secos o humedos a partir de indices de sequía estandarizados. Además, se encuentra un documento que  define el concepto de evento seco, como identificarlos y las métricas que lo caracterizan.

