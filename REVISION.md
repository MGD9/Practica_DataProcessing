# Revisión técnica · 14/11/2026

Se ha inspeccionado `Practica_final.scala`. Esta revisión no ejecuta Spark ni valida las cifras que muestra la entrega antigua.

| Hallazgo | Consecuencia | Corrección necesaria |
| --- | --- | --- |
| CSV con cabecera, sin esquema ni inferencia explícita. | Varias columnas quedan como texto; máximos y ordenaciones pueden ser lexicográficos. | Convertir campos a tipos numéricos y verificar nulos. |
| «Continente» se obtiene de `Regional indicator`. | La etiqueta describe regiones del informe, no continentes. | Nombrar correctamente la agrupación o aportar una correspondencia geográfica documentada. |
| Ranking de 2020 busca `Ireland` literalmente. | No depende del país que realmente maximiza la renta. | Seleccionar el país desde los datos y definir cómo tratar empates. |
| Variación usa cociente de medias de logaritmos y signo/base invertidos. | El porcentaje publicado no mide el crecimiento indicado. | Definir indicador y universo antes de calcular `(nuevo - anterior) / anterior * 100`. |
| Se comparan histórico y corte 2021. | Puede cambiar el conjunto de países y la comparabilidad de los valores. | Emparejar países, revisar metodología, unidades y valores ausentes. |
| Esperanza de vida se ordena sin convertir y se elige máximo sobre todo el histórico. | Posible selección incorrecta o mezcla de años. | Tipar, definir año/universo y validar la existencia de 2019. |
| Uso de `.first()` sin comprobar ausencia de resultados. | Puede fallar con datos vacíos o incompletos. | Incorporar controles y mensajes claros. |

Exponenciar un logaritmo puede recuperar su escala si se confirma su definición, pero la media simple de países no representa por sí sola el PIB mundial ni el PIB mundial por habitante. No sustituir una cifra errónea por otra sin una definición válida.

Para validar después: cargar los CSV originales con procedencia y versión, comprobar unicidad país-año y cobertura común, contrastar resultados y probar empates, nulos y entradas vacías. El DBC sigue siendo el original.
