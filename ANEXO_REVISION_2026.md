# Revisión posterior: Spark y Scala: preguntas de felicidad y renta

**Fecha: 14/11/2026. Anexo separado de la entrega original.**

Después de retomar esta práctica con más perspectiva y herramientas, he vuelto a revisar cómo la resolví al empezar. Mantengo aquella entrega como parte de mi aprendizaje y recojo aquí una versión posterior que corrige los puntos identificados. Esta revisión se ha preparado con apoyo de IA y con las comprobaciones indicadas al final.

Es una revisión actual. No se atribuye su uso a una fecha anterior ni se presenta como un despliegue o un resultado de trabajo que no se haya comprobado.

## Archivos de esta versión

- [ANEXO_REVISION_2026.scala](ANEXO_REVISION_2026.scala)

## Qué cambia y por qué

El anexo incorpora funciones separadas para las seis preguntas: ganadores de 2021, ganadores por región, años en cabeza, posición en felicidad del país con mayor renta en 2020, comparación entre fuentes y esperanza de vida saludable. Se convierten valores numéricos, se conservan empates y se rechazan claves país-año duplicadas.

Las regiones del informe se llaman regiones. Los países con mayor renta se seleccionan desde los datos, sin escribir Irlanda a mano. La comparación de renta usa países comunes y valores en escala original, bajo el supuesto explícito de logaritmos naturales.

La edición de un informe no demuestra el año de referencia de todos sus indicadores. Por defecto, `compararEdiciones` devuelve una comparación descriptiva y deja el porcentaje a NULL. Solo con `periodosVerificados=true`, después de contrastar la documentación de los datos, calcula la variación de la media simple de países comunes. Eso no es el crecimiento del PIB mundial.

## Cómo usarla

```text
spark-shell --master local[1] --conf spark.sql.shuffle.partitions=2
:load ANEXO_REVISION_2026.scala
RevisionFelicidad2026.autoprueba(spark)
```

Para datos reales, cargar los dos CSV con `leerHistorico(spark,ruta)` y `leer2021(spark,ruta)`, y pasar esos DataFrames a las funciones. No se necesitan las rutas DBFS antiguas. El anexo conserva las columnas de las fuentes indicadas en sus dos lectores; si no coinciden con tu edición, hay que revisar el contrato de datos.

## Qué se ha comprobado

8 comprobaciones ejecutadas en Spark 3.5.7 y Scala 2.12.18, con Java 21, usando datos sintéticos. Se cargó el archivo Scala real en spark-shell. Se comprobaron empates, regiones, clave duplicada, selección del país por renta, dato de 2019 y una variación conocida del 10% en la muestra artificial.

**Pendiente:** recuperar y contrastar los CSV originales, sus periodos y metodología. No se han recalculado las cifras históricas de la práctica ni ejecutado una sesión de Databricks.

## La entrega inicial sigue aquí

Los scripts, notebooks y archivos originales conservan su nombre y contenido. El README y REVISION.md explican sus límites; este anexo ofrece la alternativa. Se puede estudiar la evolución sin borrar el punto de partida.
