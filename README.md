# Procesamiento de datos con Spark y Scala

En esta práctica de KeepCoding trabajé con datos del World Happiness Report para comparar países, clasificaciones de felicidad, renta por persona y esperanza de vida. Fue mi primer acercamiento a resolver preguntas de datos con Spark en Databricks.

**Estado:** entrega original de formación. La revisión documental identifica errores que deben corregirse antes de reutilizar los resultados. El notebook Scala y el archivo DBC se conservan sin cambios.

## Archivos

| Archivo | Qué contiene |
| --- | --- |
| [Practica_final.scala](Practica_final.scala) | Notebook exportado como código Scala, con celdas de Databricks. |
| [Practica_final.dbc](Practica_final.dbc) | Archivo de la entrega para importar en Databricks. No es una versión corregida. |
| [Entrega original](ENTREGA-ORIGINAL.md) | El README anterior, con sus consultas y salidas históricas. |
| [Revisión técnica](REVISION.md) | Problemas detectados, datos necesarios y validación pendiente. |

## Preguntas de la práctica

1. Qué país tiene la puntuación de felicidad más alta en 2021.
2. Qué país encabeza cada región del informe. Las regiones del dataset no son continentes.
3. Qué país aparece más veces en el primer puesto anual.
4. Qué posición ocupa en felicidad el país con mayor renta por persona de 2020.
5. Cómo comparar la renta por persona entre 2020 y 2021.
6. Qué país alcanza la mayor esperanza de vida saludable y su dato de 2019.

## Cómo abrirlo

Necesitas un entorno con Spark y Scala; el original utiliza funciones de Databricks, como `display`, por lo que no es un programa Scala independiente.

1. Importa `Practica_final.scala` como notebook, o abre el DBC para consultar la entrega histórica.
2. Aporta los dos CSV de clase: `world_happiness_report_2021.csv` y `world_happiness_report.csv`. No están incluidos.
3. Ajusta las dos rutas de lectura al lugar donde los hayas guardado. Las rutas originales llevan `dbfs:/FileStore/` duplicado y no son portables.
4. Revisa esquema, tipos y los puntos pendientes antes de ejecutar y sacar conclusiones.

No se ha ejecutado el notebook en Databricks durante esta limpieza. Tampoco se han descargado los CSV ni confirmado las cifras de las salidas antiguas.

## Lo importante de la revisión

El cálculo original de variación de PIB opera sobre logaritmos, invierte la fórmula y compara muestras que podrían ser distintas. Su porcentaje no debe usarse como crecimiento mundial del PIB. También hay ordenaciones de valores numéricos leídos como texto y una selección de Irlanda escrita a mano.

Queda explicado en la revisión para que el siguiente paso sea corregir y validar el análisis con los datos adecuados, conservando esta entrega como referencia de mi aprendizaje.

[Volver a mi perfil](https://github.com/MGD9).
