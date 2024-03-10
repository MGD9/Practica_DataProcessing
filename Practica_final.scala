// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

val df1 = spark.read.option("header", "true").csv("dbfs:/FileStore/dbfs:/FileStore/Practica/world_happiness_report_2021.csv")
val df2 = spark.read.option("header", "true").csv("dbfs:/FileStore/dbfs:/FileStore/Practica/world_happiness_report.csv")

// COMMAND ----------

display(df1)

// COMMAND ----------

display(df2)

// COMMAND ----------

df1.printSchema()

// COMMAND ----------

df1.show(5)

// COMMAND ----------

df2.printSchema()
df2.show(5)

// COMMAND ----------

// 1. ¿Cuál es el país más “feliz” del 2021 según la data?
val happiestCountry2021 = df1.select($"Country name", $"Ladder score".cast("double"))
  .orderBy(desc("Ladder score"))
  .first()

// COMMAND ----------

// 2. ¿Cuál es el país más “feliz” del 2021 por continente según la data?
val happiestCountryByContinent = df1.join(
    df1.groupBy($"Regional indicator").agg(max($"Ladder score").alias("Max Ladder Score")),
    Seq("Regional indicator")
  )
  .select($"Country name", $"Regional indicator", $"Ladder score")
  .where($"Ladder score" === $"Max Ladder Score")

happiestCountryByContinent.show()

// COMMAND ----------

/// 3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

val maxLifeLadderByYear = df2.groupBy($"year")
                              .agg(max("Life Ladder").alias("maxLifeLadder"))
                              .join(df2, Seq("year"))
                              .where($"Life Ladder" === $"maxLifeLadder")
                              .select($"year", $"Country name".alias("CountryWithMaxLifeLadder"))
maxLifeLadderByYear.show()

// COMMAND ----------

// Contar cuántas veces aparece cada país en la lista
val countryCount = maxLifeLadderByYear.groupBy($"CountryWithMaxLifeLadder")
                                      .count()
                                      .orderBy(desc("count"))

countryCount.show()


// COMMAND ----------

// Obtener el país que más veces ocupó el primer lugar en todos los años
val mostFrequentCountry = countryCount.select($"CountryWithMaxLifeLadder")
                                      .first()
                                      .getString(0)

// COMMAND ----------

// Imprimir el resultado
println(s"El país que más veces ocupó el primer lugar en todos los años es: $mostFrequentCountry")

// COMMAND ----------

///4 ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?
///Saber que país es el que mas GDP tiene del 2020
val countryWithMaxGDP = df2.filter($"year" === 2020)
  .select($"Country name", $"Log GDP per capita".cast("double"))
  .orderBy(desc("Log GDP per capita"))
countryWithMaxGDP.show()

// COMMAND ----------

// Filtrar los datos del año 2020
val df2020 = df2.filter($"year" === 2020)

// Ordenar los datos por Life Ladder en orden descendente
val df2020Ranked = df2020.orderBy(desc("Life Ladder"))

// Encontrar el puesto de Irlanda en el ranking
val positionOfIreland = df2020Ranked.select("Country name")
                                    .collect()
                                    .map(_.getString(0))
                                    .indexOf("Ireland") + 1

println(s"La posición de Ireland en el ranking de Life Ladder del año 2020 es: $positionOfIreland")


// COMMAND ----------

///5 ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

// Calcular el promedio de Log GDP per capita para el año 2020 en df2
val avgGDP_2020 = df2.filter($"year" === 2020)
                          .select(avg($"Log GDP per capita").alias("avgLogGDP_2020"))
                          .first()
                          .getDouble(0)


// COMMAND ----------

// Calcular el promedio de Logged GDP per capita para el año 2020 en df1
val avgGDP_2021 = df1.select(avg($"Logged GDP per capita").alias("avgLoggedGDP_2020"))
                          .first()
                          .getDouble(0)

// COMMAND ----------

// Calcular la variación porcentual del PIB promedio entre 2020 y 2021
val percentageChange = ((avgGDP_2020 - avgGDP_2021) / avgGDP_2021) * 100

// COMMAND ----------

// Determinar si el PIB promedio aumentó o disminuyó
val changeStatus = if (avgGDP_2021 > avgGDP_2020) "aumentó" else "disminuyó"

// COMMAND ----------

// Imprimir los resultados
println(s"El promedio del GDP en el año 2020 según df2 fue: $avgGDP_2020")
println(s"El promedio del GDP en el año 2021 según df2 fue: $avgGDP_2021")
println(f"La variación porcentual del GDP promedio del 2020 al 2021, $changeStatus un $percentageChange%.2f%%.")

// COMMAND ----------

///6 ¿Cuál es el país con mayor expectativa de vide (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia en ese indicador en el 2019?

// País con el valor más alto en la columna "Healthy life expectancy at birth"
val countryWithHighestLifeExpectancy = df2.orderBy(desc("Healthy life expectancy at birth"))
                                           .select($"Country name")
                                           .first()
                                           .getString(0)

// COMMAND ----------

// Encontrar la expectativa de vida en el año 2019 para el país con la mayor expectativa de vida
val lifeExpectancy2019 = df2.filter($"Country name" === countryWithHighestLifeExpectancy && $"year" === 2019)
                             .select($"Healthy life expectancy at birth")
                             .first()
                             .getString(0)

// COMMAND ----------

// Imprimir los resultados
println(s"El país con la mayor expectativa de vida es: $countryWithHighestLifeExpectancy")
println(s"En el año 2019, la expectativa de vida en $countryWithHighestLifeExpectancy fue de: $lifeExpectancy2019")
