# Practica_DataProcessing

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6775690556640646/4319931110963560/2363052867088684/latest.html


# 1. ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score”
mayor número más feliz es el país)

El país más feliz del 2021 es: Finland

## código:

val country_happiness_2021 = df1.select("Country name", "Ladder score")
val happiest_country_2021 = country_happiness_2021.orderBy(desc("Ladder score")).select("Country name").first().getString(0)
println(s"El país más feliz del 2021 es: $happiest_country_2021")

# 2. ¿Cuál es el país más “feliz” del 2021 por continente según la data?

+--------------------+--------------------+------------+
|        Country name|  Regional indicator|Ladder score|
+--------------------+--------------------+------------+
|             Finland|      Western Europe|       7.842|
|         New Zealand|North America and...|       7.277|
|              Israel|Middle East and N...|       7.157|
|          Costa Rica|Latin America and...|       7.069|
|      Czech Republic|Central and Easte...|       6.965|
|Taiwan Province o...|           East Asia|       6.584|
|           Singapore|      Southeast Asia|       6.377|
|          Uzbekistan|Commonwealth of I...|       6.179|
|           Mauritius|  Sub-Saharan Africa|       6.049|
|               Nepal|          South Asia|       5.269|
+--------------------+--------------------+------------+


## código:

val happiestCountryByContinent = df1.join(
    df1.groupBy($"Regional indicator").agg(max($"Ladder score").alias("Max Ladder Score")),
    Seq("Regional indicator")
  )
  .select($"Country name", $"Regional indicator", $"Ladder score")
  .where($"Ladder score" === $"Max Ladder Score")

happiestCountryByContinent.show()


# 3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?
   
El país que más veces ocupó el primer lugar en todos los años es: Denmark

## código:

val maxLifeLadderByYear = df2.groupBy($"year")
                              .agg(max("Life Ladder").alias("maxLifeLadder"))
                              .join(df2, Seq("year"))
                              .where($"Life Ladder" === $"maxLifeLadder")
                              .select($"year", $"Country name".alias("CountryWithMaxLifeLadder"))
maxLifeLadderByYear.show()

// Contar cuántas veces aparece cada país en la lista
val countryCount = maxLifeLadderByYear.groupBy($"CountryWithMaxLifeLadder")
                                      .count()
                                      .orderBy(desc("count"))

countryCount.show()

// Obtener el país que más veces ocupó el primer lugar en todos los años
val mostFrequentCountry = countryCount.select($"CountryWithMaxLifeLadder")
                                      .first()
                                      .getString(0)

// Imprimir el resultado
println(s"El país que más veces ocupó el primer lugar en todos los años es: $mostFrequentCountry")


# 4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?
La posición de Ireland en el ranking de Life Ladder del año 2020 es: 13

## código:

//Ordenar por GDP descendente
val countryWithMaxGDP = df2.filter($"year" === 2020)
  .select($"Country name", $"Log GDP per capita".cast("double"))
  .orderBy(desc("Log GDP per capita"))
countryWithMaxGDP.show()

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


# 5. ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó
o disminuyó?

La variación porcentual del GDP promedio del 2020 al 2021 disminuyó un 3.38%.

## código:

// Calcular el promedio de Log GDP per capita para el año 2020 en df2
val avgGDP_2020 = df2.filter($"year" === 2020)
                          .select(avg($"Log GDP per capita").alias("avgLogGDP_2020"))
                          .first()
                          .getDouble(0)

// Calcular el promedio de Logged GDP per capita para el año 2020 en df1
val avgGDP_2021 = df1.select(avg($"Logged GDP per capita").alias("avgLoggedGDP_2020"))
                          .first()
                          .getDouble(0)

// Calcular la variación porcentual del PIB promedio entre 2020 y 2021
val percentageChange = ((avgGDP_2020 - avgGDP_2021) / avgGDP_2021) * 100

// Determinar si el PIB promedio aumentó o disminuyó
val changeStatus = if (avgGDP_2021 > avgGDP_2020) "aumentó" else "disminuyó"

// Imprimir los resultados
println(s"El promedio del GDP en el año 2020 según df2 fue: $avgGDP_2020")
println(s"El promedio del GDP en el año 2021 según df2 fue: $avgGDP_2021")
println(f"La variación porcentual del GDP promedio del 2020 al 2021, $changeStatus un $percentageChange%.2f%%.")

# 6. ¿Cuál es el país con mayor expectativa de vide (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia
en ese indicador en el 2019?

El país con la mayor expectativa de vida es: Singapore
En el año 2019, la expectativa de vida en Singapore fue de: 77.100

## código:

// País con el valor más alto en la columna "Healthy life expectancy at birth"
val countryWithHighestLifeExpectancy = df2.orderBy(desc("Healthy life expectancy at birth"))
                                           .select($"Country name")
                                           .first()
                                           .getString(0)

// Encontrar la expectativa de vida en el año 2019 para el país con la mayor expectativa de vida
val lifeExpectancy2019 = df2.filter($"Country name" === countryWithHighestLifeExpectancy && $"year" === 2019)
                             .select($"Healthy life expectancy at birth")
                             .first()
                             .getString(0)

// Imprimir los resultados
println(s"El país con la mayor expectativa de vida es: $countryWithHighestLifeExpectancy")
println(s"En el año 2019, la expectativa de vida en $countryWithHighestLifeExpectancy fue de: $lifeExpectancy2019")
