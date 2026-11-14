// Revisión posterior, 14/11/2026. No sustituye Practica_final.scala ni el DBC.
// Cargar en spark-shell con :load ANEXO_REVISION_2026.scala
// Pruebas sintéticas: RevisionFelicidad2026.autoprueba(spark)
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RevisionFelicidad2026 {
  def revisarClave(df: DataFrame): Unit = {
    require(df.filter(col("pais").isNull || trim(col("pais")) === "" || col("anio").isNull).limit(1).count() == 0,
      "País o año ausente")
    require(df.groupBy("pais", "anio").count().filter(col("count") > 1).limit(1).count() == 0,
      "Hay duplicados país-año: no se eligen ni promedian silenciosamente")
  }

  def leer2021(spark: SparkSession, ruta: String): DataFrame = {
    val raw = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta)
    val df = raw.select(trim(col("Country name")).as("pais"), lit(2021).as("anio"),
      col("Regional indicator").as("region"), col("Ladder score").cast("double").as("felicidad"),
      col("Logged GDP per capita").cast("double").as("logRenta"))
    revisarClave(df); df
  }

  def leerHistorico(spark: SparkSession, ruta: String): DataFrame = {
    val raw = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta)
    val df = raw.select(trim(col("Country name")).as("pais"), col("year").cast("int").as("anio"),
      col("Life Ladder").cast("double").as("felicidad"),
      col("Log GDP per capita").cast("double").as("logRenta"),
      col("Healthy life expectancy at birth").cast("double").as("salud"))
    revisarClave(df); df
  }

  def finita(nombre: String) = col(nombre).isNotNull && !isnan(col(nombre)) && abs(col(nombre)) < lit(Double.PositiveInfinity)

  def ganadores2021(df: DataFrame): DataFrame = {
    revisarClave(df)
    df.filter(col("anio") === 2021 && finita("felicidad"))
      .withColumn("puesto", dense_rank().over(Window.orderBy(col("felicidad").desc)))
      .filter(col("puesto") === 1).orderBy("pais")
  }

  def ganadoresPorRegion(df: DataFrame): DataFrame = {
    revisarClave(df)
    df.filter(col("anio") === 2021 && finita("felicidad") && col("region").isNotNull)
      .withColumn("puesto", dense_rank().over(Window.partitionBy("region").orderBy(col("felicidad").desc)))
      .filter(col("puesto") === 1).orderBy("region", "pais")
  }

  def masPrimerosPuestos(df: DataFrame): DataFrame = {
    revisarClave(df)
    val cuentas = df.filter(finita("felicidad"))
      .withColumn("puesto", dense_rank().over(Window.partitionBy("anio").orderBy(col("felicidad").desc)))
      .filter(col("puesto") === 1).groupBy("pais").agg(countDistinct("anio").as("aniosEnCabeza"))
    cuentas.withColumn("orden", dense_rank().over(Window.orderBy(col("aniosEnCabeza").desc)))
      .filter(col("orden") === 1).orderBy("pais")
  }

  def ranking2020MayorRenta(df: DataFrame): DataFrame = {
    revisarClave(df)
    // El ranking de felicidad incluye todos los países con felicidad conocida.
    val orden = df.filter(col("anio") === 2020 && finita("felicidad"))
      .withColumn("puestoFelicidad", dense_rank().over(Window.orderBy(col("felicidad").desc)))
    val maxRenta = df.filter(col("anio") === 2020 && finita("logRenta")).agg(max("logRenta").as("maxRenta"))
    orden.crossJoin(maxRenta).filter(col("logRenta") === col("maxRenta")).drop("maxRenta").orderBy("pais")
  }

  def compararEdiciones(historico: DataFrame, corte2021: DataFrame, periodosVerificados: Boolean = false): DataFrame = {
    revisarClave(historico); revisarClave(corte2021)
    val a = historico.filter(col("anio") === 2020 && finita("logRenta"))
      .select(col("pais"), exp(col("logRenta")).as("nivelA"))
    val b = corte2021.filter(col("anio") === 2021 && finita("logRenta"))
      .select(col("pais"), exp(col("logRenta")).as("nivelB"))
    val comunes = a.join(b, Seq("pais")).filter(finita("nivelA") && finita("nivelB"))
    require(comunes.limit(1).count() > 0, "No hay países comparables con renta finita")
    comunes.agg(count(lit(1)).as("paisesComunes"), avg("nivelA").as("mediaA"), avg("nivelB").as("mediaB"))
      .withColumn("variacionPct", when(lit(periodosVerificados) && col("mediaA") > 0,
        (col("mediaB") - col("mediaA")) / col("mediaA") * 100).otherwise(lit(null).cast("double")))
      .withColumn("interpretacion", lit(if (periodosVerificados)
        "Variación de la media simple de renta por persona de países comunes; no es PIB mundial"
        else "Comparación descriptiva entre fuentes: periodos y comparabilidad sin verificar; no se calcula crecimiento"))
  }

  def maxSaludY2019(df: DataFrame): DataFrame = {
    revisarClave(df)
    val maximo = df.filter(finita("salud")).agg(max("salud").as("maximo"))
    val paises = df.crossJoin(maximo).filter(col("salud") === col("maximo"))
      .select("pais", "maximo").distinct()
    val datos2019 = df.filter(col("anio") === 2019 && finita("salud"))
      .select(col("pais"), col("salud").as("salud2019"))
    paises.join(datos2019, Seq("pais"), "left").orderBy("pais")
  }

  def autoprueba(spark: SparkSession): Unit = {
    import spark.implicits._
    val h = Seq(("A",2019,6.0,math.log(100.0),80.0), ("A",2020,7.0,math.log(100.0),81.0),
      ("B",2019,7.0,math.log(200.0),82.0), ("B",2020,6.0,math.log(200.0),82.0))
      .toDF("pais", "anio", "felicidad", "logRenta", "salud")
    val c = Seq(("A",2021,"R1",9.0,math.log(110.0)), ("B",2021,"R2",9.0,math.log(220.0)))
      .toDF("pais", "anio", "region", "felicidad", "logRenta")
    assert(ganadores2021(c).count() == 2, "Se conservan empates")
    assert(ganadoresPorRegion(c).count() == 2)
    assert(masPrimerosPuestos(h).count() == 2)
    assert(ranking2020MayorRenta(h).select("pais").as[String].head() == "B")
    assert(compararEdiciones(h,c).select("variacionPct").head().isNullAt(0))
    assert(math.abs(compararEdiciones(h,c,true).select("variacionPct").as[Double].head() - 10.0) < 1e-8)
    assert(maxSaludY2019(h).select("pais").as[String].head() == "B")
    var duplicadoRechazado = false
    try revisarClave(h.union(h)) catch { case _: IllegalArgumentException => duplicadoRechazado = true }
    assert(duplicadoRechazado)
    println("8 comprobaciones sintéticas de Spark superadas; no son resultados del World Happiness Report")
  }
}
