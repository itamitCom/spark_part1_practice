package com.example.spark_part1_practice

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * С помощью Spark соберите агрегат по районам (поле district) со следующими метриками:
  * crimes_total - общее количество преступлений в этом районе
  * crimes_monthly - медиана числа преступлений в месяц в этом районе
  * frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе,
  *    объединенных через запятую с одним пробелом “, ” , расположенных в порядке убывания частоты
  * crime_type - первая часть NAME из таблицы offense_codes, разбитого по разделителю “-”
  *    (например, если NAME “BURGLARY - COMMERICAL - ATTEMPT”, то crime_type “BURGLARY”)
  * lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
  * lng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов
  *
  * crime
  * +---------------+------------+--------------------+--------------------+--------+--------------+--------+-------------------+----+-----+-----------+----+----------+-----------------+-----------+------------+--------------------+
  * |INCIDENT_NUMBER|OFFENSE_CODE|  OFFENSE_CODE_GROUP| OFFENSE_DESCRIPTION|DISTRICT|REPORTING_AREA|SHOOTING|   OCCURRED_ON_DATE|YEAR|MONTH|DAY_OF_WEEK|HOUR|  UCR_PART|           STREET|        Lat|        Long|            Location|
  * +---------------+------------+--------------------+--------------------+--------+--------------+--------+-------------------+----+-----+-----------+----+----------+-----------------+-----------+------------+--------------------+
  * |     I182070945|         619|             Larceny|  LARCENY ALL OTHERS|     D14|           808|    null|2018-09-02 13:00:00|2018|    9|     Sunday|  13|  Part One|       LINCOLN ST|42.35779134|-71.13937053|(42.35779134, -71...|
  * |     I182070943|        1402|           Vandalism|           VANDALISM|     C11|           347|    null|2018-08-21 00:00:00|2018|    8|    Tuesday|   0|  Part Two|         HECLA ST|42.30682138|-71.06030035|(42.30682138, -71...|
  *
  *
  * ИНДИКАТОРНЫЙ НОМЕР
  * КОД ОФЕРЕНЦИИ
  * ГРУППА ОФИСНЫХ КОДОВ
  * ОПИСАНИЕ НАРУШЕНИЯ
  * ОКРУГ
  * ОБЛАСТЬ ОТЧЕТНОСТИ
  * СТРЕЛЬБА
  * СОБЫТИЯ НА ДАТУ
  * ГОД
  * МЕСЯЦ
  * ДЕНЬ НЕДЕЛИ
  * ЧАС
  * UCR_PART
  * УЛИЦА
  * Lat
  * Долго
  * Место нахождения
  *
  * offense_codes
  * +----+--------------------+
  * |CODE|                NAME|
  * +----+--------------------+
  * | 612|LARCENY PURSE SNA...|
  * | 613| LARCENY SHOPLIFTING|
  * | 615|LARCENY THEFT OF ...|
  *
  * КОД
  * НАЗВАНИЕ
  */
object BostonCrimesMap extends App {

  val spark = SparkSession
    .builder()
    .appName("Boston crimes map")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val crimeCSV = args(0)
  val offenseCodesCSV = args(1)
  val outputFolderParquet = args(2)

  val crimeDF: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$crimeCSV")

  val offenseCodesDF: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$offenseCodesCSV")
    .withColumn("crime_type", trim(split($"NAME", "-")(0)))

  val offenseCodesBroadcast: Broadcast[DataFrame] = spark.sparkContext.broadcast(offenseCodesDF)

  val crimeOffence: DataFrame = crimeDF
    .join(offenseCodesBroadcast.value, crimeDF("OFFENSE_CODE") === offenseCodesBroadcast.value("CODE"))
    .select("INCIDENT_NUMBER", "DISTRICT", "REPORTING_AREA", "MONTH", "Lat", "Long", "crime_type")
    .filter($"DISTRICT".isNotNull)
    .cache

  val crimesTotalAndMonthly: DataFrame = crimeOffence
    .groupBy($"DISTRICT", $"MONTH")
    .agg(count($"INCIDENT_NUMBER").alias("count_incident"))
    .groupBy($"DISTRICT".alias("district"))
    .agg(expr("SUM(count_incident)").alias("crimes_total"),
      expr("percentile_approx(count_incident, 0.5)").alias("crimes_monthly"))

  crimesTotalAndMonthly.show(false)

  /**
    * frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе,
    *    объединенных через запятую с одним пробелом “, ” , расположенных в порядке убывания частоты
    * crime_type - первая часть NAME из таблицы offense_codes, разбитого по разделителю “-”
    *    (например, если NAME “BURGLARY - COMMERICAL - ATTEMPT”, то crime_type “BURGLARY”)
    */
  val frequentCrimeTypes: DataFrame = crimeOffence
    .groupBy($"DISTRICT", $"crime_type")
    .agg(count($"INCIDENT_NUMBER").alias("count_incident"))
    .withColumn(
      "index",
      dense_rank().over(partitionBy("DISTRICT").orderBy($"count_incident".desc))
    )
    .filter($"index".leq(3))
    .groupBy($"DISTRICT")
    .agg(
      concat_ws(", ", collect_list(substring_index($"crime_type", " -", 1)))
        .alias("frequent_crime_types")
    )

  frequentCrimeTypes.show(false)

  /**
    * lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
    * lng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов
    */
  val latAndLng: DataFrame = crimeOffence
    .groupBy($"DISTRICT")
    .agg(
      mean($"Lat").alias("lat"),
      mean($"Long").alias("lng")
    )

  latAndLng.show(false)

  val parquetBostonCrimeStats: DataFrame = crimesTotalAndMonthly
    .join(frequentCrimeTypes, Seq("DISTRICT"))
    .join(latAndLng, Seq("DISTRICT"))

  parquetBostonCrimeStats.show(false)

  parquetBostonCrimeStats
    .repartition(1)
    .write
      .mode("OVERWRITE")
      .parquet(outputFolderParquet)

}
