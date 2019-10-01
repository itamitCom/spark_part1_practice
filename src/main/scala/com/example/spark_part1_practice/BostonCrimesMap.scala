package com.example.spark_part1_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BostonCrimesMap extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val crimeCSV = args(0)
  val offenseCodesCSV = args(1)
  val outputFolderParquet = args(2)

  val crimeDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$crimeCSV")

  val offenseCodesDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$offenseCodesCSV")

  val bostonCrimeStats = crimeDF
    .join(offenseCodesDF, $"CODE" === $"OFFENSE_CODE")
    .filter($"NAME".startsWith("ROBBERY"))
    .groupBy($"NAME")
    .count()
    .orderBy($"count".desc)

  //offenseCodesDF.show(10);

  //crimeDF.write.parquet(s"$outputFolderParquet/crime.parquet")

}
