package com.example.spark_part1_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class CategoryStats(GENDER_CD: String, category: String, count: Long)

object ClickStreamExample extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val csFolder = args(0)

  val userEvetnts = spark.read.json(s"$csFolder/omni_clickstream.json")
  val products = spark.read.json(s"$csFolder/products.json")
  val users = spark.read.json(s"$csFolder/users.json")

  userEvetnts
    .join(products, userEvetnts("url") === products("url"))
    .join(users, userEvetnts("swid") === concat(lit("{"), users("swid"), lit("}")))
    .where($"GENDER_CD" =!= "U")
    .groupBy("GENDER_CD", "category")
    .count()
    .orderBy('count.desc)
    .as[CategoryStats]
    .groupByKey(_.GENDER_CD)
    .flatMapGroups{
      case (gender, iter) => iter.toList.sortBy(-_.count).take(3)
    }
    //.show(false)
    .explain(true)
}