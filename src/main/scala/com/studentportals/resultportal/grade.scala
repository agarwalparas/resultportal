package com.studentportals.resultportal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when


object grade {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("total_marks").getOrCreate()
    val df = spark.read.option("header","true").csv("src/main/resources/students.csv")
    val df1 = df.select(
      col("full_name"),
      col("class"),
      col("section"),
      col("marks_english"),
      col("marks_maths"),
      col("marks_physics"),
      (((col("marks_english") + col("marks_maths") + col("marks_physics"))/ 300) * 100).alias("percentage")
    )
    val df2 = df1.withColumn("grade", when(col("percentage") > 90, "A")
      .when(col("percentage") > 80, "B")
      .when(col("percentage") > 70, "C")
      .when(col("percentage") > 60, "D")
      .otherwise("E"))
    df2.show()
  }

}
