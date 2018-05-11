package org.quocanh.spark.cherry

import org.apache.spark.sql.SparkSession

trait SparkSessionInstance {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}

