package org.quocanh.spark.cherry

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

trait SparkSessionForTests {
  sys.props("testing") = "true" // This helps detecting test env in any tool, not just SBT
  Logger.getLogger("org").setLevel(Level.ERROR) // only show errors in log
  Logger.getLogger("akka").setLevel(Level.ERROR) // suppress info messages

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}

