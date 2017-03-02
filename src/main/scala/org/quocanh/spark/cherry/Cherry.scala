package org.quocanh.spark.cherry

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType,StructField,StringType}

object Cherry {

  def createDataFrameFromListWithHeader(data: List[List[String]], header: String) : DataFrame = {

    val session = SparkSession.builder().getOrCreate() // get spark session

    val rows = data.map{x => Row(x:_*)}
    val rdd = session.sparkContext.parallelize(rows)

    val schema = StructType(header.split(",").
       map(fieldName => StructField(fieldName, StringType, true)))
    session.sqlContext.createDataFrame(rdd,schema)

  }
}
