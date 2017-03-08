package org.quocanh.spark.cherry

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType,StructField,StringType}

object Cherry {

  // create a dataframe from list of list of values
  def createDataFrameFromListWithHeader(data: List[List[String]], header: String) : DataFrame = {

    val session = SparkSession.builder().getOrCreate() // get spark session
    val rows = data.map{x => Row(x:_*)}
    val rdd = session.sparkContext.parallelize(rows)
    val schema = StructType(header.split(",").
       map(fieldName => StructField(fieldName, StringType, true)))

    session.sqlContext.createDataFrame(rdd,schema)

  }

  // retain only rows that have no duplicates in specified columns
  def duplicateRemoval(df: DataFrame, fieldList: List[String]) : DataFrame = {

    val df2 = df.groupBy(fieldList.head,fieldList.tail: _*).count()
    val df3 = df2.filter(col("count") > 1)
    df.join(df3, fieldList, "leftanti")

  }

  // test if input dataframe has all columns in the 'columnList'
  def ensureColumnsExist(df: DataFrame, columnList: List[String]) : Boolean = {

    df.columns.intersect(columnList).size == columnList.size

  }

}
