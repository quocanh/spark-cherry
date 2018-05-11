package org.quocanh.spark.cherry

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Cherry {

  // Create a dataframe from list of list of string values
  // A dataframe is list of Rows, each has many Columns
  // Hence the data to create a simple dataframe is List[List[String]]
  // The string for header must have the same number of items as the number of columns
  // The header is in un_escaped CSV format
  def createStringDataFrame(data: List[List[String]], header: List[String]) : DataFrame = {
    val session = SparkSession.builder().getOrCreate() // get spark session
    val rows = data.map{x => Row(x:_*)}
    val rdd = session.sparkContext.parallelize(rows)
    val schema = StructType(header.
      map(fieldName => StructField(fieldName, StringType, true)))

    session.sqlContext.createDataFrame(rdd,schema)
  }

  // Create a dataframe from list of list of values
  // This method also needs a schema for the df
  // Note: data in 1st list must satisfy the types in schema
  def createDataFrame(data: List[List[Any]], schema: StructType) : DataFrame = {
    val session = SparkSession.builder().getOrCreate() // get spark session
    val rows = data.map{x => Row(x:_*)}
    val rdd = session.sparkContext.parallelize(rows)
    session.sqlContext.createDataFrame(rdd,schema)
  }

  // Create a dataframe from list of list of values
  // This method also needs a list of tuple3 for schema
  // Note: data in 1st list must satisfy the types in schema
  def createDataFrame(data: List[List[Any]], schemaTuple: List[Tuple3[String,DataType,Boolean]]) : DataFrame = {
    val session = SparkSession.builder().getOrCreate() // get spark session
    val rows = data.map{x => Row(x:_*)}
    val schema = StructType(schemaTuple.map(column => {
      StructField(column._1, column._2, column._3)
    }))
    val rdd = session.sparkContext.parallelize(rows)
    session.sqlContext.createDataFrame(rdd,schema)
  }

  def createDataFrameWithRows(data: List[Row], schema: StructType) : DataFrame = {
    val session = SparkSession.builder().getOrCreate() // get spark session
    val rdd = session.sparkContext.parallelize(data)
    session.sqlContext.createDataFrame(rdd,schema)
  }

  // Remove duplicate rows in a dataframe for specified columns
  // This method retains only rows that have no duplicates
  def duplicateRemoval(df: DataFrame, fieldList: List[String]) : DataFrame = {
    val window = Window.partitionBy(fieldList.head, fieldList.tail: _*)
    val column = fieldList(0)
    df.withColumn("__count", count(column).over(window))
      .filter(col("__count") === 1).drop("__count")
  }

  // Test if input dataframe has all columns in the 'columnList'
  def ensureColumnsExist(df: DataFrame, columnList: List[String]) : Boolean = {
    df.columns.intersect(columnList).size == columnList.size
  }

  // Given 'columnList' find missing columns in the dataframe
  def missingColumns(df: DataFrame, columnList: List[String]) : List[String] = {
    (columnList diff df.columns.intersect(columnList)).toList
  }

  // Test if input dataframe doesn't have specified columns
  def ensureColumnDoesNotExist(df: DataFrame, columnList: List[String]) : Boolean = {
    df.columns.intersect(columnList).size == 0
  }

  // Ensure a string is a valid provider NPI (this is not organization npi)
  def isValidProviderNpi(npi:String) : Boolean = {
    def isNumeric(s: String): Boolean = {
      try {
        s.toInt
        return true
      } catch {
        case e: Exception => false
      }
    }

    def isLuhnValid(number: Seq[Char]): Boolean = {
      val idx = List(9,8,7,6,5,4,3,2,1,0)
      if (number.size != 10) return false // npi must have 10 digits
      var sum = 24    // value 24 is required in specs
      for (i <- idx) {
        var digit = number(i).toString.toShort
        if (i%2 == 1) {
          sum += digit
        } else if (digit >4) {
          sum += (digit * 2) - 9
        } else {
          sum += digit * 2
        }
      }
      return sum % 10 == 0
    }

    if ( !isNumeric(npi) ) return false
    val s = npi.trim
    // We care about provider, not organization, so npi begins with '1'
    if ( s.charAt(0) != '1' || !isLuhnValid(s)) {
      return false
    }
    return true
  }
  // Note: one can wrap isValidProviderNpi into a UDF like this
  // val isValidProviderNpiUdf = udf[Boolean, String](isValidProviderNpi)
  // The above UDF is safe with NULL values

  // Return columns size for string column, 0 for all other types
  // This is useful for DDL to create a table in a database to load the extracted data from the df
  def columnSize(df: DataFrame) : List[Int] = {
    var res = List[Int]()
    df.schema.foreach{c =>
      if (c.dataType == StringType) {
        res = df.agg(max(length(col(c.name)))).first.get(0).asInstanceOf[Int] :: res
      } else {
        res = 0 :: res
      }
    }
    res.reverse
  }

  // Return a dataframe that is a simple profile for the input dataframe
  // Each row in output dataframe describes one column in the input
  // The output has three columns: column_name, count-of-not-null-rows, count-of-null-rows
  def simpleProfile(df: DataFrame) : DataFrame = {
    val schema = StructType(
      List(StructField("column_name", StringType, true),
        StructField("non_empty_count", LongType, false),
        StructField("empty_count", LongType, false)
      )
    )
    var data = List[List[Any]]()
    df.columns.foreach{c =>
      data = data :::
      List(List(c, df.filter(col(c).isNotNull).count, df.filter(col(c).isNull).count))
    }
    Cherry.createDataFrame(data, schema)
  }

}
