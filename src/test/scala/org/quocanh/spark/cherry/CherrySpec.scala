package org.quocanh.spark.cherry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.FunSpec
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

class CherrySpec extends FunSpec with DataFrameSuiteBase  {

  import spark.implicits._

  describe(".createSimpleDataFrame"){
    it ("create a dataframe from dynamic content"){
      val header = List("first_name","last_name","phone")

      // build dynamic content
      var data = List(List("TOM","CAT","1234567890"))
      data = data ++ List(List("JERRY","MOUSE","1324567890"))

      // build dataframe from dynamic content
      val dfFromList = Cherry.createStringDataFrame(data, header)

      // build hardcoded dataframe for assertion
      val dfWithHardcodedValues = Seq(
        ("TOM","CAT","1234567890"),
        ("JERRY","MOUSE","1324567890")
      ).toDF("first_name","last_name","phone")

      assertDataFrameEquals(dfFromList, dfWithHardcodedValues)
    }
  }

  describe(".createDataFrameWithSchema"){
    it ("create a dataframe from dynamic content with schema"){
      val schema = StructType(
        List(StructField("first_name", StringType, true),
          StructField("last_name", StringType, true),
          StructField("age", IntegerType, false)
        )
      )

      // build dynamic content
      var data = List(List("TOM","CAT",12))
      data = data ++ List(List("JERRY","MOUSE",14))

      // build dataframe from dynamic content
      val dfFromList = Cherry.createDataFrame(data, schema)

      // build hardcoded dataframe for assertion
      val dfWithHardcodedValues = Seq(
        ("TOM","CAT",12),
        ("JERRY","MOUSE",14)
      ).toDF("first_name","last_name","age")

      assertDataFrameEquals(dfFromList, dfWithHardcodedValues)
    }
  }

  describe(".createDataFrameFromListWithSchemaTuple"){
    it ("create a dataframe from dynamic content with a list for schema"){
      val schemaTuple = List(
        ("first_name", StringType, true),
        ("last_name", StringType, true),
        ("age", IntegerType, false)
      )

      // build dynamic content
      var data = List(List("TOM","CAT",12))
      data = data ++ List(List("JERRY","MOUSE",14))

      // build dataframe from dynamic content
      val dfFromList = Cherry.createDataFrame(data, schemaTuple)

      // build hardcoded dataframe for assertion
      val dfWithHardcodedValues = Seq(
        ("TOM","CAT",12),
        ("JERRY","MOUSE",14)
      ).toDF("first_name","last_name","age")

      assertDataFrameEquals(dfFromList, dfWithHardcodedValues)
    }
  }

  describe(".duplicateRemoval"){
    it ("remove rows that have a duplicate in a dataframe"){
      val header = "first_name,last_name,phone"

      val dfWithDuplicates = Seq(
        ("Tom","Hanks"),
        ("Nicole", "Kidman"),
        ("Gorge", "Clooney"),
        ("Tom","Cruise")
      ).toDF("first_name","last_name")

      val dfWithoutDuplicates = Seq(
        ("Nicole", "Kidman"),
        ("Gorge", "Clooney")
      ).toDF("first_name","last_name")

      assertDataFrameEquals(Cherry.duplicateRemoval(dfWithDuplicates, List("first_name")), dfWithoutDuplicates)
    }
  }

  describe(".ensureColumnsExist"){
    it ("check if a dataframe has required columns"){
      // Create a dataframe with three columns: "first_name","last_name","phone"
      val df = Seq(
        ("TOM","CAT","1234567890"),
        ("JERRY","MOUSE","1324567890")
      ).toDF("first_name","last_name","phone")

      assert(Cherry.ensureColumnsExist(df, List("phone","first_name")))
    }

    it ("check if ensureColumnsExist detects missing column"){
      // Create a dataframe with three columns: "first_name","last_name","phone"
      val df = Seq(
        ("TOM","CAT","1234567890"),
        ("JERRY","MOUSE","1324567890")
      ).toDF("first_name","last_name","phone")

      assert(Cherry.ensureColumnsExist(df, List("age","first_name")) == false)
    }
  }

  describe(".missingColumns") {
    it("finds missing column(s) in a dataframe") {
      // Create a dataframe with two columns: "first_name","last_name"
      val df = Seq(
        ("TOM", "CAT"),
        ("JERRY", "MOUSE")
      ).toDF("first_name", "last_name")

      assert(Cherry.missingColumns(df, List("phone", "first_name")) == List("phone"))
    }
  }

  describe(".ensureColumnDoesNotExist") {
    it("ensure no specified columns in a dataframe") {
      // Create a dataframe with two columns: "first_name","last_name"
      val df = Seq(
        ("TOM", "CAT"),
        ("JERRY", "MOUSE")
      ).toDF("first_name", "last_name")

      assert(Cherry.ensureColumnDoesNotExist(df, List("phone", "date")))
    }

    it("find overlap columns in a dataframe") {
      // Create a dataframe with two columns: "first_name","last_name"
      val df = Seq(
        ("TOM", "CAT"),
        ("JERRY", "MOUSE")
      ).toDF("first_name", "last_name")

      assert(Cherry.ensureColumnDoesNotExist(df, List("last_name", "date")) == false)
    }
  }

  describe(".isValidProviderNpi") {
    it("validate NPIs in a dataframe") {
      val df = Seq(
        ("123"),
        ("9876543210"),
        ("212-123-3456"),
        (null),
        ("1234567893"),
        ("JERRY")
      ).toDF("npi")

      val expectedData = List(
        Row("123",false),
        Row("9876543210",false),
        Row("212-123-3456",false),
        Row(null,false),
        Row("1234567893",true),
        Row("JERRY",false)
      )

      val expectedSchema = List(
        StructField("npi", StringType, true),
        StructField("valid", BooleanType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      val isValidProviderNpiUdf = udf[Boolean, String](Cherry.isValidProviderNpi)
      val actualDf = df.withColumn("valid", isValidProviderNpiUdf($"npi"))

      assertDataFrameEquals(actualDf, expectedDf)
    }
  }

}