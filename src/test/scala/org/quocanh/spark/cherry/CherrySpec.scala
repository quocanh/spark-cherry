package org.quocanh.spark.cherry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

class CherrySpec extends FunSpec with DataFrameSuiteBase  {

  import spark.implicits._

  describe("createDataFrame"){

    it ("create a dataframe from dynamic content"){
      val header = "first_name,last_name,phone"

      // build dynamic content
      var data = List(List("TOM","CAT","1234567890"))
      data = data ++ List(List("JERRY","MOUSE","1324567890"))

      // build dataframe from dynamic content
      val dfFromList = Cherry.createDataFrameFromListWithHeader(data, header)

      // build hardcoded dataframe for assertion
      val dfWithHardcodedValues = Seq(
        ("TOM","CAT","1234567890"),
        ("JERRY","MOUSE","1324567890")
      ).toDF("first_name","last_name","phone")

      assertDataFrameEquals(dfFromList, dfWithHardcodedValues)
    }

  }

  describe("duplicateRemoval"){

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

  describe("ensureColumnsExist"){

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

}