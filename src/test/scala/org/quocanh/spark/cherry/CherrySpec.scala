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
      data = data :+ List("JERRY","MOUSE","1324567890")

      // build dataframe from dynamic content
      val dfFromList = Cherry.createDataFrameFromListWithHeader(data, header)

      // build hardcoded dataframe
      val dfWithHardcodedValues = Seq(
        ("TOM","CAT","1234567890"),
        ("JERRY","MOUSE","1324567890")
      ).toDF("first_name","last_name","phone")

      assertDataFrameEquals(dfFromList, dfWithHardcodedValues)
    }
  }
}