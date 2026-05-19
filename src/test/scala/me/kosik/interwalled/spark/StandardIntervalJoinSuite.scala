package me.kosik.interwalled.spark

import me.kosik.interwalled.spark.data.{DataSuiteResultRow, DataSuiteTestRow, TestDatasets}
import me.kosik.interwalled.spark.utils.WithDataFrameAssertions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class StandardIntervalJoinSuite extends AnyFunSpec with Matchers with WithDataFrameAssertions {

  describe("IntervalJoin standard") {
    val configuration = IntervalJoin.Configuration(0L, 100000L, 10000L)

    it("should correctly join data") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val lhsDataset = TestDatasets.databaseUniformFlat(100, 1)
      val rhsDataset = TestDatasets.querySparse(10, 1)

      val expectedData = createDF(List(
        DataSuiteResultRow("CH-0",  0,  0,  0,  0),
        DataSuiteResultRow("CH-0", 10, 10, 10, 10),
        DataSuiteResultRow("CH-0", 20, 20, 20, 20),
        DataSuiteResultRow("CH-0", 30, 30, 30, 30),
        DataSuiteResultRow("CH-0", 40, 40, 40, 40),
        DataSuiteResultRow("CH-0", 50, 50, 50, 50),
        DataSuiteResultRow("CH-0", 60, 60, 60, 60),
        DataSuiteResultRow("CH-0", 70, 70, 70, 70),
        DataSuiteResultRow("CH-0", 80, 80, 80, 80),
        DataSuiteResultRow("CH-0", 90, 90, 90, 90)
      ), DataSuiteResultRow.sparkSchema)

      val actualData = (new IntervalJoin(configuration)).join(lhsDataset, rhsDataset)
      assertDataFramesEqual(expectedData, actualData)
    }
  }

  def createDF[T : Encoder](data: List[T], schema: StructType)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    sparkSession.createDataFrame(data.toDF().rdd, schema)
  }
}
