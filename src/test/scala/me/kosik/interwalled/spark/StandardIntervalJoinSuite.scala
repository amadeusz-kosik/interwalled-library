package me.kosik.interwalled.spark

import me.kosik.interwalled.spark.data.{DataSuiteResultRow, TestDatasets}
import me.kosik.interwalled.spark.utils.WithDataFrameAssertions
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class StandardIntervalJoinSuite extends AnyFunSpec with Matchers with WithDataFrameAssertions {

  describe("IntervalJoin standard") {
    val configuration = IntervalJoin.Configuration(0L, 100000L, 10000L)

    it("should correctly join data: uniform flat x sparse") {
      implicit val _sparkSession: SparkSession = sparkSession

      val lhsDataset = TestDatasets.databaseUniformFlat(100, 1)
      val rhsDataset = TestDatasets.querySparse(100, 1)

      val expectedData = createExpectedDF(lhsDataset, rhsDataset)
      val actualData = (new IntervalJoin(configuration)).join(lhsDataset, rhsDataset)

      assertDataFramesEqual(expectedData, actualData)
    }

    it("should correctly join data: uniform flat x sparse, multiple groups") {
      implicit val _sparkSession: SparkSession = sparkSession

      val lhsDataset = TestDatasets.databaseUniformFlat(100, 4)
      val rhsDataset = TestDatasets.querySparse(100, 4)

      val expectedData = createExpectedDF(lhsDataset, rhsDataset)
      val actualData = (new IntervalJoin(configuration)).join(lhsDataset, rhsDataset)

      assertDataFramesEqual(expectedData, actualData)
    }

    it("should correctly join data: uniform flat x dense") {
      implicit val _sparkSession: SparkSession = sparkSession

      val lhsDataset = TestDatasets.databaseUniformFlat(100, 1)
      val rhsDataset = TestDatasets.queryDense(100, 1)

      val expectedData = createExpectedDF(lhsDataset, rhsDataset)
      val actualData = (new IntervalJoin(configuration)).join(lhsDataset, rhsDataset)

      assertDataFramesEqual(expectedData, actualData)
    }

    it("should correctly join data: uniform flat x dense, multiple groups") {
      implicit val _sparkSession: SparkSession = sparkSession

      val lhsDataset = TestDatasets.databaseUniformFlat(100, 4)
      val rhsDataset = TestDatasets.queryDense(100, 4)

      val expectedData = createExpectedDF(lhsDataset, rhsDataset)
      val actualData = (new IntervalJoin(configuration)).join(lhsDataset, rhsDataset)

      assertDataFramesEqual(expectedData, actualData)
    }
  }


}
