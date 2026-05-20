package me.kosik.interwalled.spark

import me.kosik.interwalled.spark.data.TestDatasets
import me.kosik.interwalled.spark.utils.WithDataFrameAssertions
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class IntervalJoinSuite extends AnyFunSpec with Matchers with WithDataFrameAssertions {

  val intervalJoinStrategies: Map[String, IntervalJoin.Configuration] = Map(
    "broadcast" -> IntervalJoin.Configuration(1_000_000L, 1_000_000L, 1_000_000L),
    "standard"  -> IntervalJoin.Configuration(        0L, 1_000_000L, 1_000_000L)
  )

  intervalJoinStrategies foreach { case(strategyName, configuration) =>
    describe(f"IntervalJoin $strategyName") {

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
}
