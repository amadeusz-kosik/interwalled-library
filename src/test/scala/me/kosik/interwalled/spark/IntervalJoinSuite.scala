package me.kosik.interwalled.spark

import me.kosik.interwalled.spark.data.TestDatasets
import me.kosik.interwalled.spark.utils.WithDataFrameAssertions
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class IntervalJoinSuite extends AnyFunSpec with Matchers with WithDataFrameAssertions {

  val intervalJoinStrategies: Map[String, IntervalJoin.Configuration] = Map(
    "broadcast" -> IntervalJoin.Configuration(1_000_000L, 1_000_000L, 1_000_000L),
    "ranked"    -> IntervalJoin.Configuration(        0L, 1_000_000L,        10L)
  )

  val testDatasets: Map[String, ((Long, Int, SparkSession) => DataFrame, (Long, Int, SparkSession) => DataFrame)] = Map(
    "uniform flat x sparse"           -> (TestDatasets.databaseUniformFlat(_, _)(_),          TestDatasets.querySparse(_, _)(_)),
    "uniform flat x dense"            -> (TestDatasets.databaseUniformFlat(_, _)(_),          TestDatasets.queryDense(_, _)(_)),
    "uniform stacked x sparse"        -> (TestDatasets.databaseUniformStacked(_, _)(_),       TestDatasets.querySparse(_, _)(_)),
    "uniform stacked x dense"         -> (TestDatasets.databaseUniformStacked(_, _)(_),       TestDatasets.queryDense(_, _)(_)),
    "uniform heavy stacked x sparse"  -> (TestDatasets.databaseUniformHeavyStacked(_, _)(_),  TestDatasets.querySparse(_, _)(_)),
    "uniform heavy stacked x dense"   -> (TestDatasets.databaseUniformHeavyStacked(_, _)(_),  TestDatasets.queryDense(_, _)(_)),
    "uniform heavy stacked x skewed"  -> (TestDatasets.databaseUniformHeavyStacked(_, _)(_),  TestDatasets.querySkewedDense(_, _)(_)),
    "skewed flat x sparse"            -> (TestDatasets.databaseSkewedFlat(_, _)(_),           TestDatasets.querySparse(_, _)(_)),
    "skewed flat x dense"             -> (TestDatasets.databaseSkewedFlat(_, _)(_),           TestDatasets.queryDense(_, _)(_)),
    "skewed flat x skewed"            -> (TestDatasets.databaseSkewedFlat(_, _)(_),           TestDatasets.querySkewedDense(_, _)(_)),
    "skewed stacked x sparse"         -> (TestDatasets.databaseSkewedStacked(_, _)(_),        TestDatasets.querySparse(_, _)(_)),
    "skewed stacked x dense"          -> (TestDatasets.databaseSkewedStacked(_, _)(_),        TestDatasets.queryDense(_, _)(_)),
    "skewed stacked x skewed"         -> (TestDatasets.databaseSkewedStacked(_, _)(_),        TestDatasets.querySkewedDense(_, _)(_))
  )

  intervalJoinStrategies foreach { case(strategyName, configuration) =>
    describe(f"IntervalJoin $strategyName") {

      testDatasets foreach { case (name, (databaseCallback, queryCallback)) =>
        it(f"should correctly join data: $name") {
          implicit val _sparkSession: SparkSession = sparkSession

          val lhsDataset = databaseCallback(100, 4, sparkSession)
          val rhsDataset = queryCallback(100, 4, sparkSession)

          val expectedData = createExpectedDF(lhsDataset, rhsDataset)
          val (eventLog, actualData) = (new IntervalJoin(configuration)).join(lhsDataset, rhsDataset)

          eventLog should contain (s"join_method=$strategyName")
          assertDataFramesEqual(expectedData, IntervalJoin.toStrictDataFrame(actualData))
        }
      }

      // ---------------------------------------------------------------------------------------------------------------

      it("should correctly join large data: uniform flat x dense") {
        implicit val _sparkSession: SparkSession = sparkSession
        val largeDataConfiguration = configuration.copy(thresholdSaltQuery = 1_000, thresholdDatabaseBatchSize = 10_000)

        val lhsDataset = TestDatasets.databaseUniformFlat(100_000, 4)
        val rhsDataset = TestDatasets.queryDense(100_000, 4)

        /* For this volume of data, computing expected by hand is highly ineffective. */
        val expectedData = createDF(lhsDataset
          .select(
            F.col("key"),
            F.struct(
              F.col("from"),
              F.col("to")
            ).as("lhs"),
            F.struct(
              (F.col("from") - (F.col("from") % 10)).as("from"),
              (F.col("from") - (F.col("from") % 10) + 9).as("to")
            ).as("rhs")
          ), IntervalJoin.outputSparkSchema)
        val (eventLog, actualData) = (new IntervalJoin(largeDataConfiguration)).join(lhsDataset, rhsDataset)

        eventLog should contain (s"join_method=$strategyName")
        assertDataFramesEqual(expectedData, IntervalJoin.toStrictDataFrame(actualData))
      }
    }
  }
}
