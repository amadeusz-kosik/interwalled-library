package me.kosik.interwalled.spark.utils

import me.kosik.interwalled.spark.IntervalJoin
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.{functions => F}
import org.scalatest.Suite


trait WithDataFrameAssertions extends WithSparkSession { self: Suite =>

  def assertSchemasEqual(expected: DataFrame, actual: DataFrame): Unit = {
    assert(expected.schema == actual.schema, s"$expected and $actual schemas do not align.")
  }

  def assertRowsCountEqual(expected: DataFrame, actual: DataFrame): Unit = {
    assert(expected.count() == actual.count(), s"$expected and $actual rows count is not equal.")
  }

  def assertDataFramesEqual(expected: DataFrame, actual: DataFrame): Unit = {
    assertSchemasEqual(expected, actual)
    assertRowsCountEqual(expected, actual)

    val expectedExclusive = expected.except(actual)
    assert(expectedExclusive.isEmpty, s"Expected DF should not contain anything not present in actual, got ${expectedExclusive.count()} rows." + peekDF(expected))

    val actualExclusive = actual.except(expected)
    assert(expectedExclusive.isEmpty, s"Actual DF should not contain anything not present in expected, got ${actualExclusive.count()} rows." + peekDF(actual))
  }

  // -------------------------------------------------------------------------------------------------------------------

  def createDF[T : Encoder](data: List[T], schema: StructType)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    sparkSession.createDataFrame(data.toDF().rdd, schema)
  }

  def createDF(data: DataFrame, schema: StructType)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(data.rdd, schema)
  }

  def createExpectedDF(database: DataFrame, query: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val dArray = database.collect().map(r => r.getAs[String]("key") -> (r.getAs[Long]("from"), r.getAs[Long]("to")))
    val qArray = query.collect().map(r => r.getAs[String]("key") -> (r.getAs[Long]("from"), r.getAs[Long]("to")))

    val joined = for {
      (dKey, (dFrom, dTo)) <- dArray
      (qKey, (qFrom, qTo)) <- qArray
      if dKey == qKey && dFrom <= qTo && qFrom <= dTo
    } yield (dKey, dFrom, dTo, qFrom, qTo)

    val joinedRDD = joined
      .toList
      .toDF("key", "dFrom", "dTo", "qFrom", "qTo")
      .select(
        F.col("key"),
        F.struct(F.col("dFrom").as("from"), F.col("dTo").as("to")).as("lhs"),
        F.struct(F.col("qFrom").as("from"), F.col("qTo").as("to")).as("rhs")
      )
      .rdd

    sparkSession.createDataFrame(joinedRDD, IntervalJoin.outputSparkSchema)
  }

  // -------------------------------------------------------------------------------------------------------------------

  private def peekDF(df: DataFrame): String = {
    val DFRowsToPeek = 10
    df.take(DFRowsToPeek).map("\t" + _).mkString("\n")
  }
}
