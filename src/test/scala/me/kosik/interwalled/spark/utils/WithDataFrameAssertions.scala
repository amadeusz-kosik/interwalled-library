package me.kosik.interwalled.spark.utils

import org.apache.spark.sql.DataFrame
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

  private def peekDF(df: DataFrame): String = {
    val DFRowsToPeek = 10
    df.take(DFRowsToPeek).map("\t" + _).mkString("\n")
  }
}
