package me.kosik.interwalled.spark.data

import me.kosik.interwalled.spark.data.DataSuiteResultRow.DataSuiteInterval
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


case class DataSuiteResultRow(key: String, lhs: DataSuiteInterval, rhs: DataSuiteInterval)

object DataSuiteResultRow {
  case class DataSuiteInterval(from: Long, to: Long)

  def apply(key: String, lhsFrom: Long, lhsTo: Long, rhsFrom: Long, rhsTo: Long): DataSuiteResultRow =
    DataSuiteResultRow(key, DataSuiteInterval(lhsFrom, lhsTo), DataSuiteInterval(rhsFrom, rhsTo))

  def sparkSchema = StructType(Array(
    StructField("key", DataTypes.StringType, nullable = false),
    StructField("lhs", StructType(Array(
      StructField("from", DataTypes.LongType, nullable = false),
      StructField("to", DataTypes.LongType, nullable = false)
    )), nullable = false),
    StructField("rhs", StructType(Array(
      StructField("from", DataTypes.LongType, nullable = false),
      StructField("to", DataTypes.LongType, nullable = false)
    )), nullable = false)
  ))
}

