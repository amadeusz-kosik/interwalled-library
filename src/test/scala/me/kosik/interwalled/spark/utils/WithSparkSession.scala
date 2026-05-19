package me.kosik.interwalled.spark.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}


trait WithSparkSession extends BeforeAndAfterAll { self: Suite =>

  @transient
  protected var sparkSession: SparkSession = _

  protected def sparkAppName: String = "Apache Spark unit test"

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.getActiveSession.getOrElse {
      SparkSession.builder()
        .master("local[*]")
        .appName(sparkAppName)
        .getOrCreate()
    }
    super.beforeAll()
  }
}
