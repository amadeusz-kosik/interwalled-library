package me.kosik.interwalled.spark

import me.kosik.interwalled.ailist.{AIListBuilder, Configuration, Interval}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.storage.StorageLevel


object IntervalJoin {

  case class Configuration(
    /**
     * Maximum number of rows in the database (AIList) that allows running broadcast join.
     *  Broadcast is faster than fully distributed join, but require lower memory usage.
     */
    thresholdBroadcastJoinRowsCount: Long = 10_000_000L,

    /**
     * Maximum number of distinct values of key column that allows splitting the dataset per
     *  each group. The check is used to guard against exploding data on the driver due to
     *  extremely high cardinality of the key column.
     */
    thresholdGroupsCount: Long = 10_000L,

    /**
     * Maximum number of rows per group. If both this and @ThresholdBroadcastJoinRowsCount thresholds are exceeded,
     *  the exceeding groups (defined by key column) will be split into smaller ones.
     */
    thresholdGroupSplit: Long = 1_000_000
  )

  val outputSparkSchema: StructType = StructType(Array(
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

class IntervalJoin(configuration: IntervalJoin.Configuration) extends Serializable {
  import IntervalJoin.outputSparkSchema

  // FIXME: this function has no description
  // FIXME: this function needs logging
  def join(lhs: DataFrame, rhs: DataFrame): DataFrame = {
    val (databaseSize, databaseDF, querySize, queryDF, isSwapped) = chooseDatabaseAndQuery(lhs, rhs)

    if (databaseSize <= configuration.thresholdBroadcastJoinRowsCount) {
      runBroadcastIntervalJoin(databaseDF, queryDF)
    } else {
      val keyCounts = databaseDF
        .select("key")
        .distinct()
        .count()

      if (keyCounts <= configuration.thresholdGroupsCount) {
        val groupsCounts = databaseDF
          .groupBy("key")
          .count().as("rows_count")
          .collect()

        val groupsToSplit = groupsCounts
          .filter(r => r.getLong(1) > configuration.thresholdGroupSplit)
          .map(r => r.getString(0))
          .toList

        if (groupsToSplit.nonEmpty)
          runRankedIntervalJoin(databaseDF, queryDF, groupsToSplit)
        else
          runStandardIntervalJoin(databaseDF, queryDF, isSwapped)
      } else {
        runStandardIntervalJoin(databaseDF, queryDF, isSwapped)
      }
    }
  }

  /**
   * Select which side of the join will be converted to AIList (database) and which will be left as is.
   *  The database is supposed to have less or the same number of rows as the query.
   * @param lhs left side of the join.
   * @param rhs right side of the join.
   * @return tuple of (databaseCount, database, queryCount, query, isSwapped).
   */
  private def chooseDatabaseAndQuery(lhs: DataFrame, rhs:DataFrame): (Long, DataFrame, Long, DataFrame, Boolean) = {
    val lhsSize = lhs.count()
    val rhsSize = rhs.count()

    if(lhsSize <= rhsSize)
      (lhsSize, lhs, rhsSize, rhs, false)
    else
      (rhsSize, rhs, lhsSize, lhs, true)
  }

  private def runBroadcastIntervalJoin(database: DataFrame, query: DataFrame): DataFrame = {


    ???
  }

  private def runRankedIntervalJoin(databaseDF: DataFrame, queryDF: DataFrame, groupsToSplit: List[String]): DataFrame = {
    val rankedDatabaseDF = databaseDF
      .withColumn("__rank",
        F.when(F.col("key").isin(groupsToSplit),
            F.dense_rank().over(Window.partitionBy("key").orderBy(F.col("from").asc, F.col("to").asc)))
          .otherwise(F.lit(0))
      )
      .withColumn("__bucket", (F.col("__rank") / F.lit(configuration.thresholdGroupSplit)).cast(DataTypes.LongType))
      .drop("__rank")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val ranks = rankedDatabaseDF
      .groupBy("key", "__bucket")
      .agg(F.min("from").as("min_from"), F.max("to").as("max_to"))
      .collect()

    val databaseRDD = rankedDatabaseDF
      .select("key", "__bucket", "from", "to")
      .repartition(F.col("key"), F.col("__bucket"))
      .rdd

    val aiLists = databaseRDD
      .mapPartitions { dbRowsIterator =>
        val groupedDatabaseData = dbRowsIterator.toArray
          .map(row => (row.getAs[String]("key"), row.getAs[Int]("__bucket")) -> Interval(row.getAs[Long]("from"), row.getAs[Long]("to")))
          .groupBy { case (keys, _) => keys }
          .map { case (keys, values) => (keys, values.map { case (_, interval) => interval }) }

        val aiLists = groupedDatabaseData.map { case (keys, rows) =>
          keys -> AIListBuilder.build(Configuration.apply(), rows)
        }

        aiLists.iterator
      }

    val aiListComponents = aiLists
      .flatMap { case (keys, aiLists) =>
        aiLists.map(keys -> _)
      }

    // FIXME: grouping!
    val queryRDD = queryDF
      .select("key", "from", "to")
      .rdd
      .map(r => r.getAs[String]("key") -> Interval(r.getAs[Long]("from"), r.getAs[Long]("to")))
      .groupByKey()


    ???
//    val joinedRDD = aiListComponents
//      .join(queryRDD)
//      .mapPartitions { rows => rows.flatMap { case (key, (aiList, queries)) =>
//        queries.flatMap(query => aiList.overlapping(query).map((key, _, query)))
//      }}
//
//    if(isSwapped)
//      joinedRDD.toDF("key", "rhs.from", "rhs.to", "lhs.from", "lhs.to")
//    else
//      joinedRDD.toDF("key", "lhs.from", "lhs.to", "rhs.from", "rhs.to")
  }

  private def runStandardIntervalJoin(databaseDF: DataFrame, queryDF: DataFrame, isSwapped: Boolean): DataFrame = {
    import databaseDF.sparkSession.implicits._

    val aiLists = databaseDF
      .select("key", "from", "to")
      .repartition(F.col("key"))
      .rdd
      .mapPartitions { dbRowsIterator =>
        val groupedDatabaseData = dbRowsIterator.toArray
          .map(row => row.getAs[String]("key") -> Interval(row.getAs[Long]("from"), row.getAs[Long]("to")))
          .groupBy { case (key, _) => key }
          .map { case (key, values) => (key, values.map { case (_, interval) => interval }) }

        val aiLists = groupedDatabaseData.map { case (key, rows) =>
          key -> AIListBuilder.build(Configuration.apply(), rows)
        }

        aiLists.iterator
      }

    val aiListComponents = aiLists
      .flatMap { case (key, aiLists) =>
        aiLists.map(key -> _)
      }

    val queryRDD = queryDF
      .select("key", "from", "to")
      .rdd
      .map(r => r.getAs[String]("key") -> Interval(r.getAs[Long]("from"), r.getAs[Long]("to")))
      .groupByKey()

    val joinedRDD = aiListComponents
      .join(queryRDD)
      .mapPartitions { rows => rows.flatMap { case (key, (aiList, queries)) =>
        queries.flatMap(query => aiList.overlapping(query).map(dbRow => (key, dbRow.from, dbRow.to, query.from, query.to)))
      }}

    val initialDFColumnNames = {
      if (isSwapped)
        Array("key", "rhs_from", "rhs_to", "lhs_from", "lhs_to")
      else
        Array("key", "lhs_from", "lhs_to", "rhs_from", "rhs_to")
    }

    databaseDF.sparkSession.createDataFrame(
      joinedRDD
        .toDF(initialDFColumnNames: _*)
        .select(
          F.col("key"),
          F.struct(
            F.col("lhs_from").as("from"),
            F.col("lhs_to").as("to"),
          ).as("lhs"),
          F.struct(
            F.col("rhs_from").as("from"),
            F.col("rhs_to").as("to"),
          ).as("rhs")
        )
        .rdd,
      outputSparkSchema
    )
  }

  // -------------------------------------------------------------------------------------------------------------------

}
