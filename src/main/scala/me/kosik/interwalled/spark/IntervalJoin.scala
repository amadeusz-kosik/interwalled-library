package me.kosik.interwalled.spark

import me.kosik.interwalled.ailist.{AIList, AIListBuilder, Configuration, Interval}
import me.kosik.interwalled.spark.IntervalJoin.{DatabaseQueryChoice, JoinedRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
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

  case class DatabaseQueryChoice(
    database: DataFrame,
    databaseCount: Long,
    query: DataFrame,
    queryCount: Long,
    isSwapped: Boolean
  )

  type JoinedRDD = RDD[(String, Long, Long, Long, Long)]

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
  def join(lhs: DataFrame, rhs: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val databaseQueryChoice = chooseDatabaseAndQuery(lhs, rhs)
    val joinedRDD = selectAndRunIntervalJoinStrategy(databaseQueryChoice)

    val initialDFColumnNames = {
      if (databaseQueryChoice.isSwapped)
        Array("key", "rhs_from", "rhs_to", "lhs_from", "lhs_to")
      else
        Array("key", "lhs_from", "lhs_to", "rhs_from", "rhs_to")
    }

    sparkSession.createDataFrame(
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

  /**
   * Select which side of the join will be converted to AIList (database) and which will be left as is.
   *  The database is supposed to have less or the same number of rows as the query.
   * @param lhs left side of the join.
   * @param rhs right side of the join.
   * @return tuple of (databaseCount, database, queryCount, query, isSwapped).
   */
  private def chooseDatabaseAndQuery(lhs: DataFrame, rhs:DataFrame): DatabaseQueryChoice = {
    val lhsSize = lhs.count()
    val rhsSize = rhs.count()

    if(lhsSize <= rhsSize)
      DatabaseQueryChoice(lhs, lhsSize, rhs, rhsSize, isSwapped = false)
    else
      DatabaseQueryChoice(rhs, rhsSize, lhs, lhsSize, isSwapped = true)
  }

  private def selectAndRunIntervalJoinStrategy(databaseQueryChoice: DatabaseQueryChoice)(implicit sparkSession: SparkSession): JoinedRDD = {
    if (databaseQueryChoice.databaseCount <= configuration.thresholdBroadcastJoinRowsCount) {
      runBroadcastIntervalJoin(databaseQueryChoice.database, databaseQueryChoice.query)
    } else {
      val keyCounts = databaseQueryChoice.database
        .select("key")
        .distinct()
        .count()

      if (keyCounts <= configuration.thresholdGroupsCount) {
        val groupsCounts = databaseQueryChoice.database
          .groupBy("key")
          .count().as("rows_count")
          .collect()

        val groupsToSplit = groupsCounts
          .filter(r => r.getLong(1) > configuration.thresholdGroupSplit)
          .map(r => r.getString(0))
          .toList

        if (groupsToSplit.nonEmpty)
          runRankedIntervalJoin(databaseQueryChoice.database, databaseQueryChoice.query, groupsToSplit)
        else
          runStandardIntervalJoin(databaseQueryChoice.database, databaseQueryChoice.query)
      } else {
        runStandardIntervalJoin(databaseQueryChoice.database, databaseQueryChoice.query)
      }
    }
  }

  // -------------------------------------------------------------------------------------------------------------------

  private def runBroadcastIntervalJoin(database: DataFrame, query: DataFrame): JoinedRDD = {
    val sparkSession = database.sparkSession

    val databaseCollected = database
      .select("key", "from", "to")
      .rdd
      .groupBy(row => row.getAs[String]("key"))
      .map { case(key, rows) => key -> rows.map(row => Interval(row.getAs[Long]("from"), row.getAs[Long]("to"))) }
      .collect()

    val aiLists: Map[String, Array[AIList]] = databaseCollected
      .map { case (key, intervals) =>
        val intervalsIterator = intervals.iterator
        key -> AIListBuilder.build(Configuration.apply(), intervalsIterator)
      }
      .toMap

    val aiListsBroadcast = sparkSession.sparkContext.broadcast(aiLists)

    val queryRDD = query
      .select("key", "from", "to")
      .rdd

    val joinedRDD = queryRDD
      .flatMap { row =>
        val qKey = row.getAs[String]("key")
        val qFrom = row.getAs[Long]("from")
        val qTo = row.getAs[Long]("to")

        aiListsBroadcast.value
          .get(qKey)
          .map { aiLists =>
            aiLists
              .flatMap(_.overlapping(Interval(qFrom, qTo)))
              .map(dInterval => (qKey, dInterval.from, dInterval.to, qFrom, qTo))
          }
          .getOrElse(Array.empty)
      }

    joinedRDD
  }

  private def runRankedIntervalJoin(databaseDF: DataFrame, queryDF: DataFrame, groupsToSplit: List[String])(implicit sparkSession: SparkSession): JoinedRDD = {
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
      .map(row => (row.getAs[String]("key"), row.getAs[Int]("__bucket"), row.getAs[Long]("min_from"), row.getAs[Long]("max_to")))

    val ranksBroadcast = sparkSession.sparkContext
      .broadcast(ranks)

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

    val queryRDD = queryDF
      .select("key", "from", "to")
      .rdd
      .flatMap { row =>
        ranksBroadcast
          .value
          .filter { case (key, _, minFrom, maxTo) => row.getAs[String]("key") == key && row.getAs[Long]("from") <= maxTo && row.getAs[Long]("to") >= minFrom }
          .map { case (key, bucket, _, _) => (key, bucket, row.getAs[Long]("from"), row.getAs[Long]("to"))}
      }
      .groupBy { case (key, bucket, _, _) => (key, bucket) }


    val joinedRDD = aiListComponents
      .join(queryRDD)
      .mapPartitions { rows => rows.flatMap { case ((key, bucket), (aiList, queries)) =>
        queries.flatMap { case (_, _, qFrom, qTo) =>
          aiList
            .overlapping(Interval(qFrom, qTo))
            .map(i => (key, i.from, i.to, qFrom, qTo))
        }
      }}

      joinedRDD
  }

  private def runStandardIntervalJoin(databaseDF: DataFrame, queryDF: DataFrame): JoinedRDD = {
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

    joinedRDD
  }

  // -------------------------------------------------------------------------------------------------------------------

}
