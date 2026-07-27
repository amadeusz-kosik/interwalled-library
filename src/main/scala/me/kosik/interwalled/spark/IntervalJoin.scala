package me.kosik.interwalled.spark

import me.kosik.interwalled.ailist.{AIList, AIListBuilder, Configuration, Interval}
import me.kosik.interwalled.spark.IntervalJoin.{DatabaseQueryChoice, JoinedDS}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession, functions => F}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer


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
    thresholdGroupSplit: Long = 1_000_000,

    /**
     * Maximum number of rows per group in query. If a group exceeds this, it will be split on query side and multiplied
     *  on the database side (salted). This does not apply to broadcast join.
     */
    thresholdSaltQuery: Long = 100_000,

    /**
     * Maximum number of rows per single batch in the database dataset.
     */
    thresholdDatabaseBatchSize: Long = 1_000_000
  )

  case class DatabaseQueryChoice(
    database: DataFrame,
    databaseCount: Long,
    query: DataFrame,
    queryCount: Long,
    isSwapped: Boolean
  )

  type JoinedDS = Dataset[(String, Long, Long, Long, Long)]

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

  def toDataFrame(joinedDS: JoinedDS): DataFrame = {
    joinedDS.toDF("key", "lhs_from", "lhs_to", "rhs_from", "rhs_to")
  }

  def toStrictDataFrame(joinedDS: JoinedDS)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(
      joinedDS
        .toDF("key", "lhs_from", "lhs_to", "rhs_from", "rhs_to")
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
}

class IntervalJoin(configuration: IntervalJoin.Configuration) extends Serializable with Logging {
  // FIXME: this function has no description
  def join(lhs: DataFrame, rhs: DataFrame)(implicit sparkSession: SparkSession): (List[String], JoinedDS) = {
    import sparkSession.implicits._

    val eventLog = ListBuffer[String]()
    val databaseQueryChoice = chooseDatabaseAndQuery(lhs, rhs)

    log.info(s"Database size: ${databaseQueryChoice.databaseCount}, query size: ${databaseQueryChoice.queryCount}.")
    eventLog.append(s"database_size=${databaseQueryChoice.databaseCount}")
    eventLog.append(s"query_size=${databaseQueryChoice.queryCount}")

    val (joinMethod, joinedDS) = selectAndRunIntervalJoinStrategy(databaseQueryChoice, eventLog)

    log.info(s"Chosen join method: $joinMethod.")
    eventLog.append(s"join_method=$joinMethod")

    val finalDS = {
      if (databaseQueryChoice.isSwapped)
        joinedDS.map { case (key, rhsFrom, rhsTo, lhsFrom, lhsTo) => (key, lhsFrom, lhsTo, rhsFrom, rhsTo) }
      else
        joinedDS
    }

    eventLog.toList -> finalDS
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

  private def selectAndRunIntervalJoinStrategy(databaseQueryChoice: DatabaseQueryChoice, eventLog: ListBuffer[String])(implicit sparkSession: SparkSession): (String, JoinedDS) = {
    if (databaseQueryChoice.databaseCount <= configuration.thresholdBroadcastJoinRowsCount)
      "broadcast" -> runBroadcastIntervalJoin(databaseQueryChoice.database, databaseQueryChoice.query)
    else
      "ranked" -> runRankedIntervalJoin(databaseQueryChoice, eventLog)
  }

  // -------------------------------------------------------------------------------------------------------------------

  private def runBroadcastIntervalJoin(database: DataFrame, query: DataFrame)(implicit sparkSession: SparkSession): JoinedDS = {
    import sparkSession.implicits._

    val databaseDS = database
      .select("key", "from", "to")
      .groupBy("key").agg(F.collect_list(F.struct("from", "to").as("__interval")))
      .as[(String, List[(Long, Long)])]

    val aiLists: Map[String, Array[AIList]] = databaseDS
      .collect()
      .map { case (key, intervals) =>
        val intervalsIterator = intervals.map { case (from, to) => Interval(from, to) }.iterator
        key -> AIListBuilder.build(Configuration.apply(), intervalsIterator)
      }
      .toMap

    val aiListsBroadcast = sparkSession.sparkContext
      .broadcast(aiLists)

    val queryDS = query
      .select("key", "from", "to")
      .as[(String, Long, Long)]

    val joinedDS = queryDS
      .flatMap { case (qKey, qFrom, qTo) =>

        aiListsBroadcast.value
          .get(qKey)
          .map { aiLists =>
            aiLists
              .flatMap(_.overlapping(Interval(qFrom, qTo)))
              .map(dInterval => (qKey, dInterval.from, dInterval.to, qFrom, qTo))
          }
          .getOrElse(Array.empty)
      }

    joinedDS
  }

  private def runRankedIntervalJoin(inputData: DatabaseQueryChoice, eventLog: ListBuffer[String])(implicit sparkSession: SparkSession): JoinedDS = {
    import sparkSession.implicits._

    val (batchedDatabaseDF, databaseMasterBatches) = addBatchColumn(
      inputData.database,
      inputData.databaseCount,
      configuration.thresholdDatabaseBatchSize,
      eventLog
    )

    val queryRanks = computeRanks(inputData.query)
    val queryRanksBroadcast = sparkSession.sparkContext.broadcast(queryRanks)

    eventLog += s"Query ranks: ${queryRanks.size}"

    val queryDS = inputData.query
      .join(queryRanks.toList.toDF("key", "lookup_count"), Array("key"))
      .withColumn("__salt", F.floor(F.rand() * F.col("lookup_count") / F.lit(configuration.thresholdSaltQuery)).cast(DataTypes.IntegerType))
      .select("key", "__salt", "from", "to")
      .cache()

    // Split database into batches
    val batchedResults = (0 until databaseMasterBatches) map { databaseBatchIndex =>
      val databaseBatchDF = batchedDatabaseDF
        .filter(F.col("__batch") === F.lit(databaseBatchIndex))

      val databaseRanks = computeRanks(databaseBatchDF)

      val groupsToSplit = databaseRanks
        .filter { case (_, rowsCount) => rowsCount > configuration.thresholdGroupSplit }
        .keys
        .toList

      val rankedDatabaseDF = databaseBatchDF
        .withColumn("__rank",
          F.when(F.col("key").isin(groupsToSplit.toArray: _*),
              F.dense_rank().over(Window.partitionBy("key").orderBy(F.col("from").asc, F.col("to").asc)))
            .otherwise(F.lit(0))
        )
        .withColumn("__bucket", (F.col("__rank") / F.lit(configuration.thresholdGroupSplit)).cast(DataTypes.IntegerType))
        .drop("__rank")
        .persist(StorageLevel.MEMORY_AND_DISK)

      val ranks = rankedDatabaseDF
        .groupBy("key", "__bucket")
        .agg(F.min("from").as("min_from"), F.max("to").as("max_to"))
        .collect()
        .map(row => (row.getAs[String]("key"), row.getAs[Int]("__bucket"), row.getAs[Long]("min_from"), row.getAs[Long]("max_to")))

      val ranksBroadcast = sparkSession.sparkContext.broadcast(ranks)
      eventLog += s"Database ranks: ${ranks.length}"

      val aiListsDS = rankedDatabaseDF
        .select("key", "__bucket", "from", "to")
        .repartition(F.col("key"), F.col("__bucket"))
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
        .flatMap { case ((key, bucket), lists) =>
          lists.map(list => (key, bucket, list))
        }
        .flatMap { case (key, bucket, list) =>
          val lookupCount: Long = queryRanksBroadcast.value.getOrElse(key, 1)
          (0 to (lookupCount / configuration.thresholdSaltQuery).toInt)
            .toArray
            .map(salt => (key, bucket, salt, list))
        }
        .repartition(F.col("_1"), F.col("_2"), F.col("_3"))

      val preparedQueryDS = queryDS
        .join(queryRanks.toList.toDF("key", "lookup_count"), Array("key"))
        .withColumn("__salt", F.floor(F.rand() * F.col("lookup_count") / F.lit(configuration.thresholdSaltQuery)).cast(DataTypes.IntegerType))
        .select("key", "__salt", "from", "to")
        .flatMap { row =>
          ranksBroadcast
            .value
            .filter { case (key, _, minFrom, maxTo) => row.getAs[String]("key") == key && row.getAs[Long]("from") <= maxTo && row.getAs[Long]("to") >= minFrom }
            .map { case (key, bucket, _, _) => (key, bucket, row.getAs[Int]("__salt"), row.getAs[Long]("from"), row.getAs[Long]("to"))}
        }
        .toDF("key", "__bucket", "__salt", "from", "to")
        .repartition(F.col("key"), F.col("__bucket"), F.col("__salt"))
        .groupBy("key", "__bucket", "__salt")
        .agg(F.collect_list(F.struct(F.col("from"), F.col("to"))).as("__queries"))

      val joinedDS = aiListsDS
        .joinWith(preparedQueryDS, (aiListsDS.col("_1") === preparedQueryDS.col("key")) and (aiListsDS.col("_2") === preparedQueryDS.col("__bucket")) and (aiListsDS.col("_3") === preparedQueryDS.col("__salt")))
        .as[((String, Int, Int, AIList), (String, Int, Int, List[(Long, Long)]))]
        .mapPartitions { rows => rows.flatMap { case ((key, _, _, aiList), (_, _, _, queries)) =>
          queries.flatMap { case (qFrom, qTo) =>
            aiList
              .overlapping(Interval(qFrom, qTo))
              .map(i => (key, i.from, i.to, qFrom, qTo))
          }
        }}

      joinedDS
    }

    unionAll(batchedResults.toList)
  }

  // -------------------------------------------------------------------------------------------------------------------

  private def addBatchColumn(data: DataFrame, dataRowsCount: Long, threshold: Long, eventLog: ListBuffer[String]): (DataFrame, Int) = {
    val batchesCount = ((dataRowsCount - 1) / threshold).toInt + 1
    eventLog += s"Database batches count: $batchesCount."

    val batchedData = if(batchesCount > 1) {
      data
        .withColumn("__hash",   F.hash(F.col("key"), F.col("from"), F.col("to")))
        .withColumn("__batch",  F.abs(F.col("__hash")) % F.lit(batchesCount))
    } else {
      data
        .withColumn("__hash",   F.lit(0))
        .withColumn("__batch",  F.lit(0))
    }

    (batchedData, batchesCount)
  }

  private def computeRanks(data: DataFrame): Map[String, Long] = {
    data
      .groupBy("key")
      .agg(F.count("*").as("__lookup_count"))
      .collect()
      .map(row => (row.getAs[String]("key"), row.getAs[Long]("__lookup_count")))
      .toMap
  }

  private def unionAll[T : Encoder](datasets: List[Dataset[T]])(implicit sparkSession: SparkSession): Dataset[T] = datasets match {
    case Nil =>
      sparkSession.createDataset(Seq.empty[T])

    case head :: Nil =>
      head

    case head :: tail =>
      head.unionByName(unionAll(tail))
  }
}
