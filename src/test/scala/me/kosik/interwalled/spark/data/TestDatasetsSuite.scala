package me.kosik.interwalled.spark.data


import me.kosik.interwalled.spark.utils.WithDataFrameAssertions
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec


class TestDatasetsSuite extends AnyFunSpec with WithDataFrameAssertions {
  private val TestDataGroupsCount = 4
  private val TestDataSize = 16

  describe("DataSuites.databaseUniformFlat") {
    it("should generate set of consecutive point intervals") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.databaseUniformFlat(TestDataSize, TestDataGroupsCount)
      val expectedData = List(
        DataSuiteTestRow("CH-0", 0, 0),
        DataSuiteTestRow("CH-0", 1, 1),
        DataSuiteTestRow("CH-0", 2, 2),
        DataSuiteTestRow("CH-0", 3, 3),
        DataSuiteTestRow("CH-1", 0, 0),
        DataSuiteTestRow("CH-1", 1, 1),
        DataSuiteTestRow("CH-1", 2, 2),
        DataSuiteTestRow("CH-1", 3, 3),
        DataSuiteTestRow("CH-2", 0, 0),
        DataSuiteTestRow("CH-2", 1, 1),
        DataSuiteTestRow("CH-2", 2, 2),
        DataSuiteTestRow("CH-2", 3, 3),
        DataSuiteTestRow("CH-3", 0, 0),
        DataSuiteTestRow("CH-3", 1, 1),
        DataSuiteTestRow("CH-3", 2, 2),
        DataSuiteTestRow("CH-3", 3, 3)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseUniformStacked") {
    it("should generate set of consecutive wide intervals, each overlapping some of its neighbours") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.databaseUniformStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", -20, 20),
        DataSuiteTestRow("CH-0", -19, 21),
        DataSuiteTestRow("CH-0", -18, 22),
        DataSuiteTestRow("CH-0", -17, 23),
        DataSuiteTestRow("CH-1", -20, 20),
        DataSuiteTestRow("CH-1", -19, 21),
        DataSuiteTestRow("CH-1", -18, 22),
        DataSuiteTestRow("CH-1", -17, 23),
        DataSuiteTestRow("CH-2", -20, 20),
        DataSuiteTestRow("CH-2", -19, 21),
        DataSuiteTestRow("CH-2", -18, 22),
        DataSuiteTestRow("CH-2", -17, 23),
        DataSuiteTestRow("CH-3", -20, 20),
        DataSuiteTestRow("CH-3", -19, 21),
        DataSuiteTestRow("CH-3", -18, 22),
        DataSuiteTestRow("CH-3", -17, 23)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseUniformHeavyStacked") {
    it("should generate set of consecutive intervals, all starting to 0 and lasting incrementally further") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.databaseUniformHeavyStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", 0, 5),
        DataSuiteTestRow("CH-0", 0, 6),
        DataSuiteTestRow("CH-0", 0, 7),
        DataSuiteTestRow("CH-0", 0, 8),
        DataSuiteTestRow("CH-1", 0, 5),
        DataSuiteTestRow("CH-1", 0, 6),
        DataSuiteTestRow("CH-1", 0, 7),
        DataSuiteTestRow("CH-1", 0, 8),
        DataSuiteTestRow("CH-2", 0, 5),
        DataSuiteTestRow("CH-2", 0, 6),
        DataSuiteTestRow("CH-2", 0, 7),
        DataSuiteTestRow("CH-2", 0, 8),
        DataSuiteTestRow("CH-3", 0, 5),
        DataSuiteTestRow("CH-3", 0, 6),
        DataSuiteTestRow("CH-3", 0, 7),
        DataSuiteTestRow("CH-3", 0, 8),
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseSkewedFlat") {
    it("should generate set of consecutive intervals, but the first group should be bigger than any other") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.databaseSkewedFlat(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0",  0,  0),
        DataSuiteTestRow("CH-1",  1,  1),
        DataSuiteTestRow("CH-2",  2,  2),
        DataSuiteTestRow("CH-3",  3,  3),
        DataSuiteTestRow("CH-0",  4,  4),
        DataSuiteTestRow("CH-0",  5,  5),
        DataSuiteTestRow("CH-0",  6,  6),
        DataSuiteTestRow("CH-0",  7,  7),
        DataSuiteTestRow("CH-0",  8,  8),
        DataSuiteTestRow("CH-1",  9,  9),
        DataSuiteTestRow("CH-2", 10, 10),
        DataSuiteTestRow("CH-3", 11, 11),
        DataSuiteTestRow("CH-0", 12, 12),
        DataSuiteTestRow("CH-0", 13, 13),
        DataSuiteTestRow("CH-0", 14, 14),
        DataSuiteTestRow("CH-0", 15, 15)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseSkewedStacked") {
    it("should generate set of increasingly overlapping intervals, but the first group should be bigger than any other") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.databaseSkewedStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", -20, 20),
        DataSuiteTestRow("CH-1", -19, 21),
        DataSuiteTestRow("CH-2", -18, 22),
        DataSuiteTestRow("CH-3", -17, 23),
        DataSuiteTestRow("CH-0", -16, 24),
        DataSuiteTestRow("CH-0", -15, 25),
        DataSuiteTestRow("CH-0", -14, 26),
        DataSuiteTestRow("CH-0", -13, 27),
        DataSuiteTestRow("CH-0", -12, 28),
        DataSuiteTestRow("CH-1", -11, 29),
        DataSuiteTestRow("CH-2", -10, 30),
        DataSuiteTestRow("CH-3",  -9, 31),
        DataSuiteTestRow("CH-0",  -8, 32),
        DataSuiteTestRow("CH-0",  -7, 33),
        DataSuiteTestRow("CH-0",  -6, 34),
        DataSuiteTestRow("CH-0",  -5, 35)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.querySparse") {
    it("should generate set of increasingly non-overlapping intervals with some padding between them") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.querySparse(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0",  0,  0),
        DataSuiteTestRow("CH-0", 10, 10),
        DataSuiteTestRow("CH-0", 20, 20),
        DataSuiteTestRow("CH-0", 30, 30),
        DataSuiteTestRow("CH-1",  0,  0),
        DataSuiteTestRow("CH-1", 10, 10),
        DataSuiteTestRow("CH-1", 20, 20),
        DataSuiteTestRow("CH-1", 30, 30),
        DataSuiteTestRow("CH-2",  0,  0),
        DataSuiteTestRow("CH-2", 10, 10),
        DataSuiteTestRow("CH-2", 20, 20),
        DataSuiteTestRow("CH-2", 30, 30),
        DataSuiteTestRow("CH-3",  0,  0),
        DataSuiteTestRow("CH-3", 10, 10),
        DataSuiteTestRow("CH-3", 20, 20),
        DataSuiteTestRow("CH-3", 30, 30)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.queryDense") {
    it("should generate set of increasingly non-overlapping intervals without any padding between them") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.queryDense(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0",  0,  9),
        DataSuiteTestRow("CH-0", 10, 19),
        DataSuiteTestRow("CH-0", 20, 29),
        DataSuiteTestRow("CH-0", 30, 39),
        DataSuiteTestRow("CH-1",  0,  9),
        DataSuiteTestRow("CH-1", 10, 19),
        DataSuiteTestRow("CH-1", 20, 29),
        DataSuiteTestRow("CH-1", 30, 39),
        DataSuiteTestRow("CH-2",  0,  9),
        DataSuiteTestRow("CH-2", 10, 19),
        DataSuiteTestRow("CH-2", 20, 29),
        DataSuiteTestRow("CH-2", 30, 39),
        DataSuiteTestRow("CH-3",  0,  9),
        DataSuiteTestRow("CH-3", 10, 19),
        DataSuiteTestRow("CH-3", 20, 29),
        DataSuiteTestRow("CH-3", 30, 39)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.queryStacked") {
    it("should generate set of increasingly overlapping intervals") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.queryStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", -10, 19),
        DataSuiteTestRow("CH-0",   0, 29),
        DataSuiteTestRow("CH-0",  10, 39),
        DataSuiteTestRow("CH-0",  20, 49),
        DataSuiteTestRow("CH-1", -10, 19),
        DataSuiteTestRow("CH-1",   0, 29),
        DataSuiteTestRow("CH-1",  10, 39),
        DataSuiteTestRow("CH-1",  20, 49),
        DataSuiteTestRow("CH-2", -10, 19),
        DataSuiteTestRow("CH-2",   0, 29),
        DataSuiteTestRow("CH-2",  10, 39),
        DataSuiteTestRow("CH-2",  20, 49),
        DataSuiteTestRow("CH-3", -10, 19),
        DataSuiteTestRow("CH-3",   0, 29),
        DataSuiteTestRow("CH-3",  10, 39),
        DataSuiteTestRow("CH-3",  20, 49)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.querySkewedDense") {
    it("should generate skewed set of overlapping intervals") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.querySkewedDense(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", -20, 20),
        DataSuiteTestRow("CH-0", -19, 21),
        DataSuiteTestRow("CH-0", -18, 22),
        DataSuiteTestRow("CH-0", -17, 23),
        DataSuiteTestRow("CH-0", -16, 24),
        DataSuiteTestRow("CH-0", -15, 25),
        DataSuiteTestRow("CH-0", -14, 26),
        DataSuiteTestRow("CH-0", -13, 27),
        DataSuiteTestRow("CH-0", -12, 28),
        DataSuiteTestRow("CH-0", -11, 29),
        DataSuiteTestRow("CH-0", -10, 30),
        DataSuiteTestRow("CH-0",  -9, 31),
        DataSuiteTestRow("CH-0",  -8, 32),
        DataSuiteTestRow("CH-0",  -7, 33),
        DataSuiteTestRow("CH-0",  -6, 34),
        DataSuiteTestRow("CH-0",  -5, 35)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.queryDummy") {
    it("should generate uniform set of non-overlapping intervals, but half of them does not match by group") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = TestDatasets.queryDummy(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0",  0, 9),
        DataSuiteTestRow("CH-1",  0, 9),
        DataSuiteTestRow("CH-2",  0, 9),
        DataSuiteTestRow("CH-3",  0, 9),
        DataSuiteTestRow("CH-4",  0, 9),
        DataSuiteTestRow("CH-5",  0, 9),
        DataSuiteTestRow("CH-6",  0, 9),
        DataSuiteTestRow("CH-7",  0, 9),
        DataSuiteTestRow("CH-8",  0, 9),
        DataSuiteTestRow("CH-9",  0, 9),
        DataSuiteTestRow("CH-10", 0, 9),
        DataSuiteTestRow("CH-11", 0, 9),
        DataSuiteTestRow("CH-12", 0, 9),
        DataSuiteTestRow("CH-13", 0, 9),
        DataSuiteTestRow("CH-14", 0, 9),
        DataSuiteTestRow("CH-15", 0, 9)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }
}
