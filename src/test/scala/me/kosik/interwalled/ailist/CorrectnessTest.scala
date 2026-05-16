package me.kosik.interwalled.ailist

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class CorrectnessTest extends AnyFunSpec with Matchers {

  describe("AIList built with deque") {
    def computeActual(aiLists: Array[AIList], query: Array[Interval]): Array[(Interval, Interval)] =
      for {
        aiList      <- aiLists
        rhsInterval <- query
        lhsInterval <- aiList.overlapping(rhsInterval)
      } yield (lhsInterval, rhsInterval)

    it("should return empty list if no intervals overlap") {
      val configuration = Configuration()
      val database = TestDataGenerator.consecutive(100).iterator
      val query    = TestDataGenerator.consecutive(100, 200)

      val aiLists = AIListBuilder.build(configuration, database)

      val expected = Array.empty[(Interval, Interval)]
      val actual = computeActual(aiLists, query)

      actual should contain theSameElementsAs expected
    }

    it("should correctly map 1:1 relation") {
      val configuration = Configuration()
      val database = TestDataGenerator.consecutive(100).iterator
      val query    = TestDataGenerator.consecutive(100)

      val aiLists = AIListBuilder.build(configuration, database)

      val expected = TestDataGenerator.consecutive(100).map(i => (i, i))
      val actual = computeActual(aiLists, query)

      actual should contain theSameElementsAs expected
    }

    it("should correctly map 1:all relation") {
      val configuration = Configuration.apply()
      val database = TestDataGenerator.consecutive(1, 0, 100).iterator
      val query    = TestDataGenerator.consecutive(100)

      val aiLists = AIListBuilder.build(configuration, database)

      val expected = TestDataGenerator.consecutive(100).map(i => (Interval(0, 100), i))
      val actual = computeActual(aiLists, query)

      actual should contain theSameElementsAs expected
    }

    it("should correctly map all:1 relation") {
      val configuration = Configuration.apply()
      val database = TestDataGenerator.consecutive(100).iterator
      val query    = TestDataGenerator.consecutive(1, 0, 100)

      val aiLists = AIListBuilder.build(configuration, database)

      val expected = TestDataGenerator.consecutive(100).map(i => (i, Interval(0, 100)))
      val actual = computeActual(aiLists, query)

      actual should contain theSameElementsAs expected
    }
  }
}