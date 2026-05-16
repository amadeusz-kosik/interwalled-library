package me.kosik.interwalled.ailist

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object AIListBuilder {

  def build(configuration: Configuration, sourceIntervals: Iterator[Interval]): Array[AIList] = {
    assert(configuration.intervalsCountToCheckLookahead >= configuration.intervalsCountToTriggerExtraction)
    assert(configuration.intervalsCountToCheckLookahead > 0)
    assert(configuration.maximumComponentSize > 0)

    var intervals = mutable.ArrayDeque.from(
      sourceIntervals.toArray.sorted(Ordering.by[Interval, (Long, Long)](i => (i.from, i.to)))
    )
    val results = ArrayBuffer[AIList]()

    while(intervals.nonEmpty) {
      val newComponent = mutable.ArrayBuilder.make[Interval]
      val leftovers = mutable.ArrayDeque[Interval]()

      while(intervals.nonEmpty && newComponent.length < configuration.maximumComponentSize) {
        val nextInterval = intervals.head
        intervals.remove(0)

        val includeInCurrentComponent = {
          val lookaheadCoverage = intervals.view
            .slice(0, configuration.intervalsCountToCheckLookahead)
            .count(_.to <= nextInterval.to)

          lookaheadCoverage <= configuration.intervalsCountToTriggerExtraction
        }

        if(includeInCurrentComponent)
          newComponent += nextInterval
        else
          leftovers += nextInterval
      }

      if (intervals.nonEmpty)
        leftovers.addAll(intervals)

      intervals = leftovers
      results += AIList(newComponent.result())
    }

    results.toArray
  }
}
