package me.kosik.interwalled.ailist

case class AIList(intervals: Array[Interval], maxE: Array[Long]) {

    def length: Int =
        intervals.length

    def overlapping(query: Interval): Array[Interval] = {
        val lastCandidateIndex = SearchUtils.findRightmost(intervals, query.to)

        if(lastCandidateIndex > -1) {
            Range.inclusive(lastCandidateIndex, 0, -1)
                .takeWhile(i => query.from <= maxE(i))
                .filter(i => Interval.overlaps(intervals(i), query))
                .map(i => intervals(i))
                .toArray
        } else {
            Array.empty[Interval]
        }
    }
}

object AIList {
    def apply(intervals: Array[Interval]): AIList = {
        val maxE = {
            if(! intervals.isEmpty)
                intervals.scanLeft(Long.MinValue)(_ max _.to).drop(1)
            else
                Array.empty[Long]
        }

        AIList(intervals, maxE)
    }
}