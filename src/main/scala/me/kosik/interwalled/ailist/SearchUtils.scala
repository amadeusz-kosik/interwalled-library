package me.kosik.interwalled.ailist

import scala.annotation.tailrec

object SearchUtils {

  private val BinaryCutoff = 63

  def findRightmost(intervals: Array[Interval], queryEnd: Long): Int = {
    findRightmost(intervals, queryEnd, 0, intervals.length)
  }

  @tailrec
  private def findRightmost(intervals: Array[Interval], queryEnd: Long, leftBound: Int, rightBound: Int): Int = {
    if(rightBound - leftBound <= BinaryCutoff) {
      findBinary(intervals, queryEnd, leftBound, rightBound)
    } else {
      val middleIndex = (leftBound + rightBound) / 2

      if(intervals(middleIndex).from > queryEnd)
        findRightmost(intervals, queryEnd, leftBound, middleIndex)
      else
        findRightmost(intervals, queryEnd, middleIndex, rightBound)
    }
  }

  private def findBinary(intervals: Array[Interval], queryEnd: Long, leftBound: Int, rightBound: Int): Int = {
    if(intervals(leftBound).from > queryEnd) {
      -1
    } else {
      var index = leftBound

      while(index + 1 < rightBound && intervals(index + 1).from <= queryEnd)
        index += 1

      index
    }
  }
}
