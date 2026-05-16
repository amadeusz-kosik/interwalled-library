package me.kosik.interwalled.ailist

final case class Interval(
    from: Long,
    to: Long
)

object Interval {
    def overlaps(lhs: Interval, rhs: Interval): Boolean = {
        lhs.from <= rhs.to && rhs.from <= lhs.to
    }
}
