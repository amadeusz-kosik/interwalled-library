package me.kosik.interwalled.ailist

object TestDataGenerator {

  def consecutive(rowsCount: Int): Array[Interval] =
    generate(rowsCount, i => Interval(i, i))

  def consecutive(rowsCount: Int, offset: Int): Array[Interval] =
    generate(rowsCount, i => Interval(i + offset, i + offset))

  def consecutive(rowsCount: Int, offset: Int, rowWidth: Int): Array[Interval] =
    generate(rowsCount, i => Interval(i + offset, i + offset + rowWidth))


  private def generate(rowsCount: Int, intervalFn: Int => Interval): Array[Interval] = {
    (0 until rowsCount)
      .map(intervalFn)
      .toArray
  }
}
