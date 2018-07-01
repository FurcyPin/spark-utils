package fpin.spark.utils.analysis

import scala.annotation.tailrec

case class ColumnAggregation(
  nbDistinct: Long,
  topN: List[(Long, Value)]
) {

  def merge(that: ColumnAggregation): ColumnAggregation = {
    ColumnAggregation(
      nbDistinct = this.nbDistinct + that.nbDistinct,
      topN = ColumnAggregation.mergeTopN(this.topN, that.topN)
    )
  }

}

object ColumnAggregation {

  private val defaultLimit: Int = 100

  private [analysis]
  def mergeTopN(
    leftTop: List[(Long, Value)],
    rightTop: List[(Long, Value)],
    limit: Int = defaultLimit
  ): List[(Long, Value)] = {

    @tailrec
    def recurse(
      leftTop: List[(Long, Value)],
      rightTop: List[(Long, Value)],
      res: List[(Long, Value)],
      size: Int
    ): List[(Long, Value)] = {
      (leftTop, rightTop) match {
        case _ if size >= limit =>
          res.reverse
        case (Nil, l2) =>
          res.reverse ++ l2.take(limit - size)
        case (l1, Nil) =>
          res.reverse ++ l1.take(limit - size)
        case ((c1, v1)::q1, l2 @ (c2, _)::_) if c1 >= c2 =>
          recurse(q1, l2, (c1, v1) :: res , size + 1 )
        case (l1 @ (c1, _)::_, (c2, v2)::q2) if c1 < c2 =>
          recurse(l1, q2, (c2, v2) :: res , size + 1 )
      }
    }
    recurse(leftTop, rightTop, Nil, 0)
  }

  def apply(value: Value, nbOccurrences: Long): ColumnAggregation = {
    new ColumnAggregation(
      topN = (nbOccurrences, value)::Nil ,
      nbDistinct = 1
    )
  }

}