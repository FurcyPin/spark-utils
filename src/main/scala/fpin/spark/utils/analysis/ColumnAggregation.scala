package fpin.spark.utils.analysis

import scala.annotation.tailrec

case class ColumnAggregation(
  count: Long,
  countDistinct: Long,
  countNull: Long,
  topN: List[(Long, Any)],
  min: Option[Any],
  max: Option[Any]
) {

  import ColumnAggregation._

  def merge(that: ColumnAggregation): ColumnAggregation = {


    ColumnAggregation(
      count = this.count + that.count,
      countDistinct = this.countDistinct + that.countDistinct,
      countNull = this.countNull + that.countNull,
      topN = ColumnAggregation.mergeTopN(this.topN, that.topN),
      min = genericMin(this.min, that.min),
      max = genericMax(this.max, that.max)
    )
  }

}

object ColumnAggregation {

  private val defaultLimit: Int = 100

  private def genericMin(left: Option[Any], right: Option[Any]): Option[Any] = {
    (left, right) match {
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(b)

      case (Some(a: Byte), Some(b: Byte)) if a <= b => Some(a)
      case (Some(a: Byte), Some(b: Byte)) => Some(b)

      case (Some(a: Char), Some(b: Char)) if a <= b => Some(a)
      case (Some(a: Char), Some(b: Char)) => Some(b)

      case (Some(a: String), Some(b: String)) if a <= b => Some(a)
      case (Some(a: String), Some(b: String)) => Some(b)

      case (Some(a: Short), Some(b: Short)) if a <= b => Some(a)
      case (Some(a: Short), Some(b: Short)) => Some(b)

      case (Some(a: Int), Some(b: Int)) if a <= b => Some(a)
      case (Some(a: Int), Some(b: Int)) => Some(b)

      case (Some(a: Long), Some(b: Long)) if a <= b => Some(a)
      case (Some(a: Long), Some(b: Long)) => Some(b)

      case (Some(a: Float), Some(b: Float)) if a <= b => Some(a)
      case (Some(a: Float), Some(b: Float)) => Some(b)

      case (Some(a: Double), Some(b: Double)) if a <= b => Some(a)
      case (Some(a: Double), Some(b: Double)) => Some(b)

      case (Some(a: java.sql.Date), Some(b: java.sql.Date)) if a.compareTo(b) <= 0 => Some(a)
      case (Some(a: java.sql.Date), Some(b: java.sql.Date)) => Some(b)

      case (Some(a: java.sql.Timestamp), Some(b: java.sql.Timestamp)) if a.compareTo(b) <= 0 => Some(a)
      case (Some(a: java.sql.Timestamp), Some(b: java.sql.Timestamp)) => Some(b)

      case _ => None
    }
  }

  private def genericMax(left: Option[Any], right: Option[Any]): Option[Any] = {
    (left, right) match {
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(b)

      case (Some(a: Byte), Some(b: Byte)) if a >= b => Some(a)
      case (Some(a: Byte), Some(b: Byte)) => Some(b)

      case (Some(a: Char), Some(b: Char)) if a >= b => Some(a)
      case (Some(a: Char), Some(b: Char)) => Some(b)

      case (Some(a: String), Some(b: String)) if a >= b => Some(a)
      case (Some(a: String), Some(b: String)) => Some(b)

      case (Some(a: Short), Some(b: Short)) if a >= b => Some(a)
      case (Some(a: Short), Some(b: Short)) => Some(b)

      case (Some(a: Int), Some(b: Int)) if a >= b => Some(a)
      case (Some(a: Int), Some(b: Int)) => Some(b)

      case (Some(a: Long), Some(b: Long)) if a >= b => Some(a)
      case (Some(a: Long), Some(b: Long)) => Some(b)

      case (Some(a: Float), Some(b: Float)) if a >= b => Some(a)
      case (Some(a: Float), Some(b: Float)) => Some(b)

      case (Some(a: Double), Some(b: Double)) if a >= b => Some(a)
      case (Some(a: Double), Some(b: Double)) => Some(b)

      case (Some(a: java.sql.Date), Some(b: java.sql.Date)) if a.compareTo(b) >= 0 => Some(a)
      case (Some(a: java.sql.Date), Some(b: java.sql.Date)) => Some(b)

      case (Some(a: java.sql.Timestamp), Some(b: java.sql.Timestamp)) if a.compareTo(b) >= 0 => Some(a)
      case (Some(a: java.sql.Timestamp), Some(b: java.sql.Timestamp)) => Some(b)

      case _ => None
    }
  }

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
    val comparableValue = value match {
      case v: Byte => Some(v)
      case v: Char => Some(v)
      case v: String => Some(v)
      case v: Short => Some(v)
      case v: Int => Some(v)
      case v: Long => Some(v)
      case v: Float => Some(v)
      case v: Double => Some(v)
      case v: java.sql.Timestamp => Some(v)
      case v: java.sql.Date => Some(v)
      case _ => None
    }

    val topN =
      value match {
//        case _ : Array[Byte] => Nil
        case _ => (nbOccurrences, value)::Nil
      }

    new ColumnAggregation(
      count = nbOccurrences,
      countDistinct = 1,
      countNull = if (value == null) nbOccurrences else 0,
      topN = topN,
      min = comparableValue,
      max = comparableValue
    )
  }

}