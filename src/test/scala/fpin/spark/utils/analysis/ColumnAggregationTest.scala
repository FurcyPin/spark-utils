package fpin.spark.utils.analysis

import org.scalatest.freespec.AnyFreeSpec

class ColumnAggregationTest extends AnyFreeSpec {

  "mergeTopN should work" - {

    "with two tops" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = List(10L -> "a", 5L -> "b", 3L -> "c"),
          rightTop = List(11L -> "d", 6L -> "e", 2L -> "f")
        )
      val expected = List(11L -> "d", 10L -> "a", 6L -> "e", 5L -> "b", 3L -> "c", 2L -> "f")
      assert(actual === expected)
    }

    "with two when the limit N is reached" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = List(10L -> "a", 5L -> "b", 3L -> "c"),
          rightTop = List(11L -> "d", 6L -> "e", 2L -> "f"),
          limit = 5
        )
      val expected = List(11L -> "d", 10L -> "a", 6L -> "e", 5L -> "b", 3L -> "c")
      assert(actual === expected)
    }

    "with the left top empty" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = Nil,
          rightTop = List(11L -> "d", 6L -> "e", 2L -> "f")
        )
      val expected = List(11L -> "d", 6L -> "e", 2L -> "f")
      assert(actual === expected)
    }

    "with the right top empty" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = List(10L -> "a", 5L -> "b", 3L -> "c"),
          rightTop = Nil
        )
      val expected = List(10L -> "a", 5L -> "b", 3L -> "c")
      assert(actual === expected)
    }
  }

}
