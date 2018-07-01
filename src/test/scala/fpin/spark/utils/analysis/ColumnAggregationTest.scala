package fpin.spark.utils.analysis

import org.scalatest.FreeSpec

class ColumnAggregationTest extends FreeSpec {

  "mergeTopN should work" - {

    "with two tops" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = List(10l -> "a", 5l -> "b", 3l -> "c"),
          rightTop = List(11l -> "d", 6l -> "e", 2l -> "f")
        )
      val expected = List(11l -> "d", 10l -> "a", 6l -> "e", 5l -> "b", 3l -> "c", 2l -> "f")
      assert(actual === expected)
    }

    "with two when the limit N is reached" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = List(10l -> "a", 5l -> "b", 3l -> "c"),
          rightTop = List(11l -> "d", 6l -> "e", 2l -> "f"),
          limit = 5
        )
      val expected = List(11l -> "d", 10l -> "a", 6l -> "e", 5l -> "b", 3l -> "c")
      assert(actual === expected)
    }

    "with the left top empty" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = Nil,
          rightTop = List(11l -> "d", 6l -> "e", 2l -> "f")
        )
      val expected = List(11l -> "d", 6l -> "e", 2l -> "f")
      assert(actual === expected)
    }

    "with the right top empty" in {
      val actual =
        ColumnAggregation.mergeTopN(
          leftTop = List(10l -> "a", 5l -> "b", 3l -> "c"),
          rightTop = Nil
        )
      val expected = List(10l -> "a", 5l -> "b", 3l -> "c")
      assert(actual === expected)
    }
  }

}
