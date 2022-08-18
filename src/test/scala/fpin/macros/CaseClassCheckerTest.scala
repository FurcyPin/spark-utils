package fpin.macros

import org.scalatest.freespec.AnyFreeSpec

class CaseClassCheckerTest extends AnyFreeSpec {

  "CaseClassChecker should work" in {
    import CaseClassCheckerTest._

    assert( CaseClassChecker.isSubsetOf[Int, Int] === true )
    assert( CaseClassChecker.isSubsetOf[Int, Long] === false )

    assert( CaseClassChecker.isSubsetOf[Seq[Int], Seq[Int]] === true )
    assert( CaseClassChecker.isSubsetOf[Seq[Int], Seq[Long]] === false )

    assert( CaseClassChecker.isSubsetOf[User, User] === true )
    assert( CaseClassChecker.isSubsetOf[Users, Users] === true )
    assert( CaseClassChecker.isSubsetOf[BigUser, BigUser] === true )
    assert( CaseClassChecker.isSubsetOf[BigUsers, BigUsers] === true )

    assert( CaseClassChecker.isSubsetOf[User, BigUser] === true )
    assert( CaseClassChecker.isSubsetOf[Users, BigUsers] === true )

    assert( CaseClassChecker.isSubsetOf[BigUser, User] === false )
    assert( CaseClassChecker.isSubsetOf[BigUsers, Users] === false )

    assert( CaseClassChecker.isSubsetOf[Users, MapUsers] === false )
    assert( CaseClassChecker.isSubsetOf[MapUsers, BigMapUsers] === true )
  }

  "test run" in {
    import TestCaseClasses._

    assert(CaseClassChecker.isSubsetOf[Small, Big] === true)

    assert(CaseClassChecker.isSubsetOf[Big, Big] === true)
    assert(CaseClassChecker.isSubsetOf[Small, Big] === true)
    assert(CaseClassChecker.isSubsetOf[Big, Small] === false)
    assert(CaseClassChecker.isSubsetOf[SwappedSmall, Big] === true)
    assert(CaseClassChecker.isSubsetOf[Big, SwappedSmall] === false)
    assert(CaseClassChecker.isSubsetOf[BadSmall, Big] === false)
    assert(CaseClassChecker.isSubsetOf[Big, BadSmall] === false)

    assert(CaseClassChecker.isSubsetOf[BiggerNestedBig, BiggerNestedBig] === true)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedSmall, BiggerNestedBig] === true)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedBig, BiggerNestedBig] === true)
    assert(CaseClassChecker.isSubsetOf[BiggerNestedSmall, BiggerNestedBig] === true)

    assert(CaseClassChecker.isSubsetOf[BiggerNestedBig, SmallerNestedSmall] === false)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedSmall, SmallerNestedSmall] === true)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedBig, SmallerNestedSmall] === false)
    assert(CaseClassChecker.isSubsetOf[BiggerNestedSmall, SmallerNestedSmall] === false)

    assert(CaseClassChecker.isSubsetOf[BiggerNestedBig, SmallerNestedBig] === false)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedSmall, SmallerNestedBig] === true)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedBig, SmallerNestedBig] === true)
    assert(CaseClassChecker.isSubsetOf[BiggerNestedSmall, SmallerNestedBig] === false)

    assert(CaseClassChecker.isSubsetOf[BiggerNestedBig, BiggerNestedSmall] === false)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedSmall, BiggerNestedSmall] === true)
    assert(CaseClassChecker.isSubsetOf[SmallerNestedBig, BiggerNestedSmall] === false)
    assert(CaseClassChecker.isSubsetOf[BiggerNestedSmall, BiggerNestedSmall] === true)
  }

}


object CaseClassCheckerTest {

  case class User(id: Long, name: String)

  case class Users(users: Seq[User])

  case class BigUser(a: Int, id: Long, name: String)

  case class BigUsers(a: Int, users: Seq[BigUser])

  case class MapUsers(users: Map[User, User])

  case class BigMapUsers(a: Int, users: Map[BigUser, BigUser])

}

object TestCaseClasses {

  case class ExampleValue[T](value: T)

  case class Big(a: Int, b: Int, c: String)
  object Big {
    implicit val example: Big = Big(0, 1, "a")
  }

  case class Small(a: Int, b: Int)
  object Small {
    implicit val example: Small = Small(0, 1)
  }
  case class SwappedSmall(b: Int, a: Int)
  object SwappedSmall {
    implicit val example: SwappedSmall = SwappedSmall(1, 0)
  }
  case class BadSmall(a: Int, b:Int, c:Int)
  object BadSmall {
    implicit val example: BadSmall = BadSmall(0, 1, 2)
  }

  case class BiggerNestedBig(a: Int, nested: Big)
  object BiggerNestedBig {
    implicit val example: BiggerNestedBig = BiggerNestedBig(3, Big.example)
  }
  case class SmallerNestedSmall(nested: Small)
  object SmallerNestedSmall {
    implicit val example: SmallerNestedSmall = SmallerNestedSmall(Small.example)
  }
  case class SmallerNestedBig(nested: Big)
  object SmallerNestedBig {
    implicit val example: SmallerNestedBig = SmallerNestedBig(Big.example)
  }
  case class BiggerNestedSmall(a: Int, nested: Small)
  object BiggerNestedSmall {
    implicit val example: BiggerNestedSmall = BiggerNestedSmall(3, Small.example)
  }

}
