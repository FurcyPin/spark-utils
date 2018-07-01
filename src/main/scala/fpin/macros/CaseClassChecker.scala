package fpin.macros

import scala.reflect.macros.Context
import scala.language.experimental.macros

object CaseClassChecker {

  def assertIsSubsetOf[L, R]: Unit = macro assertIsSubsetOfMacro[L, R]

  def isSubsetOf[L, R]: Boolean = macro isSubsetOfMacro[L, R]

  def getFieldNamesAndTypes(c: Context)(tpe: c.universe.Type): Iterable[(c.universe.Name, c.universe.Type)] = {
    import c.universe._

    object CaseField {
      def unapply(trmSym: TermSymbol): Option[(Name, Type)] = {
        if (trmSym.isVal && trmSym.isCaseAccessor)
          Some((newTermName(trmSym.name.toString.trim), trmSym.typeSignature))
        else
          None
      }
    }

    tpe.declarations.collect {
      case CaseField(nme, tpe) =>
        (nme, tpe)
    }
  }

  def isSubsetOfMacro[L: c.WeakTypeTag, R: c.WeakTypeTag](c: Context): c.Expr[Boolean] = {
    import c.universe._
    val l: Type = weakTypeOf[L]
    val r: Type = weakTypeOf[R]

    val res: Boolean = isSubsetOf(c)(l,r)
    if(res){
      c.Expr[Boolean](reify(true).tree)
    }
    else {
      c.Expr[Boolean](reify(false).tree)
    }
  }

  def assertIsSubsetOfMacro[L: c.WeakTypeTag, R: c.WeakTypeTag](c: Context): c.Expr[Unit] = {
    import c.universe._
    val l: Type = weakTypeOf[L]
    val r: Type = weakTypeOf[R]

    val res: Boolean = isSubsetOf(c)(l,r)
    if(!res) {
      c.error(c.enclosingPosition, s"Type $l is not a subset of Type $r")
    }
    c.Expr[Unit](reify(()).tree)
  }

  def isSubsetOf(c: Context)(lType: c.Type, rType: c.Type): Boolean = {
    import c.universe._
    if (lType == rType) {
      true
    }
    else {
      (lType, rType) match {
        case (TypeRef(_, _, lArgs), TypeRef(_, _, rArgs)) if lArgs.nonEmpty =>
          lArgs.size == rArgs.size &&
            lArgs.zip(rArgs).forall { case (x, y) => isSubsetOf(c)(x, y) }
        case (ll, rr) =>
          val lNT: Iterable[(c.universe.Name, c.universe.Type)] = getFieldNamesAndTypes(c)(ll)
          val rNT: Map[c.universe.Name, c.universe.Type] = getFieldNamesAndTypes(c)(rr).toMap
          if (lNT.isEmpty) {
            false
          }
          else {
            lNT.forall {
              case (fieldName, fieldType) => rNT.get(fieldName).isDefined && isSubsetOf(c)(fieldType, rNT(fieldName))
            }
          }
      }
    }
  }
}