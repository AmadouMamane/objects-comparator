package com.mydevs.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.storage.StorageLevel
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import shapeless.{HList, LabelledGeneric}

import scala.collection.generic.CanBuildFrom


case class IgnoredFields(all: Symbol*)

trait CompareIgnoringFields[T] extends Serializable {
  def compareIgnoringFields(t1: T, t2: T, allIgnoredFields: IgnoredFields): Boolean

  def compareIgnoringFields(t1: Dataset[T], t2: Dataset[T], pks: Set[String], allIgnoredFields: IgnoredFields): Map[String, Dataset[_]]
}


object CompareIgnoringFields {
  implicit val ignoredFields = IgnoredFields('update_time, 'bkv)

  def apply[A: CompareIgnoringFields]: CompareIgnoringFields[A] = implicitly[CompareIgnoringFields[A]]

  implicit def listSerializableCanBuildFrom[T]: CanBuildFrom[List[T], T, List[T]] =
    new CanBuildFrom[List[T], T, List[T]] with Serializable {
      def apply(from: List[T]) = from.genericBuilder[T]

      def apply() = List.newBuilder[T]
    }

  implicit def ops[T <: Product : Encoder, Repr <: HList, KeysRepr <: HList](implicit gen: LabelledGeneric.Aux[T, Repr],
                                                                             keys: Keys.Aux[Repr, KeysRepr],
                                                                             traversable: ToTraversable.Aux[KeysRepr, List, Symbol]
                                                                            ): CompareIgnoringFields[T] = {
    new CompareIgnoringFields[T] {
      override def compareIgnoringFields(t1: T, t2: T, allIgnoredFields: IgnoredFields): Boolean = {
        val caseClassFields = keys().toList.map(_.name).asInstanceOf[Seq[String]]
        val ignoredFields = allIgnoredFields.all.toSet.map((symbol: Symbol) => symbol.name)
        val comparisonIndices = caseClassFields.zipWithIndex.filter((fieldZip: (String, Int)) => !ignoredFields.contains(fieldZip._1)).map(_._2)
        computeHash(t1, ignoredFields, comparisonIndices) == computeHash(t2, ignoredFields, comparisonIndices)
      }

      override def compareIgnoringFields(t1: Dataset[T], t2: Dataset[T], pks: Set[String], allIgnoredFields: IgnoredFields)
      : Map[String, Dataset[_]] = {
        assert(pks subsetOf t1.columns.toSet)
        val columnPk = pks.map(colName => col("left." + colName).equalTo(col("right." + colName)))
        val joinPkCol = columnPk.reduce((a, b) => a.and(b))
        val cached = t1.alias("left").joinWith(t2.alias("right"), joinPkCol, "full_outer").persist(StorageLevel.MEMORY_AND_DISK)
        val RowsNotInLeft = cached.filter(r => Option(r._2).isDefined && Option(r._1).isEmpty).map(_._2)
        val RowsNotInRight = cached.filter(r => Option(r._1).isDefined && Option(r._2).isEmpty).map(_._1)
        val RowsWithSomeDifferenciesAll = cached.filter(r => Option(r._1).isDefined && Option(r._2).isDefined
          && !r._1.compareIgnoringFields(r._2)(allIgnoredFields, CompareIgnoringFields[T]))
        val identicalRows = cached.filter(r => Option(r._1).isDefined && Option(r._2).isDefined
          && r._1.compareIgnoringFields(r._2)(allIgnoredFields, CompareIgnoringFields[T])).map(_._2)
        val RowsWithSomeDifferencies = RowsWithSomeDifferenciesAll.map(_._2)
        val RowsWithSomeDifferenciesAllLimited: Dataset[(T, T)] = RowsWithSomeDifferenciesAll.limit(30).cache()
        Map("RowsNotInLeft" -> RowsNotInLeft, "RowsWithSomeDifferencies" -> RowsWithSomeDifferencies,
          "RowsNotInRight" -> RowsNotInRight, "IdenticalRows" -> identicalRows, "RowsWithSomeDifferenciesAllLimitedLeft" ->  RowsWithSomeDifferenciesAllLimited.map(_._1)
          ,"RowsWithSomeDifferenciesAllLimitedRight" ->  RowsWithSomeDifferenciesAllLimited.map(_._2))
      }
    }
  }

  def computeHash[T <: Product](elem: T, ignoredFields: Set[String], comparisonIndices: Seq[Int]): Int = comparisonIndices.map(elem.productElement(_).##).toList.##

  implicit class Comparator[T <: Product, Repr <: HList, KeysRepr <: HList](toCompare: T) {
    def compareIgnoringFields(other: T)(implicit allIgnoredFields: IgnoredFields, eqT: CompareIgnoringFields[T]): Boolean = eqT.compareIgnoringFields(toCompare, other, allIgnoredFields)
  }

  implicit class DatasetComparator[T](toCompare: Dataset[T]) {
    def compareIgnoringFields(other: Dataset[T])(pks: Set[String])(implicit ignoredFields: IgnoredFields, eqT: CompareIgnoringFields[T])
    : Map[String, Dataset[_]] = eqT.compareIgnoringFields(toCompare, other, pks, ignoredFields)
  }
}
