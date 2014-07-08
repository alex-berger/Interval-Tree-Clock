package ch.osgiliath.logical.clocks.itc

import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.collection.mutable.ArrayBuffer

sealed abstract class ID {

  def normalize: ID

  def split: (ID, ID)

  def join(rhs: ID): Try[ID] = (this, rhs) match {
    case (LeafID(false), r) => Success(r)
    case (l, LeafID(false)) => Success(l)
    case (InnerID(l1, r1), InnerID(l2, r2)) =>
      val a = l1 join l2
      val b = r1 join r2
      for (i <- a; j <- b) yield InnerID(i, j).normalize
    case _ => Failure(new IllegalArgumentException(s"$this join $rhs : overlapping Identities cannot be joined!"))
  }

}

final case class InnerID private[itc] (left: ID, right: ID) extends ID {

  require(left != null && right != null)

  import ID._

  def normalize: ID = (left.normalize, right.normalize) match {
    case (LeafID(true), LeafID(true)) => TRUE
    case (LeafID(false), LeafID(false)) => FALSE
    case (a, b) => if (a == left && b == right) this else InnerID(a, b)
  }

  override def split: (ID, ID) = (left, right) match {
    case (LeafID(false), ir) =>
      val (l, r) = ir.split
      (InnerID(FALSE, l), InnerID(FALSE, r))
    case (il, LeafID(false)) =>
      val (l, r) = il.split
      (InnerID(l, FALSE), InnerID(r, FALSE))
    case _ =>
      (InnerID(left, FALSE), InnerID(FALSE, right))
  }

  override def toString = s"($left,$right)"

}

final case class LeafID private[itc] (value: Boolean) extends ID {

import ID._

  override def normalize: ID = this

  override def split: (ID, ID) = value match {
    case false => FALSE_SPLIT
    case true => TRUE_SPLIT
  }

  override def toString = s"$value"

}

object ID {

  private[itc] val TRUE = LeafID(true)

  private[itc] val FALSE = LeafID(false)

  private[itc] val TRUE_SPLIT = (InnerID(TRUE, FALSE), InnerID(FALSE, TRUE))

  private[itc] val FALSE_SPLIT = (FALSE, FALSE)

  def apply() = TRUE

}
