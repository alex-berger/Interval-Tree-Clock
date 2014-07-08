package ch.osgiliath.logical.clocks.itc

import scala.collection.mutable.ArrayBuffer
import scala.math.{max => imax}
import scala.math.{min => imin}

sealed abstract class Event {

import Event._

  def value : Int
  
  def join(other: Event): Event = (this, other) match {
    case (LeafEvent(a), LeafEvent(b)) => Event(imax(a, b))
    case (LeafEvent(a), InnerEvent(b, l, r)) => Event(a, ZERO, ZERO).join(other)
    case (InnerEvent(a, l, r), LeafEvent(b)) => this.join(Event(b, ZERO, ZERO))
    case (InnerEvent(a, l1, r1), InnerEvent(b, l2, r2)) =>
      if (a > b)
        other.join(this)
      else
        Event(a, (l1.join(l2.lift(b - a))), (r1.join(r2.lift(b - a)))).normalize
  }

  def <=(rhs: Event): Boolean = (this, rhs) match {
    case (LeafEvent(a), LeafEvent(b)) =>
      a <= b
    case (LeafEvent(a), InnerEvent(b, _, _)) =>
      a <= b
    case (InnerEvent(a, l1, r1), LeafEvent(b)) =>
      a <= b && (l1.lift(a) <= rhs) && (r1.lift(a) <= rhs)
    case (InnerEvent(a, l1, r1), InnerEvent(b, l2, r2)) =>
      a <= b && (l1.lift(a) <= l2.lift(b)) && (r1.lift(a) <= r2.lift(b))
  }

  private[itc] def lift(m: Int): Event

  private[itc] def sink(m: Int): Event

  private[itc] def min: Int

  private[itc] def max: Int

  private[itc] def normalize: Event

  def maxDepth : Int = maxDepth(0)
  
  def maxDepth(d: Int) : Int
   
}

final case class LeafEvent private[itc](value: Int) extends Event {

  private[itc] override def lift(m: Int): Event = Event(value + m)

  private[itc] override def sink(m: Int): Event = Event(value - m)

  private[itc] def min: Int = value

  private[itc] def max: Int = value

  private[itc] def normalize: Event = this

  override def toString = s"$value"
  
  def maxDepth(d: Int) : Int = d

}

final case class InnerEvent private[itc](value: Int, left: Event, right: Event) extends Event {

  require(left != null && right != null)

  private[itc] override def lift(m: Int): Event = Event(value + m, left, right)

  private[itc] override def sink(m: Int): Event = Event(value - m, left, right)

  private[itc] def min: Int = value + imin(left.min, right.min)

  private[itc] def max: Int = value + imax(left.max, right.max)

  private[itc] def normalize: Event = (left, right) match {
    case (LeafEvent(x), LeafEvent(y)) if (x == y) => Event(value + x)
    case _ =>
      val m = imin(left.min, right.min);
      Event(value + m, left.sink(m), right.sink(m))
  }

  override def toString = s"($value,$left,$right)"

  def maxDepth(d: Int) : Int = imax(left.maxDepth(d), right.maxDepth(d))
 
}

private[itc] object Event {

  private[itc] val ZERO: Event = Event(0)

  def apply() = ZERO

  def apply(value: Int): Event = new LeafEvent(value)

  def apply(value: Int, left: Event, right: Event): Event = new InnerEvent(value, left, right)
  
}
