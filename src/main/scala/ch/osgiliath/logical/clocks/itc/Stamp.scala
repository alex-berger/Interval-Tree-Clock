package ch.osgiliath.logical.clocks.itc

import scala.collection.mutable.ArrayBuffer
import scala.math.{ max => imax }
import scala.util.Try

final case class Stamp(identity: ID, event: Event) {

  import Stamp._

  def this() = this(ID(), Event())

  def <=(other: Stamp): Boolean = event <= other.event

  /**
   * Check if the event enclosed by this [[Stamp]] happened before the one of a given
   * other [[Stamp]]. In other words, check if a given [[Stamp]] causally dominates this one
   * or not.
   *
   * @return `true` if the given [[Stamp]] dominates this one.
   */
  def happenedBefore(other: Stamp) = this <= other && !(other <= this)

  /**
   * Check if the event enclosed by this [[Stamp]] happened after the one of a given
   * other [[Stamp]]. In other words, check if this [[Stamp]] causally dominates a
   * given [[Stamp]] or not.
   *
   * @return `true` if this [[Stamp]] dominates the given one.
   */
  def happenedAfter(other: Stamp) = !(this <= other) && other <= this

  /**
   * Check if the event enclosed by this [[Stamp]] did neither happened before nor after
   * the one of a given other [[Stamp]]. In other words, check that none of the given
   * [[Stamp]]s dominates the other and therefore represent concurrent (non causal) events.
   *
   * @return `true` if the two [[Stamp]]s are concurrent.
   */
  def isConcurrentTo(other: Stamp) = !(this <= other || other <= this)

  /**
   * Check if the event enclosed by this [[Stamp]] and the one of a given other [[Stamp]]
   * are identical. In other words, check if both [[Stamp]]s dominate each other which means
   * that they convey the same event and therefore are consistent in regards to each other.
   *
   * @return `true` if the two [[Stamp]]s are consistent (represent the same event).
   */
  def isConsistentWith(other: Stamp) = this <= other && other <= this

  /**
   * Fork is used to introduce new stamps. Allows the cloning of the causal past of a stamp,
   * resulting in a pair of stamps that have identical copies of the event component and
   * distinct id components. E.g. it can be used to introduce new replicas to a system.
   */
  def fork: (Stamp, Stamp) = {
    val (i1, i2) = identity.split
    return (Stamp(i1, event), Stamp(i2, event))
  }

  /**
   * Peek is a special case of fork that only copies the event component and creates a new
   * stamp with a null id. It can be used to make messages that transport causal information.
   */
  def peek: Stamp = {
    return Stamp(ID.FALSE, event)
  }

  /**
   * Join is used to merge two stamps. Produces a new stamp that incorporates both causal pasts.
   * E.g. it can be used to retire replicas or receive causal information from messages.
   */
  def join(other: Stamp): Try[Stamp] =
    (this.identity join other.identity) map { i => Stamp(i, this.event join other.event) }

  /**
   * Recording an event is used to add causal information to a stamp, "incrementing" the
   * event component and keeping the id.
   */
  def recordEvent: Try[Stamp] = Try {
    require(identity != ID.FALSE, s"$this: cannot record an event for a Stamp without an identity (a stamp obtained by peeking)!")
    val ev = Stamp.fill(identity, event)
    if (ev != event)
      Stamp(identity, ev)
    else {
      val (e, c) = Stamp.grow(identity, event)
      Stamp(identity, e)
    }
  }

}

object Stamp {

  def apply() = new Stamp()

  private def fill(identity: ID, event: Event): Event = (identity, event) match {
    case (LeafID(false), e) => e
    case (LeafID(true), e) => Event(e.max)
    case (i, LeafEvent(n)) => event
    case (InnerID(LeafID(true), i), InnerEvent(n, l, r)) =>
      val x = fill(i, r)
      new InnerEvent(n, Event(imax(l.max, x.min)), x).normalize
    case (InnerID(i, LeafID(true)), InnerEvent(n, l, r)) =>
      val x = fill(i, l)
      new InnerEvent(n, x, Event(imax(r.max, x.min))).normalize
    case (InnerID(il, ir), InnerEvent(n, l, r)) =>
      new InnerEvent(n, fill(il, l), fill(ir, r)).normalize
  }

  private def grow(identity: ID, event: Event): (Event, Int) = (identity, event) match {
    case (LeafID(true), LeafEvent(n)) => (Event(n + 1), 0)
    case (i, LeafEvent(n)) =>
      val (e, c) = grow(i, Event(n, Event.ZERO, Event.ZERO))
      (e, c + event.maxDepth + 1)
    case (InnerID(LeafID(false), i), InnerEvent(n, l, r)) =>
      val (e, c) = grow(i, r)
      (Event(n, l, e), c + 1)
    case (InnerID(i, LeafID(false)), InnerEvent(n, l, r)) =>
      val (e, c) = grow(i, l)
      (Event(n, e, r), c + 1)
    case (InnerID(il, ir), InnerEvent(n, l, r)) =>
      val (el, cl) = grow(il, l)
      val (er, cr) = grow(ir, r)
      if (cl < cr)
        (Event(n, el, r), cl + 1)
      else
        (Event(n, l, er), cr + 1)
  }

}