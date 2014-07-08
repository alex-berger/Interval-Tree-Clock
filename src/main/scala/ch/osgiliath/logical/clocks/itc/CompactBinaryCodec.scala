package ch.osgiliath.logical.clocks.itc

import scala.annotation.tailrec
import scala.collection.mutable.Buffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Compact binary encoding of [[Stamp]]. 
 */
private[itc] object CompactBinaryCodec extends BinaryCodec {

  def encode(stamp: Stamp, buffer: Buffer[Byte]) {
    buffer.append(0)
    encodeIdentity(stamp.identity, buffer)
    encodeEvent(stamp.event, buffer)
  }

  def encodeIdentity(identity: ID, buffer: Buffer[Byte]) {
    identity match {
      case a: LeafID =>
        buffer.append(if (a.value) 1 else 0)
      case b: InnerID =>
        buffer.append(0x80 toByte)
        encodeIdentity(b.left, buffer)
        encodeIdentity(b.right, buffer)
    }
  }

  def encodeEvent(event: Event, buffer: Buffer[Byte]) {
    event match {
      case a: LeafEvent =>
        encodeLeafEvent(a, buffer)
      case b: InnerEvent =>
        encodeInnerEvent(b, buffer)
    }
  }

  private final def encodeLeafEvent(event: LeafEvent, buffer: Buffer[Byte]) {
    val value = event.value
    if (value < 65)
      buffer.append((0x40 | (value & 0x3F)).toByte);
    else
      buffer.append(0x00 toByte,
        ((value >> 24) & 0xFF).toByte,
        ((value >> 16) & 0xFF).toByte,
        ((value >> 8) & 0xFF).toByte,
        (value & 0xFF).toByte)
  }

  private final def encodeInnerEvent(event: InnerEvent, buffer: Buffer[Byte]) {
    val value = event.value
    if (value < 65)
      buffer.append((0xC0 | (value & 0x3F)).toByte);
    else
      buffer.append(0x80 toByte,
        ((value >> 24) & 0xFF).toByte,
        ((value >> 16) & 0xFF).toByte,
        ((value >> 8) & 0xFF).toByte,
        (value & 0xFF).toByte)
    encodeEvent(event.left, buffer)
    encodeEvent(event.right, buffer)
  }

  def decode(input: Seq[Byte]): Try[Option[(Stamp, Seq[Byte])]] = {
    if (input.isEmpty)
      Success(None)
    else {
      val h = input.head
      if ( h != 0 ) 
        Failure(new IllegalArgumentException("Invalid encoding"))
      else {
        for (
          (l, rem) <- decodeIdentity(input.tail);
          (r, rest) <- decodeEvent(rem)
        ) yield Some((Stamp(l, r), rest))
      }
    }
  }

  def decodeIdentity(input: Seq[Byte]): Try[(ID, Seq[Byte])] =
    if (input.isEmpty)
      Failure(new IllegalArgumentException())
    else {
      val h = input.head
      if ((h & 0x80) == 0)
        Success(LeafID((h & 1) == 1), input.tail)
      else {
        for (
          (l, rem) <- decodeIdentity(input.tail);
          (r, rest) <- decodeIdentity(rem)
        ) yield (InnerID(l, r), rest)
      }
    }

  def decodeEvent(input: Seq[Byte]): Try[(Event, Seq[Byte])] =
    if (input.isEmpty)
      Failure(new IllegalArgumentException())
    else if ((input.head & 0x80) == 0) decodeLeafEvent(input) else decodeInnerEvent(input)

  private def decodeLeafEvent(input: Seq[Byte]): Try[(Event, Seq[Byte])] = {
    val h = input.head
    if ((h & 0x40) == 0x40)
      Success((Event(h & 0x3F), input.tail))
    else {
      val Seq(a, b, c, d, x @ _*) = input
      Success((Event(
        (a << 24) |
          (b << 16) |
          (c << 8) |
          d), x))
    }
  }

  private def decodeInnerEvent(input: Seq[Byte]): Try[(Event, Seq[Byte])] = {
    val h = input.head
    var v = 0
    var i = input
    if ((h & 0x40) == 0x40) {
      v = (h & 0x3F)
      i = input.tail
    } else {
      val Seq(a, b, c, d, x @ _*) = input
      v = (
        (a << 24) |
        (b << 16) |
        (c << 8) |
        d)
      i = x
    }
    for (
      (l, rem) <- decodeEvent(i);
      (r, rest) <- decodeEvent(rem)
    ) yield (Event(v, l, r), rest)
  }

}