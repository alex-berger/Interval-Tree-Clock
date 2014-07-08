package ch.osgiliath.logical.clocks.itc

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * Codec to encode and decode [[Stamp]] into respectively from binary representation.
 */
trait BinaryCodec {

  def encode(stamp: Stamp): Array[Byte] = {
    val bs = ArrayBuffer[Byte]()
    encode(stamp, bs)
    bs.toArray
  }

  def encode(stamp: Stamp, buffer: Buffer[Byte])

  def decode(input: Seq[Byte]): Try[Option[(Stamp, Seq[Byte])]]

}

object BinaryCodec {
  
  def defaultCodec = CompactBinaryCodec
  
  def encode(stamp: Stamp): Array[Byte] = defaultCodec.encode(stamp)

  def encode(stamp: Stamp, buffer: Buffer[Byte]) {
    defaultCodec.encode(stamp, buffer)
  }

  def decode(input: Seq[Byte]): Try[Option[(Stamp, Seq[Byte])]] =
    if (input.isEmpty)
      Success(None)
    else input.head match {
      case 0 => CompactBinaryCodec.decode(input)
      case x => Failure(new IllegalArgumentException(s"Unsupported encoding format $x"));
    }

}