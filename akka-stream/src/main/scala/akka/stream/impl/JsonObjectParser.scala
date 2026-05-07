/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.annotation.switch

import akka.annotation.InternalApi
import akka.stream.scaladsl.Framing.FramingException
import akka.util.ByteString

/**
 * INTERNAL API: Use [[akka.stream.scaladsl.JsonFraming]] instead.
 */
@InternalApi private[akka] object JsonObjectParser {

  final val SquareBraceStart = '['.toByte
  final val SquareBraceEnd = ']'.toByte
  final val CurlyBraceStart = '{'.toByte
  final val CurlyBraceEnd = '}'.toByte
  final val DoubleQuote = '"'.toByte
  final val Backslash = '\\'.toByte
  final val Comma = ','.toByte

  final val LineBreak = 10 // '\n'
  final val LineBreak2 = 13 // '\r'
  final val Tab = 9 // '\t'
  final val Space = 32 // ' '

  def isWhitespace(b: Byte): Boolean = (b: @switch) match {
    case Space      => true
    case LineBreak  => true
    case LineBreak2 => true
    case Tab        => true
    case _          => false
  }

}

/**
 * INTERNAL API: Use [[akka.stream.scaladsl.JsonFraming]] instead.
 *
 * **Mutable** framing implementation that given any number of [[ByteString]] chunks, can emit JSON objects contained within them.
 * Typically JSON objects are separated by new-lines or commas, however a top-level JSON Array can also be understood and chunked up
 * into valid JSON objects by this framing implementation.
 *
 * Leading whitespace between elements will be trimmed.
 */
@InternalApi private[akka] class JsonObjectParser(maximumObjectLength: Int = Int.MaxValue) {
  import JsonObjectParser._

  private[this] var buffer: Array[Byte] = Array.empty

  private[this] var pos = 0 // latest position of pointer while scanning for json object end
  private[this] var start = 0 // number of chars to drop from the front of the bytestring before emitting (skip whitespace etc)
  private[this] var depth = 0 // counter of object-nesting depth, once hits 0 an object should be emitted

  private[this] var completedObject = false
  private[this] var inStringExpression = false
  private[this] var inBackslashEscape = false

  /**
   * Appends input ByteString to internal buffer.
   * Use [[poll]] to extract contained JSON objects.
   */
  def offer(input: ByteString): Unit = {
    val oldBuffer = buffer
    buffer = new Array[Byte](oldBuffer.length - start + input.size)
    System.arraycopy(oldBuffer, start, buffer, 0, oldBuffer.length - start)
    input.copyToArray(buffer, oldBuffer.length - start)
    pos -= start
    start = 0
  }

  def isEmpty: Boolean = buffer.isEmpty

  /** `true` if the buffer is in a valid state to end framing. */
  def canComplete: Boolean = depth == 0

  /**
   * Attempt to locate next complete JSON object in buffered ByteString and returns `Some(it)` if found.
   * May throw a [[akka.stream.scaladsl.Framing.FramingException]] if the contained JSON is invalid or max object size is exceeded.
   */
  def poll(): Option[ByteString] =
    try {
      val foundObject = seekObject()
      if (!foundObject) None
      else
        (pos: @switch) match {
          case -1 | 0 => None
          case _ =>
            if (start == pos) None
            else {
              val res = ByteString.fromArrayUnsafe(buffer, start, pos - start)
              start = pos
              Some(res)
            }
        }
    } catch {
      case _: ArithmeticException =>
        throw new FramingException(s"Invalid JSON encountered at position [$pos] of [${ByteString(buffer).utf8String}]")
    }

  /** @return true if an entire valid JSON object was found, false otherwise */
  private def seekObject(): Boolean = {
    completedObject = false
    val bufSize = buffer.length

    skipToNextObject(bufSize)

    while (pos < bufSize && (pos - start) < maximumObjectLength && !completedObject) {
      val input = buffer(pos)
      proceed(input)
      pos += 1
    }

    if ((pos - start) >= maximumObjectLength)
      throw new FramingException(s"""JSON element exceeded maximumObjectLength ($maximumObjectLength bytes)!""")

    completedObject
  }

  private def skipToNextObject(bufSize: Int): Unit =
    while (pos != -1 && pos < bufSize && (pos - start) < maximumObjectLength && depth == 0) {
      val outer = outerChars(buffer(pos) & 0xff)
      start += outer & 1
      depth = (outer & 2) >> 1

      1 / outer // HACK: use arithmetic exception to cover error case
      pos += 1
    }

  private def proceed(input: Byte): Unit =
    if (inStringExpression) {
      input match {
        case DoubleQuote =>
          inStringExpression = inBackslashEscape
          inBackslashEscape = false
        case Backslash =>
          inBackslashEscape = !inBackslashEscape
        case _ =>
          inBackslashEscape = false
      }
    } else
      input match {
        case DoubleQuote =>
          inStringExpression = true
        case CurlyBraceStart =>
          depth += 1
        case CurlyBraceEnd =>
          depth -= 1
          completedObject = depth == 0
        case _ =>
      }

  // lookup table to avoid branches while scanning over characters outside of objects
  private val outerChars: Array[Byte] =
    // 0x1: skip
    // 0x2: depth + 1
    Array.tabulate[Byte](256) { i =>
      i.toByte match {
        case CurlyBraceStart                           => 2 // found object
        case SquareBraceStart | SquareBraceEnd | Comma => 1 // skip
        case b if isWhitespace(b.toByte)               => 1 // skip
        case _                                         => 0 // error
      }
    }
}
