/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

/**
 * INTERNAL API: ByteString pretty printer, based on Johanes Rudolph's implementation from:
 * https://github.com/jrudolph/akka/commit/c889dddf37c8635c365a79a391eb18a709f36773#diff-947cbf07996eeb823cb9850cc2e81126R19
 */
private[akka] object PrettyByteString {
  private val indentDepth = 2
  private val indent = " " * (indentDepth + 1)

  implicit class asPretty(bs: ByteString) {
    def prettyPrint(maxBytes: Int = 16 * 5): String = formatBytes(bs, maxBytes).mkString("\n")
  }

  def formatBytes(bs: ByteString, maxBytes: Int = 16 * 5): Iterator[String] = {
    def asHex(b: Byte): String = "%02X".format(b)
    def asASCII(b: Byte): Char =
      if (b >= 0x20 && b < 0x7f) b.toChar
      else '.'

    def formatLine(bs: ByteString): String = {
      val data = bs.toSeq
      val hex = data.map(asHex _).mkString(" ")
      val ascii = data.map(asASCII _).mkString
      f"$indent%s  $hex%-48s | $ascii"
    }
    def formatBytes(bs: ByteString): String =
      bs.grouped(16).map(formatLine).mkString("\n")

    val prefix = s"${indent}ByteString(${bs.size} bytes)"

    if (bs.size <= maxBytes) Iterator(prefix + "\n", formatBytes(bs))
    else
      Iterator(
        s"$prefix first + last $maxBytes:\n",
        formatBytes(bs.take(maxBytes)),
        s"\n$indent                    ... [${bs.size - maxBytes} bytes omitted] ...\n",
        formatBytes(bs.takeRight(maxBytes)))
  }

}
