/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import akka.annotation.InternalApi

object Version {
  val Zero: Version = Version("0.0.0")

  private val Undefined = 0

  def apply(version: String): Version = {
    val v = new Version(version)
    v.parse()
  }
}

/**
 * Comparable version information.
 *
 * The typical convention is to use 3 digit version numbers `major.minor.patch`,
 * but 1 or two digits are also supported.
 *
 * If no `.` is used it is interpreted as a single digit version number or as
 * plain alphanumeric if it couldn't be parsed as a number.
 *
 * It may also have a qualifier at the end for 2 or 3 digit version numbers such as "1.2-RC1".
 * For 1 digit with qualifier, 1-RC1, it is interpreted as plain alphanumeric.
 *
 * It has support for https://github.com/dwijnand/sbt-dynver format with `+` or
 * `-` separator. The number of commits from the tag is handled as a numeric part.
 * For example `1.0.0+3-73475dce26` is less than `1.0.0+10-ed316bd024` (3 < 10).
 */
final class Version(val version: String) extends Comparable[Version] {
  import Version.Undefined

  // lazy initialized via `parse` method
  @volatile private var numbers: Array[Int] = Array.emptyIntArray
  private var rest: String = ""

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def parse(): Version = {
    def parseLastPart(s: String): (Int, String) = {
      // for example 2, 2-SNAPSHOT or dynver 2+10-1234abcd
      if (s.length == 0) {
        Undefined -> s
      } else {
        val i = s.indexOf('-')
        val j = s.indexOf('+') // for dynver
        val k =
          if (i == -1) j
          else if (j == -1) i
          else math.min(i, j)
        if (k == -1)
          s.toInt -> ""
        else
          s.substring(0, k).toInt -> s.substring(k + 1)
      }
    }

    def parseDynverPart(s: String): (Int, String) = {
      // for example SNAPSHOT or dynver 10-1234abcd
      if (s.isEmpty || !s.charAt(0).isDigit) {
        Undefined -> s
      } else {
        s.indexOf('-') match {
          case -1 =>
            Undefined -> s
          case i =>
            try {
              s.substring(0, i).toInt -> s.substring(i + 1)
            } catch {
              case _: NumberFormatException =>
                Undefined -> s
            }
        }
      }
    }

    def parseLastParts(s: String): (Int, Int, String) = {
      // for example 2, 2-SNAPSHOT or dynver 2+10-1234abcd
      val (lastNumber, rest) = parseLastPart(s)
      if (rest == "")
        (lastNumber, Undefined, rest)
      else {
        val (dynverNumber, rest2) = parseDynverPart(rest)
        (lastNumber, dynverNumber, rest2)
      }
    }

    if (numbers.length == 0) {

      val nbrs = new Array[Int](4)
      val segments = version.split('.')

      val rst =
        if (segments.length == 1) {
          // single digit or alphanumeric
          val s = segments(0)
          if (s.isEmpty)
            throw new IllegalArgumentException("Empty version not supported.")
          nbrs(1) = Undefined
          nbrs(2) = Undefined
          nbrs(3) = Undefined
          if (s.charAt(0).isDigit) {
            try {
              nbrs(0) = s.toInt
              ""
            } catch {
              case _: NumberFormatException =>
                s
            }
          } else {
            s
          }
        } else if (segments.length == 2) {
          // for example 1.2, 1.2-SNAPSHOT or dynver 1.2+10-1234abcd
          val (n1, n2, rest) = parseLastParts(segments(1))
          nbrs(0) = segments(0).toInt
          nbrs(1) = n1
          nbrs(2) = n2
          nbrs(3) = Undefined
          rest
        } else if (segments.length == 3) {
          // for example 1.2.3, 1.2.3-SNAPSHOT or dynver 1.2.3+10-1234abcd
          val (n1, n2, rest) = parseLastParts(segments(2))
          nbrs(0) = segments(0).toInt
          nbrs(1) = segments(1).toInt
          nbrs(2) = n1
          nbrs(3) = n2
          rest
        } else {
          throw new IllegalArgumentException(s"Only 3 digits separated with '.' are supported. [$version]")
        }

      this.rest = rst
      this.numbers = nbrs
    }
    this
  }

  override def compareTo(other: Version): Int = {
    if (version == other.version) // String equals without requiring parse
      0
    else {
      parse()
      other.parse()
      var diff = 0
      diff = numbers(0) - other.numbers(0)
      if (diff == 0) {
        diff = numbers(1) - other.numbers(1)
        if (diff == 0) {
          diff = numbers(2) - other.numbers(2)
          if (diff == 0) {
            diff = numbers(3) - other.numbers(3)
            if (diff == 0) {
              if (rest == "" && other.rest != "")
                diff = 1
              else if (other.rest == "" && rest != "")
                diff = -1
              else
                diff = rest.compareTo(other.rest)
            }
          }
        }
      }
      diff
    }
  }

  override def equals(o: Any): Boolean = o match {
    case v: Version => compareTo(v) == 0
    case _          => false
  }

  override def hashCode(): Int = {
    parse()
    var result = HashCode.SEED
    result = HashCode.hash(result, numbers(0))
    result = HashCode.hash(result, numbers(1))
    result = HashCode.hash(result, numbers(2))
    result = HashCode.hash(result, numbers(3))
    result = HashCode.hash(result, rest)
    result
  }

  override def toString: String = version
}
