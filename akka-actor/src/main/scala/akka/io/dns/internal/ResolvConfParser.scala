/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.io.File
import java.nio.file.Files

import scala.util.Try

import scala.jdk.CollectionConverters._

private[dns] case class ResolvConf(search: List[String], ndots: Int)

private[dns] object ResolvConfParser {

  private val DomainLabel = "domain"
  private val SearchLabel = "search"
  private val OptionsLabel = "options"
  private val NdotsOption = "ndots:"

  /**
   * Does a partial parse according to https://linux.die.net/man/5/resolver.
   */
  def parseFile(file: File): Try[ResolvConf] = {
    Try {
      parseLines(Files.lines(file.toPath).iterator().asScala)
    }
  }

  def parseLines(lines: Iterator[String]): ResolvConf = {
    // A few notes - according to the spec, search and domain are mutually exclusive, the domain is used as the
    // sole search domain if specified. Also, if multiple of either are specified, then last one wins, so as we
    // parse the file, as we encounter either a domain or search element, we replace this list with what we find.
    var search = List.empty[String]
    var ndots = 1

    lines
      .map(_.trim)
      .filter { line =>
        // Ignore blank lines and comments
        line.nonEmpty && line(0) != ';' && line(0) != '#'
      }
      .foreach { line =>
        val (label, args) = line.span(!_.isWhitespace)
        def trimmedArgs = args.trim
        label match {
          case `DomainLabel` =>
            search = List(trimmedArgs)
          case `SearchLabel` =>
            search = trimmedArgs.split("\\s+").toList
          case `OptionsLabel` =>
            args.split("\\s+").foreach { option =>
              // We're only interested in ndots
              if (option.startsWith(NdotsOption)) {
                // Allow exception to fall through to Try
                ndots = option.drop(NdotsOption.length).toInt
              }
            }
          case _ => // Ignore everything else
        }
      }

    ResolvConf(search, ndots)
  }

}
