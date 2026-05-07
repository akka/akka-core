/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import akka.AkkaValidatePullRequest.additionalTasks
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import de.heikoseeberger.sbtheader.{ CommentCreator, HeaderPlugin }
import com.typesafe.sbt.MultiJvmPlugin.MultiJvmKeys._
import sbt.Keys._
import sbt.{ Def, _ }

trait CopyrightHeader extends AutoPlugin {

  override def requires: Plugins = HeaderPlugin

  override def trigger: PluginTrigger = allRequirements

  protected def headerMappingSettings: Seq[Def.Setting[_]] =
    Seq(Compile, Test, MultiJvm).flatMap { config =>
      inConfig(config)(
        Seq(
          headerLicense := Some(HeaderLicense.Custom(headerFor(CurrentYear))),
          headerMappings := headerMappings.value ++ Map(
              HeaderFileType.scala -> cStyleComment,
              HeaderFileType.java -> cStyleComment,
              HeaderFileType("template") -> cStyleComment)))
    }

  override def projectSettings: Seq[Def.Setting[_]] = Def.settings(headerMappingSettings, additional)

  def additional: Seq[Def.Setting[_]] =
    Def.settings(Compile / compile := {
      (Compile / headerCreate).value
      (Compile / compile).value
    }, Test / compile := {
      (Test / headerCreate).value
      (Test / compile).value
    })

  // We hard-code this so PR's created in year X will not suddenly fail in X+1.
  // Of course we should remember to update it early in the year.
  val CurrentYear = "2025"
  val CopyrightPattern = "Copyright \\([Cc]\\) (\\d{4}([-–]\\d{4})?) (Lightbend|Typesafe) Inc. <.*>".r
  val CopyrightHeaderPattern = s"(?s).*${CopyrightPattern}.*".r

  def headerFor(year: String): String =
    s"Copyright (C) $year Lightbend Inc. <https://www.lightbend.com>"

  val cStyleComment = HeaderCommentStyle.cStyleBlockComment.copy(commentCreator = new CommentCreator() {

    def updateLightbendHeader(header: String): String = header match {
      case CopyrightHeaderPattern(years, null, _) =>
        if (years != CurrentYear)
          CopyrightPattern.replaceFirstIn(header, headerFor(years + "-" + CurrentYear))
        else
          CopyrightPattern.replaceFirstIn(header, headerFor(years))
      case CopyrightHeaderPattern(years, endYears, _) =>
        CopyrightPattern.replaceFirstIn(header, headerFor(years.replace(endYears, "-" + CurrentYear)))
      case _ =>
        header
    }

    def parseStartAndEndYear(header: String): Option[(String, Option[String])] = header match {
      case CopyrightHeaderPattern(years, null, _) =>
        Some((years, None))
      case CopyrightHeaderPattern(years, endYears, _) =>
        Some((years, Some(endYears)))
      case _ =>
        None
    }

    override def apply(text: String, existingText: Option[String]): String = {
      val formatted = existingText match {
        case Some(existedText) =>
          parseStartAndEndYear(existedText) match {
            case Some((years, None)) =>
              if (years != CurrentYear) {
                val header = headerFor(years + "-" + CurrentYear)
                HeaderCommentStyle.cStyleBlockComment.commentCreator(header, existingText)
              } else {
                HeaderCommentStyle.cStyleBlockComment.commentCreator(headerFor(CurrentYear), existingText)
              }
            case Some((years, Some(endYears))) =>
              val header = headerFor(years.replace(endYears, "-" + CurrentYear))
              HeaderCommentStyle.cStyleBlockComment.commentCreator(header, existingText)
            case None =>
              existedText
          }
        case None =>
          HeaderCommentStyle.cStyleBlockComment.commentCreator(text, existingText)
      }
      formatted.trim
    }
  })
}

object CopyrightHeader extends CopyrightHeader

object CopyrightHeaderInPr extends CopyrightHeader {

  override val additional =
    Def.settings(additionalTasks += Compile / headerCheck, additionalTasks += Test / headerCheck)
}
