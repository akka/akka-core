/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._
import sbt.Keys._

import scala.util.control.NonFatal

/**
 * Generate the "index" pages of stream operators.
 */
object StreamOperatorsIndexGenerator extends AutoPlugin {

  override val projectSettings: Seq[Setting[_]] = inConfig(Compile)(
    Seq(
      resourceGenerators +=
        generateAlphabeticalIndex(sourceDirectory, _ / "paradox" / "stream" / "operators" / "index.md")))

  val categories = Seq(
    "Source operators",
    "Sink operators",
    "Additional Sink and Source converters",
    "File IO Sinks and Sources",
    "Simple operators",
    "Flow operators composed of Sinks and Sources",
    "Asynchronous operators",
    "Timer driven operators",
    "Backpressure aware operators",
    "Nesting and flattening operators",
    "Time aware operators",
    "Fan-in operators",
    "Fan-out operators",
    "Watching status operators",
    "Actor interop operators",
    "Compression operators",
    "Error handling")

  def categoryId(name: String): String = name.toLowerCase.replace(' ', '-')

  val pendingSourceOrFlow = Seq(
    "to",
    "toMat",
    "via",
    "viaMat",
    "async",
    "upcast",
    "shape",
    "run",
    "runWith",
    "traversalBuilder",
    "runFold",
    "runFoldAsync",
    "runForeach",
    "runReduce",
    "named",
    "throttleEven",
    "actorPublisher",
    "addAttributes",
    "mapMaterializedValue",
    // *Graph:
    "concatGraph",
    "prependGraph",
    "mergeSortedGraph",
    "fromGraph",
    "interleaveGraph",
    "zipGraph",
    "mergeGraph",
    "wireTapGraph",
    "alsoToGraph",
    "orElseGraph",
    "divertToGraph",
    "zipWithGraph")

  // FIXME document these methods as well
  val pendingTestCases = Map(
    "Source" -> (pendingSourceOrFlow),
    "Flow" -> (pendingSourceOrFlow ++ Seq(
      "lazyInit",
      "fromProcessorMat",
      "toProcessor",
      "fromProcessor",
      "of",
      "join",
      "joinMat",
      "fromFunction")),
    "Sink" -> Seq(
      "lazyInit",
      "contramap",
      "named",
      "addAttributes",
      "async",
      "mapMaterializedValue",
      "runWith",
      "shape",
      "traversalBuilder",
      "fromGraph",
      "actorSubscriber",
      "foldAsync",
      "newOnCompleteStage"))

  val ignore =
    Set("equals", "hashCode", "notify", "notifyAll", "wait", "toString", "getClass") ++
    Set("productArity", "canEqual", "productPrefix", "copy", "productIterator", "productElement") ++
    Set(
      "create",
      "apply",
      "ops",
      "appendJava",
      "andThen",
      "andThenMat",
      "isIdentity",
      "withAttributes",
      "transformMaterializing") ++
    Set("asScala", "asJava", "deprecatedAndThen", "deprecatedAndThenMat") ++
    Set("++", "onPush", "onPull", "actorRefWithAck")

  def isPending(element: String, opName: String) =
    pendingTestCases.get(element).exists(_.contains(opName))

  val noElement = " "

  def generateAlphabeticalIndex(dir: SettingKey[File], locate: File => File) = Def.task[Seq[File]] {
    val file = locate(dir.value)

    val defs =
      List(
        "akka-stream/src/main/scala/akka/stream/scaladsl/Source.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/Source.scala",
//        "akka-stream/src/main/scala/akka/stream/scaladsl/SubSource.scala",
//        "akka-stream/src/main/scala/akka/stream/javadsl/SubSource.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/Flow.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/Flow.scala",
//        "akka-stream/src/main/scala/akka/stream/scaladsl/SubFlow.scala",
//        "akka-stream/src/main/scala/akka/stream/javadsl/SubFlow.scala",
//        "akka-stream/src/main/scala/akka/stream/scaladsl/RunnableFlow.scala",
//        "akka-stream/src/main/scala/akka/stream/javadsl/RunnableFlow.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/Sink.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/Sink.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/StreamConverters.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/StreamConverters.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/FileIO.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/FileIO.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/RestartSource.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/RestartSource.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/RestartFlow.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/RestartFlow.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/RestartSink.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/RestartSink.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/RetryFlow.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/RetryFlow.scala",
        "akka-stream/src/main/scala/akka/stream/scaladsl/Compression.scala",
        "akka-stream/src/main/scala/akka/stream/javadsl/Compression.scala",
        // akka-stream-typed
        "akka-stream-typed/src/main/scala/akka/stream/typed/javadsl/ActorSource.scala",
        "akka-stream-typed/src/main/scala/akka/stream/typed/scaladsl/ActorSource.scala",
        "akka-stream-typed/src/main/scala/akka/stream/typed/javadsl/ActorFlow.scala",
        "akka-stream-typed/src/main/scala/akka/stream/typed/scaladsl/ActorFlow.scala",
        "akka-stream-typed/src/main/scala/akka/stream/typed/scaladsl/ActorSink.scala",
        "akka-stream-typed/src/main/scala/akka/stream/typed/javadsl/ActorSink.scala",
        "akka-stream-typed/src/main/scala/akka/stream/typed/scaladsl/PubSub.scala",
        "akka-stream-typed/src/main/scala/akka/stream/typed/javadsl/PubSub.scala").flatMap { f =>
        val slashesNr = f.count(_ == '/')
        val element = f.split("/")(slashesNr).split("\\.")(0)
        IO.read(new File(f))
          .split("\n")
          .map(_.trim)
          .filter(_.startsWith("def "))
          .map(_.drop(4).takeWhile(c => c != '[' && c != '(' && c != ':'))
          .filter(op => !isPending(element, op))
          .filter(op => !ignore.contains(op))
          .map(_.replaceAll("Mat$", ""))
          .map(method => (element, method))
      } ++ List(
        (noElement, "Partition"),
        (noElement, "MergeSequence"),
        (noElement, "Broadcast"),
        (noElement, "Balance"),
        (noElement, "Unzip"),
        (noElement, "UnzipWith"))

    val sourceAndFlow =
      defs.collect { case ("Source", method) => method }.intersect(defs.collect { case ("Flow", method) => method })

    val groupedDefs =
      defs.map {
        case (element @ ("Source" | "Flow"), method) if sourceAndFlow.contains(method) =>
          ("Source/Flow", method, s"Source-or-Flow/$method.md")
        case (`noElement`, method) =>
          (noElement, method, s"$method.md")
        case (element, method) =>
          (element, method, s"$element/$method.md")
      }.distinct

    val tablePerCategory = groupedDefs
      .map {
        case (element, method, md) =>
          val (description, category) = getDetails(file.getParentFile / md)
          category -> (element, method, md, description)
      }
      .groupBy(_._1)
      .mapValues(
        lines =>
          "| |Operator|Description|\n" ++ // TODO mini images here too
          "|--|--|--|\n" ++
          lines
            .map(_._2)
            .sortBy(_._2)
            .map {
              case (element, method, md, description) =>
                s"""|$element|<a name="${method.toLowerCase}"></a>@ref[${methodToShow(method)}]($md)|$description|"""
            }
            .mkString("\n"))

    val tables = categories
      .map { category =>
        s"## $category\n\n" ++
        IO.read(dir.value / "categories" / (categoryId(category) + ".md")) ++ "\n\n" ++
        tablePerCategory(category)
      }
      .mkString("\n\n")

    val content =
      "<!-- DO NOT EDIT DIRECTLY: This file is generated by `project/StreamOperatorsIndexGenerator`. See CONTRIBUTING.md for details. -->\n" +
      "# Operators\n\n" +
      tables +
      "\n\n@@@ index\n\n" +
      groupedDefs
        .sortBy { case (_, method, _) => method.toLowerCase }
        .map { case (_, method, md) => s"* [$method]($md)" }
        .mkString("\n") + "\n\n@@@\n"

    if (!file.exists || IO.read(file) != content) IO.write(file, content)
    Seq(file)
  }

  def methodToShow(method: String): String = method match {
    case "from" => "@scala[apply]@java[from]"
    case other  => other
  }

  def getDetails(file: File): (String, String) =
    try {
      val contents = IO.read(file)
      val lines = contents.split("\\r?\\n")
      require(
        lines.size >= 5,
        s"There must be at least 5 lines in $file, including the title, description, category link and an empty line between each two of them")
      // This forces the short description to be on a single line. We could make this smarter,
      // but 'forcing' the short description to be really short seems nice as well.
      val separator = java.io.File.separatorChar
      val path =
        if (separator != '/')
          file.getAbsolutePath.replace(separator, '/')
        else
          file.getAbsolutePath
      val description =
        lines(2).replaceAll("ref:?\\[(.*?)\\]\\(", "ref[$1](" + path.replaceFirst(".*/([^/]+/).*", "$1"))
      require(description.nonEmpty, s"description in $file must be non-empty, single-line description at the 3rd line")
      val categoryLink = lines(4)
      require(
        categoryLink.startsWith("@ref"),
        s"""category link in $file should start with @ref, but saw \"$categoryLink\"""")
      val categoryName = categoryLink.drop(5).takeWhile(_ != ']')
      val categoryLinkId = categoryLink.dropWhile(_ != '#').drop(1).takeWhile(_ != ')')
      require(categories.contains(categoryName), s"category $categoryName in $file should be known")
      require(categoryLinkId == categoryId(categoryName), s"category id $categoryLinkId in $file")
      (description, categoryName)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to extract details from $file", ex)
    }

}
