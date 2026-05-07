/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.stream.scaladsl.{ Sink, Source }

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeMultiGroupBy extends RecipeSpec {

  "Recipe for multi-groupBy" must {

    "work" in {

      case class Topic(name: String)

      val elems = Source(List("1: a", "1: b", "all: c", "all: d", "1: e"))
      val extractTopics = { (msg: Message) =>
        if (msg.startsWith("1")) List(Topic("1"))
        else List(Topic("1"), Topic("2"))
      }

      //#multi-groupby
      val topicMapper: (Message) => immutable.Seq[Topic] = extractTopics

      val messageAndTopic: Source[(Message, Topic), NotUsed] = elems.mapConcat { (msg: Message) =>
        val topicsForMessage = topicMapper(msg)
        // Create a (Msg, Topic) pair for each of the topics
        // the message belongs to
        topicsForMessage.map(msg -> _)
      }

      val multiGroups = messageAndTopic.groupBy(2, _._2).map {
        case (msg, topic) =>
          // do what needs to be done
          //#multi-groupby
          (msg, topic)
        //#multi-groupby
      }
      //#multi-groupby

      val result = multiGroups
        .grouped(10)
        .mergeSubstreams
        .map(g => g.head._2.name + g.map(_._1).mkString("[", ", ", "]"))
        .limit(10)
        .runWith(Sink.seq)

      Await.result(result, 3.seconds).toSet should be(Set("1[1: a, 1: b, all: c, all: d, 1: e]", "2[all: c, all: d]"))

    }

  }

}
