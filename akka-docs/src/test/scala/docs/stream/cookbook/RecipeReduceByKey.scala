/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.stream.scaladsl._

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

@nowarn("msg=never used") // sample snippets
class RecipeReduceByKey extends RecipeSpec {

  "Reduce by key recipe" must {

    val MaximumDistinctWords = 1000

    "work with simple word count" in {

      def words = Source(List("hello", "world", "and", "hello", "universe", "akka") ++ List.fill(1000)("rocks!"))

      //#word-count
      val counts: Source[(String, Int), NotUsed] = words
      // split the words into separate streams first
        .groupBy(MaximumDistinctWords, identity)
        //transform each element to pair with number of words in it
        .map(_ -> 1)
        // add counting logic to the streams
        .reduce((l, r) => (l._1, l._2 + r._2))
        // get a stream of word counts
        .mergeSubstreams
      //#word-count

      Await.result(counts.limit(10).runWith(Sink.seq), 3.seconds).toSet should be(
        Set(("hello", 2), ("world", 1), ("and", 1), ("universe", 1), ("akka", 1), ("rocks!", 1000)))
    }

    "work generalized" in {

      def words = Source(List("hello", "world", "and", "hello", "universe", "akka") ++ List.fill(1000)("rocks!"))

      //#reduce-by-key-general
      def reduceByKey[In, K, Out](maximumGroupSize: Int, groupKey: (In) => K, map: (In) => Out)(
          reduce: (Out, Out) => Out): Flow[In, (K, Out), NotUsed] = {

        Flow[In]
          .groupBy[K](maximumGroupSize, groupKey)
          .map(e => groupKey(e) -> map(e))
          .reduce((l, r) => l._1 -> reduce(l._2, r._2))
          .mergeSubstreams
      }

      val wordCounts = words.via(
        reduceByKey(MaximumDistinctWords, groupKey = (word: String) => word, map = (word: String) => 1)(
          (left: Int, right: Int) => left + right))
      //#reduce-by-key-general

      Await.result(wordCounts.limit(10).runWith(Sink.seq), 3.seconds).toSet should be(
        Set(("hello", 2), ("world", 1), ("and", 1), ("universe", 1), ("akka", 1), ("rocks!", 1000)))

    }
  }

}
