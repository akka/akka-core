/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi

/** INTERNAL API */
@InternalApi
private[sharding] object ShardingQueries {

  /**
   * INTERNAL API
   * The result of a group query and metadata.
   *
   * @param failed the queries to shards that failed or did not reply within the configured
   *               `timeout`. This could be indicative of several states, for example still
   *               in initialization, restart, heavily loaded and busy, where returning
   *               zero entities is not indicative of the reason
   * @param responses the responses received from the query
   * @param total the total number of shards tracked versus a possible subset
   * @param timeout the timeout used to query the shards per region, for reporting metadata
   * @tparam B
   */
  final case class ShardsQueryResult[B](
      failed: Set[ShardRegion.ShardId],
      responses: Seq[B],
      total: Int,
      timeout: FiniteDuration) {

    /** The number of shards queried, which could equal the `total` or,
     * be a subset if this was a retry of those that failed.
     */
    val queried: Int = failed.size + responses.size

    override val toString: String = {
      if (total == 0)
        s"Shard region had zero shards to gather metadata from."
      else {
        val shardsOf = if (queried < total) s"shards of [$total]:" else "shards:"
        s"Queried [$queried] $shardsOf [${responses.size}] responsive, [${failed.size}] failed after $timeout."
      }
    }
  }
  object ShardsQueryResult {

    /**
     * @param ps the partitioned results of actors queried that did not reply by
     *           the timeout or returned another failure and those that did
     * @param total the total number of actors tracked versus a possible subset
     * @tparam B
     */
    def apply[B](ps: Seq[Either[ShardRegion.ShardId, B]], total: Int, timeout: FiniteDuration): ShardsQueryResult[B] = {
      val (t, r) = partition(ps)(identity)
      ShardsQueryResult(failed = t.toSet, responses = r, total, timeout)
    }

    def partition[T, A, B](ps: Seq[T])(f: T => Either[A, B]): (Seq[A], Seq[B]) = {
      val (a, b) = ps.foldLeft((Nil: Seq[A], Nil: Seq[B]))((xs, y) => prepend(xs, f(y)))
      (a.reverse, b.reverse)
    }

    def prepend[A, B](acc: (Seq[A], Seq[B]), next: Either[A, B]): (Seq[A], Seq[B]) =
      next match {
        case Left(l)  => (l +: acc._1, acc._2)
        case Right(r) => (acc._1, r +: acc._2)
      }
  }
}
