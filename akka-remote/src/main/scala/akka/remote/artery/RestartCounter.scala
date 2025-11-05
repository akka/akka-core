/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration

/**
 * INTERNAL API
 */
private[remote] object RestartCounter {
  final case class State(count: Int, deadline: Deadline)
}

/**
 * INTERNAL API: Thread safe "restarts with duration" counter
 */
private[remote] class RestartCounter(maxRestarts: Int, restartTimeout: FiniteDuration) {
  import RestartCounter._

  private val state = new AtomicReference[State](State(0, Deadline.now + restartTimeout))

  /**
   * Current number of restarts.
   */
  def count(): Int = state.get.count

  /**
   * Increment the restart counter, or reset the counter to 1 if the
   * `restartTimeout` has elapsed. The latter also resets the timeout.
   * @return `true` if number of restarts, including this one, is less
   *         than or equal to `maxRestarts`
   */
  @tailrec final def restart(): Boolean = {
    val s = state.get

    val newState =
      if (s.deadline.hasTimeLeft())
        s.copy(count = s.count + 1)
      else
        State(1, Deadline.now + restartTimeout)

    if (state.compareAndSet(s, newState))
      newState.count <= maxRestarts
    else
      restart() // recur
  }

}
