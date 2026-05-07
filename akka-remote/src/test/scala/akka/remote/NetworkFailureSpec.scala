/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Future

import akka.testkit.AkkaSpec
import akka.testkit.DefaultTimeout

trait NetworkFailureSpec extends DefaultTimeout { self: AkkaSpec =>
  import scala.concurrent.duration.Duration

  import system.dispatcher

  val BytesPerSecond = "60KByte/s"
  val DelayMillis = "350ms"
  val PortRange = "1024-65535"

  def replyWithTcpResetFor(duration: Duration, dead: AtomicBoolean) = {
    Future {
      try {
        enableTcpReset()
        println("===>>> Reply with [TCP RST] for [" + duration + "]")
        Thread.sleep(duration.toMillis)
        restoreIP()
      } catch {
        case e: Throwable =>
          dead.set(true)
          e.printStackTrace
      }
    }
  }

  def throttleNetworkFor(duration: Duration, dead: AtomicBoolean) = {
    Future {
      try {
        enableNetworkThrottling()
        println("===>>> Throttling network with [" + BytesPerSecond + ", " + DelayMillis + "] for [" + duration + "]")
        Thread.sleep(duration.toMillis)
        restoreIP()
      } catch {
        case e: Throwable =>
          dead.set(true)
          e.printStackTrace
      }
    }
  }

  def dropNetworkFor(duration: Duration, dead: AtomicBoolean) = {
    Future {
      try {
        enableNetworkDrop()
        println("===>>> Blocking network [TCP DENY] for [" + duration + "]")
        Thread.sleep(duration.toMillis)
        restoreIP()
      } catch {
        case e: Throwable =>
          dead.set(true)
          e.printStackTrace
      }
    }
  }

  def sleepFor(duration: Duration) = {
    println("===>>> Sleeping for [" + duration + "]")
    Thread.sleep(duration.toMillis)
  }

  def enableNetworkThrottling() = {
    restoreIP()
    assert(new ProcessBuilder("ipfw", "add", "pipe", "1", "ip", "from", "any", "to", "any").start.waitFor == 0)
    assert(new ProcessBuilder("ipfw", "add", "pipe", "2", "ip", "from", "any", "to", "any").start.waitFor == 0)
    assert(
      new ProcessBuilder("ipfw", "pipe", "1", "config", "bw", BytesPerSecond, "delay", DelayMillis).start.waitFor == 0)
    assert(
      new ProcessBuilder("ipfw", "pipe", "2", "config", "bw", BytesPerSecond, "delay", DelayMillis).start.waitFor == 0)
  }

  def enableNetworkDrop() = {
    restoreIP()
    assert(
      new ProcessBuilder("ipfw", "add", "1", "deny", "tcp", "from", "any", "to", "any", PortRange).start.waitFor == 0)
  }

  def enableTcpReset() = {
    restoreIP()
    assert(
      new ProcessBuilder("ipfw", "add", "1", "reset", "tcp", "from", "any", "to", "any", PortRange).start.waitFor == 0)
  }

  def restoreIP() = {
    println("===>>> Restoring network")
    assert(new ProcessBuilder("ipfw", "del", "pipe", "1").start.waitFor == 0)
    assert(new ProcessBuilder("ipfw", "del", "pipe", "2").start.waitFor == 0)
    assert(new ProcessBuilder("ipfw", "flush").start.waitFor == 0)
    assert(new ProcessBuilder("ipfw", "pipe", "flush").start.waitFor == 0)
  }
}
