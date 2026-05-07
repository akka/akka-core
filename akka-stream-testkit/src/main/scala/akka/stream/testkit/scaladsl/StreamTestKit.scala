/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit.scaladsl

import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._

import akka.actor.{ ActorRef, ActorSystem }
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.impl.{ PhasedFusingActorMaterializer, StreamSupervisor }
import akka.stream.snapshot._
import akka.testkit.TestProbe

object StreamTestKit {

  /**
   * Asserts that after the given code block is ran, no stages are left over
   * that were created by the given materializer.
   *
   * This assertion is useful to check that all of the stages have
   * terminated successfully.
   */
  def assertAllStagesStopped[T](block: => T)(implicit materializer: Materializer): T =
    materializer match {
      case impl: PhasedFusingActorMaterializer =>
        stopAllChildren(impl.system, impl.supervisor)
        val result = block
        assertNoChildren(impl.system, impl.supervisor)
        result
      case _ => block
    }

  /** INTERNAL API */
  @InternalApi private[testkit] def stopAllChildren(sys: ActorSystem, supervisor: ActorRef): Unit = {
    val probe = TestProbe()(sys)
    probe.send(supervisor, StreamSupervisor.StopChildren)
    probe.expectMsg(StreamSupervisor.StoppedChildren)
  }

  /** INTERNAL API */
  @InternalApi private[testkit] def assertNoChildren(sys: ActorSystem, supervisor: ActorRef): Unit = {
    val probe = TestProbe()(sys)
    val c = sys.settings.config.getConfig("akka.stream.testkit")
    val timeout = c.getDuration("all-stages-stopped-timeout", MILLISECONDS).millis
    probe.within(timeout) {
      try probe.awaitAssert {
        supervisor.tell(StreamSupervisor.GetChildren, probe.ref)
        val children = probe.expectMsgType[StreamSupervisor.Children].children
        assert(children.isEmpty, s"expected no StreamSupervisor children, but got [${children.mkString(", ")}]")
      } catch {
        case ex: Throwable =>
          import sys.dispatcher
          printDebugDump(supervisor)
          throw ex
      }
    }
  }

  /** INTERNAL API */
  @InternalApi private[akka] def printDebugDump(streamSupervisor: ActorRef)(implicit ec: ExecutionContext): Unit = {
    val doneDumping = MaterializerState
      .requestFromSupervisor(streamSupervisor)
      .map(snapshots => snapshots.foreach(s => println(snapshotString(s.asInstanceOf[StreamSnapshotImpl]))))
    Await.result(doneDumping, 5.seconds)
  }

  /** INTERNAL API */
  @InternalApi private[testkit] def snapshotString(snapshot: StreamSnapshotImpl): String = {
    val builder = new StringBuilder()
    builder.append(s"activeShells (actor: ${snapshot.self}):\n")
    snapshot.activeInterpreters.foreach { shell =>
      builder.append("  ")
      appendShellSnapshot(builder, shell)
      builder.append("\n")
      appendInterpreterSnapshot(builder, shell.asInstanceOf[RunningInterpreterImpl])
      builder.append("\n")
    }
    builder.append(s"newShells:\n")
    snapshot.newShells.foreach { shell =>
      builder.append("  ")
      appendShellSnapshot(builder, shell)
      builder.append("\n")
      builder.append("    Not initialized")
      builder.append("\n")
    }
    builder.toString
  }

  private def appendShellSnapshot(builder: StringBuilder, shell: InterpreterSnapshot): Unit = {
    builder.append("GraphInterpreterShell(\n  logics: [\n")
    val logicsToPrint = shell.logics
    logicsToPrint.foreach { logic =>
      builder
        .append("    ")
        .append(logic.label)
        .append(" attrs: [")
        .append(logic.attributes.attributeList.mkString(", "))
        .append("],\n")
    }
    builder.setLength(builder.length - 2)
    shell match {
      case running: RunningInterpreter =>
        builder.append("\n  ],\n  connections: [\n")
        running.connections.foreach { connection =>
          builder
            .append("    ")
            .append("Connection(")
            .append(connection.asInstanceOf[ConnectionSnapshotImpl].id)
            .append(", ")
            .append(connection.in.label)
            .append(", ")
            .append(connection.out.label)
            .append(", ")
            .append(connection.state)
            .append(")\n")
        }
        builder.setLength(builder.length - 2)

      case _ =>
    }
    builder.append("\n  ]\n)")
    builder.toString()
  }

  private def appendInterpreterSnapshot(builder: StringBuilder, snapshot: RunningInterpreterImpl): Unit = {
    try {
      builder.append("\ndot format graph for deadlock analysis:\n")
      builder.append("================================================================\n")
      builder.append("digraph waits {\n")

      for (i <- snapshot.logics.indices) {
        val logic = snapshot.logics(i)
        builder.append(s"""  N$i [label="${logic.label}"];""").append('\n')
      }

      for (connection <- snapshot.connections) {
        val inName = "N" + connection.in.asInstanceOf[LogicSnapshotImpl].index
        val outName = "N" + connection.out.asInstanceOf[LogicSnapshotImpl].index

        builder.append(s"  $inName -> $outName ")
        connection.state match {
          case ConnectionSnapshot.ShouldPull =>
            builder.append("[label=shouldPull, color=blue];")
          case ConnectionSnapshot.ShouldPush =>
            builder.append(s"[label=shouldPush, color=red, dir=back];")
          case ConnectionSnapshot.Closed =>
            builder.append("[style=dotted, label=closed, dir=both];")
          case null =>
        }
        builder.append("\n")
      }

      builder.append("}\n================================================================\n")
      builder.append(
        s"// ${snapshot.queueStatus} (running=${snapshot.runningLogicsCount}, shutdown=${snapshot.stoppedLogics.mkString(",")})")
      builder.toString()
    } catch {
      case _: NoSuchElementException => builder.append("Not all logics has a stage listed, cannot create graph")
    }
  }

}
