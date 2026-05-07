/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testconductor

import akka.actor.Address
import akka.remote.testconductor.TestConductorProtocol.BarrierOp
import akka.remote.testconductor.{ TestConductorProtocol => TCP }
import akka.remote.testkit.Direction
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.handler.codec.MessageToMessageEncoder

import scala.concurrent.duration._
import scala.language.implicitConversions

final case class RoleName(name: String)

private[akka] final case class ToClient(msg: ClientOp with NetworkOp)
private[akka] final case class ToServer(msg: ServerOp with NetworkOp)

private[akka] sealed trait ClientOp // messages sent to from Conductor to Player
private[akka] sealed trait ServerOp // messages sent to from Player to Conductor
private[akka] sealed trait CommandOp // messages sent from TestConductorExt to Conductor
private[akka] sealed trait NetworkOp // messages sent over the wire
private[akka] sealed trait UnconfirmedClientOp extends ClientOp // unconfirmed messages going to the Player
private[akka] sealed trait ConfirmedClientOp extends ClientOp

/**
 * First message of connection sets names straight.
 */
private[akka] final case class Hello(name: String, addr: Address) extends NetworkOp

private[akka] final case class EnterBarrier(name: String, timeout: Option[FiniteDuration])
    extends ServerOp
    with NetworkOp
private[akka] final case class FailBarrier(name: String) extends ServerOp with NetworkOp
private[akka] final case class BarrierResult(name: String, success: Boolean) extends UnconfirmedClientOp with NetworkOp

private[akka] final case class Throttle(node: RoleName, target: RoleName, direction: Direction, rateMBit: Float)
    extends CommandOp
private[akka] final case class ThrottleMsg(target: Address, direction: Direction, rateMBit: Float)
    extends ConfirmedClientOp
    with NetworkOp

private[akka] final case class Disconnect(node: RoleName, target: RoleName, abort: Boolean) extends CommandOp
private[akka] final case class DisconnectMsg(target: Address, abort: Boolean) extends ConfirmedClientOp with NetworkOp

private[akka] final case class Terminate(node: RoleName, shutdownOrExit: Either[Boolean, Int]) extends CommandOp
private[akka] final case class TerminateMsg(shutdownOrExit: Either[Boolean, Int])
    extends ConfirmedClientOp
    with NetworkOp

private[akka] final case class GetAddress(node: RoleName) extends ServerOp with NetworkOp
private[akka] final case class AddressReply(node: RoleName, addr: Address) extends UnconfirmedClientOp with NetworkOp

private[akka] abstract class Done extends ServerOp with UnconfirmedClientOp with NetworkOp
private[akka] case object Done extends Done {
  def getInstance: Done = this
}

private[akka] final case class Remove(node: RoleName) extends CommandOp

private[akka] class MsgEncoder extends MessageToMessageEncoder[AnyRef] {

  implicit def address2proto(addr: Address): TCP.Address =
    TCP.Address.newBuilder
      .setProtocol(addr.protocol)
      .setSystem(addr.system)
      .setHost(addr.host.get)
      .setPort(addr.port.get)
      .build

  implicit def direction2proto(dir: Direction): TCP.Direction = dir match {
    case Direction.Send    => TCP.Direction.Send
    case Direction.Receive => TCP.Direction.Receive
    case Direction.Both    => TCP.Direction.Both
  }

  override def encode(ctx: ChannelHandlerContext, msg: AnyRef, out: java.util.List[AnyRef]): Unit = {
    out.add(encode0(msg))
  }

  private def encode0(msg: AnyRef): AnyRef = msg match {
    case x: NetworkOp =>
      val w = TCP.Wrapper.newBuilder
      x match {
        case Hello(name, address) =>
          w.setHello(TCP.Hello.newBuilder.setName(name).setAddress(address))
        case EnterBarrier(name, timeout) =>
          val barrier = TCP.EnterBarrier.newBuilder.setName(name)
          timeout.foreach(t => barrier.setTimeout(t.toNanos))
          barrier.setOp(BarrierOp.Enter)
          w.setBarrier(barrier)
        case BarrierResult(name, success) =>
          val res = if (success) BarrierOp.Succeeded else BarrierOp.Failed
          w.setBarrier(TCP.EnterBarrier.newBuilder.setName(name).setOp(res))
        case FailBarrier(name) =>
          w.setBarrier(TCP.EnterBarrier.newBuilder.setName(name).setOp(BarrierOp.Fail))
        case ThrottleMsg(target, dir, rate) =>
          w.setFailure(
            TCP.InjectFailure.newBuilder
              .setAddress(target)
              .setFailure(TCP.FailType.Throttle)
              .setDirection(dir)
              .setRateMBit(rate))
        case DisconnectMsg(target, abort) =>
          w.setFailure(
            TCP.InjectFailure.newBuilder
              .setAddress(target)
              .setFailure(if (abort) TCP.FailType.Abort else TCP.FailType.Disconnect))
        case TerminateMsg(Right(exitValue)) =>
          w.setFailure(TCP.InjectFailure.newBuilder.setFailure(TCP.FailType.Exit).setExitValue(exitValue))
        case TerminateMsg(Left(false)) =>
          w.setFailure(TCP.InjectFailure.newBuilder.setFailure(TCP.FailType.Shutdown))
        case TerminateMsg(Left(true)) =>
          w.setFailure(TCP.InjectFailure.newBuilder.setFailure(TCP.FailType.ShutdownAbrupt))
        case GetAddress(node) =>
          w.setAddr(TCP.AddressRequest.newBuilder.setNode(node.name))
        case AddressReply(node, address) =>
          w.setAddr(TCP.AddressRequest.newBuilder.setNode(node.name).setAddr(address))
        case _: Done =>
          w.setDone("")
      }
      w.build
    case _ => throw new IllegalArgumentException("wrong message " + msg)
  }
}

private[akka] class MsgDecoder extends MessageToMessageDecoder[AnyRef] {

  implicit def address2scala(addr: TCP.Address): Address =
    Address(addr.getProtocol, addr.getSystem, addr.getHost, addr.getPort)

  implicit def direction2scala(dir: TCP.Direction): Direction = dir match {
    case TCP.Direction.Send    => Direction.Send
    case TCP.Direction.Receive => Direction.Receive
    case TCP.Direction.Both    => Direction.Both
  }

  override def decode(ctx: ChannelHandlerContext, msg: AnyRef, out: java.util.List[AnyRef]): Unit = {
    out.add(decode0(msg))
  }

  private def decode0(msg: AnyRef): AnyRef = msg match {
    case w: TCP.Wrapper if w.getAllFields.size == 1 =>
      if (w.hasHello) {
        val h = w.getHello
        Hello(h.getName, h.getAddress)
      } else if (w.hasBarrier) {
        val barrier = w.getBarrier
        barrier.getOp match {
          case BarrierOp.Succeeded => BarrierResult(barrier.getName, success = true)
          case BarrierOp.Failed    => BarrierResult(barrier.getName, success = false)
          case BarrierOp.Fail      => FailBarrier(barrier.getName)
          case BarrierOp.Enter =>
            EnterBarrier(
              barrier.getName,
              if (barrier.hasTimeout) Option(Duration.fromNanos(barrier.getTimeout)) else None)
        }
      } else if (w.hasFailure) {
        val f = w.getFailure
        import TCP.{ FailType => FT }
        f.getFailure match {
          case FT.Throttle       => ThrottleMsg(f.getAddress, f.getDirection, f.getRateMBit)
          case FT.Abort          => DisconnectMsg(f.getAddress, abort = true)
          case FT.Disconnect     => DisconnectMsg(f.getAddress, abort = false)
          case FT.Exit           => TerminateMsg(Right(f.getExitValue))
          case FT.Shutdown       => TerminateMsg(Left(false))
          case FT.ShutdownAbrupt => TerminateMsg(Left(true))
        }
      } else if (w.hasAddr) {
        val a = w.getAddr
        if (a.hasAddr) AddressReply(RoleName(a.getNode), a.getAddr)
        else GetAddress(RoleName(a.getNode))
      } else if (w.hasDone) {
        Done
      } else {
        throw new IllegalArgumentException("unknown message " + msg)
      }
    case _ => throw new IllegalArgumentException("wrong message " + msg)
  }
}
