/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testconductor

import akka.protobufv3.internal.Message
import akka.util.Helpers
import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandler
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.handler.codec.MessageToMessageEncoder

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/**
 * INTERNAL API.
 */
private[akka] class ProtobufEncoder extends MessageToMessageEncoder[Message] {

  override def encode(ctx: ChannelHandlerContext, msg: Message, out: java.util.List[AnyRef]): Unit = {
    msg match {
      case message: Message =>
        val bytes = message.toByteArray
        out.add(ctx.alloc().buffer(bytes.length).writeBytes(bytes))
    }
  }
}

/**
 * INTERNAL API.
 */
private[akka] class ProtobufDecoder(prototype: Message) extends MessageToMessageDecoder[ByteBuf] {

  override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: java.util.List[AnyRef]): Unit = {
    val bytes = ByteBufUtil.getBytes(msg)
    out.add(prototype.getParserForType.parseFrom(bytes))
  }
}

/**
 * INTERNAL API.
 */
@Sharable
private[akka] class TestConductorPipelineFactory(handler: ChannelInboundHandler)
    extends ChannelInitializer[SocketChannel] {

  override def initChannel(ch: SocketChannel): Unit = {
    val pipe = ch.pipeline()
    pipe.addLast("lengthFieldPrepender", new LengthFieldPrepender(4))
    pipe.addLast("lengthFieldDecoder", new LengthFieldBasedFrameDecoder(10000, 0, 4, 0, 4, false))
    pipe.addLast("protoEncoder", new ProtobufEncoder)
    pipe.addLast("protoDecoder", new ProtobufDecoder(TestConductorProtocol.Wrapper.getDefaultInstance))
    pipe.addLast("msgEncoder", new MsgEncoder)
    pipe.addLast("msgDecoder", new MsgDecoder)
    pipe.addLast("userHandler", handler)
  }
}

/**
 * INTERNAL API.
 */
private[akka] sealed trait Role

/**
 * INTERNAL API.
 */
private[akka] case object Client extends Role

/**
 * INTERNAL API.
 */
private[akka] case object Server extends Role

/**
 * INTERNAL API.
 */
private[akka] trait RemoteConnection {

  /**
   * The channel future associated with this connection.
   */
  def channelFuture: ChannelFuture

  /**
   * Shutdown the connection and release the resources.
   */
  def shutdown(): Unit
}

/**
 * INTERNAL API.
 */
private[akka] object RemoteConnection {
  def apply(
      role: Role,
      sockaddr: InetSocketAddress,
      poolSize: Int,
      handler: ChannelInboundHandler): RemoteConnection = {
    role match {
      case Client =>
        val bootstrap = new Bootstrap()
        val eventLoopGroup = new MultiThreadIoEventLoopGroup(poolSize, NioIoHandler.newFactory())
        val cf = bootstrap
          .group(eventLoopGroup)
          .channel(classOf[NioSocketChannel])
          .handler(new TestConductorPipelineFactory(handler))
          .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
          .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
          .connect(sockaddr)
          .sync()

        new RemoteConnection {
          override def channelFuture: ChannelFuture = cf

          override def shutdown(): Unit = {
            try {
              channelFuture.channel().close().sync()
              eventLoopGroup.shutdownGracefully(0L, 0L, TimeUnit.SECONDS)
            } catch {
              case NonFatal(_) => // silence this one to not make tests look like they failed, it's not really critical
            }
          }
        }

      case Server =>
        val bootstrap = new ServerBootstrap()
        val parentEventLoopGroup = new MultiThreadIoEventLoopGroup(poolSize, NioIoHandler.newFactory())
        val childEventLoopGroup = new MultiThreadIoEventLoopGroup(poolSize, NioIoHandler.newFactory())
        val cf = bootstrap
          .group(parentEventLoopGroup, childEventLoopGroup)
          .channel(classOf[NioServerSocketChannel])
          .childHandler(new TestConductorPipelineFactory(handler))
          .option[java.lang.Boolean](ChannelOption.SO_REUSEADDR, !Helpers.isWindows)
          .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 2048)
          .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
          .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
          .bind(sockaddr)

        new RemoteConnection {
          override def channelFuture: ChannelFuture = cf

          override def shutdown(): Unit = {
            try {
              channelFuture.channel().close().sync()
              parentEventLoopGroup.shutdownGracefully(0L, 0L, TimeUnit.SECONDS)
              childEventLoopGroup.shutdownGracefully(0L, 0L, TimeUnit.SECONDS)
            } catch {
              case NonFatal(_) => // silence this one to not make tests look like they failed, it's not really critical
            }
          }
        }
    }
  }

  def getAddrString(channel: Channel): String = channel.remoteAddress() match {
    case i: InetSocketAddress => i.toString
    case _                    => "[unknown]"
  }
}
