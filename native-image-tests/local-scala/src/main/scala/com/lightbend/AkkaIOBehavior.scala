/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */
package com.lightbend

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Timers
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.io.IO
import akka.io.Udp
import akka.stream.Client
import akka.stream.Server
import akka.stream.TLSClosing
import akka.stream.TLSRole
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.TLS
import akka.stream.scaladsl.Tcp
import akka.util.ByteString
import com.lightbend.Main.getClass

import java.io.InputStream
import java.net.InetSocketAddress
import java.security.KeyStore
import java.security.SecureRandom
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManagerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

object AkkaIOBehavior {

  def apply(whenDone: ActorRef[String]): Behavior[String] = Behaviors.setup { context =>
    implicit val system: ActorSystem[_] = context.system
    implicit val ec: ExecutionContext = context.executionContext

    val tcpHost = "127.0.0.1"
    val tcpPort = 1337
    val tcp = Tcp(context.system)

    // TCP echo server
    val tcpServerBound = tcp
      .bind(tcpHost, tcpPort)
      .toMat(Sink.foreach(incomingConnection => incomingConnection.handleWith(Flow[ByteString])))(Keep.left)
      .run()
      .andThen {
        case Success(_) =>
        case Failure(error) =>
          println("Failed to run regular TCP server")
          error.printStackTrace()
      }

    tcpServerBound.map { _ =>
      // pass bytes over tcp, collect response, send to self
      Source
        .single(ByteString.fromString("TCP works"))
        .via(tcp.outgoingConnection(tcpHost, tcpPort))
        .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        .onComplete {
          case Success(allTheBytes) => context.self ! allTheBytes.utf8String
          case Failure(error) =>
            println("Failed to run regular TCP client")
            error.printStackTrace()
            System.exit(1)
        }
    }

    // TCP TLS echo server
    val tlsHost = "127.0.0.1"
    val tlsPort = 1447
    val tlsServerBound = tcp
      .bindWithTls(tlsHost, tlsPort, () => createSSLEngine(Server))
      .toMat(Sink.foreach(incomingConnection =>
        incomingConnection.handleWith(
          Flow[ByteString].mapConcat(_.utf8String.toList).takeWhile(_ != '\n').map(c => ByteString(c)))))(Keep.left)
      .run()
      .andThen {
        case Success(_) =>
        case Failure(error) =>
          println("Failed to run TLS server")
          error.printStackTrace()
      }

    tlsServerBound.map { _ =>
      Source
        .single(ByteString.fromString("TLS works\n"))
        .concat(Source.maybe) // do not complete it from our side
        .via(tcp.outgoingConnectionWithTls(new InetSocketAddress(tlsHost, tlsPort), () => createSSLEngine(Client)))
        .takeWhile(!_.contains("\n"))
        .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        .onComplete {
          case Success(allTheBytes) => context.self ! allTheBytes.utf8String
          case Failure(error) =>
            println("TLS client failed")
            error.printStackTrace()
          // System.exit(1)
        }
    }

    // UDP
    val udpHost = "127.0.0.1"
    val udpPort = 1339
    val udpAddress = new InetSocketAddress(udpHost, udpPort)
    context.toClassic.actorOf(Props(new UdpListener(udpAddress, context.self)))
    context.toClassic.actorOf(Props(new UdpSender(udpAddress)))

    var waitingForMessages = Set("TCP works", "TLS works", "UDP works")
    Behaviors.receiveMessage[String] { msg =>
      context.log.info("IO check got {}", msg)
      waitingForMessages = waitingForMessages - msg
      if (waitingForMessages.isEmpty) {
        whenDone ! "IO subsystem works"
        Behaviors.stopped
      } else {
        context.log.info("IO check still waiting for {}", waitingForMessages)
        Behaviors.same
      }

    }
  }

  class UdpListener(address: InetSocketAddress, forwardTo: ActorRef[String]) extends Actor {
    import context.system
    IO(Udp) ! Udp.Bind(self, address)

    def receive = {
      case Udp.Bound(local) =>
        context.become(ready(sender()))
    }

    def ready(socket: akka.actor.ActorRef): Receive = {
      case Udp.Received(data, _) =>
        forwardTo ! data.utf8String
        context.stop(self)

      case Udp.Unbind  => socket ! Udp.Unbind
      case Udp.Unbound => context.stop(self)
    }
  }

  class UdpSender(remote: InetSocketAddress) extends Actor with Timers {
    import context.system
    IO(Udp) ! Udp.SimpleSender

    def receive = {
      case Udp.SimpleSenderReady =>
        // keep sending until stopped
        timers.startTimerAtFixedRate("key", "tick", 50.millis)
        context.become(ready(sender()))
    }

    def ready(send: akka.actor.ActorRef): Receive = {
      case msg: String =>
        send ! Udp.Send(ByteString.fromString("UDP works"), remote)
    }
  }

  lazy val sslContext: SSLContext = {
    // Don't hardcode your password in actual code
    val password = "abcdef".toCharArray

    // trust store and keys in one keystore
    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(getClass.getResourceAsStream("/tcp-tls-keystore.p12"), password)

    val trustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    trustManagerFactory.init(keyStore)

    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(keyStore, password)

    // init ssl context
    val context = SSLContext.getInstance("TLSv1.2")
    context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
    context
  }

  def createSSLEngine(role: TLSRole): SSLEngine = {
    val engine = sslContext.createSSLEngine()

    engine.setUseClientMode(role == akka.stream.Client)
    engine.setEnabledCipherSuites(Array("TLS_RSA_WITH_AES_128_CBC_SHA"))
    engine.setEnabledProtocols(Array("TLSv1.2"))

    engine
  }

}
