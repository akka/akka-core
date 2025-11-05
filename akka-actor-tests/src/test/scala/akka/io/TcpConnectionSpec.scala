/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.io.IOException
import java.net.{ InetSocketAddress, ServerSocket }
import java.net.SocketTimeoutException
import java.nio.ByteBuffer
import java.nio.channels._
import java.nio.channels.SelectionKey._
import java.nio.channels.spi.SelectorProvider
import java.nio.file.Files
import java.util.Random

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

import com.google.common.jimfs.{ Configuration, Jimfs }
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers._

import akka.actor._
import akka.io.Inet.SocketOption
import akka.io.SelectionHandler._
import akka.io.Tcp._
import akka.testkit.{ AkkaSpec, EventFilter, SocketUtil, TestActorRef, TestProbe }
import akka.testkit.SocketUtil.temporaryServerAddress
import akka.testkit.WithLogCapturing
import akka.util.{ ByteString, Helpers }

object TcpConnectionSpec {
  case class Ack(i: Int) extends Event
  object Ack extends Ack(0)
  final case class Registration(channel: SelectableChannel, initialOps: Int) extends NoSerializationVerificationNeeded
}

class TcpConnectionSpec extends AkkaSpec("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.io.tcp.trace-logging = on
    akka.io.tcp.register-timeout = 500ms
    """) with WithLogCapturing { thisSpecs =>
  import TcpConnectionSpec._

  // Helper to avoid Windows localization specific differences
  def ignoreIfWindows(): Unit =
    if (Helpers.isWindows) {
      info("Detected Windows: ignoring check")
      pending
    }

  lazy val ConnectionResetByPeerMessage: String = {
    val serverSocket = ServerSocketChannel.open()
    serverSocket.socket.bind(new InetSocketAddress("127.0.0.1", 0))
    try {
      val clientSocket = SocketChannel.open(new InetSocketAddress("127.0.0.1", serverSocket.socket().getLocalPort))
      val clientSocketOnServer = acceptServerSideConnection(serverSocket)
      clientSocketOnServer.socket.setSoLinger(true, 0)
      clientSocketOnServer.close()
      clientSocket.read(ByteBuffer.allocate(1))
      null
    } catch {
      case NonFatal(e) => e.getMessage
    }
  }

  lazy val ConnectionRefusedMessagePrefix: String = {
    val serverSocket = ServerSocketChannel.open()
    serverSocket.socket.bind(new InetSocketAddress("127.0.0.1", 0))
    try {
      serverSocket.close()
      val clientSocket = SocketChannel.open(new InetSocketAddress("127.0.0.1", serverSocket.socket().getLocalPort))
      clientSocket.finishConnect()
      clientSocket.write(ByteBuffer.allocate(1))
      null
    } catch {
      case NonFatal(e) => e.getMessage.take(15)
    }
  }

  "An outgoing connection" must {
    info("Connection reset by peer message expected is " + ConnectionResetByPeerMessage)
    info("Connection refused message prefix expected is " + ConnectionRefusedMessagePrefix)
    // common behavior

    "set socket options before connecting" in new LocalServerTest() {
      run {
        val connectionActor = createConnectionActor(options = Vector(Inet.SO.ReuseAddress(true)))
        val clientChannel = connectionActor.underlyingActor.channel
        clientChannel.socket.getReuseAddress should ===(true)
      }
    }

    "set socket options after connecting" ignore new LocalServerTest() {
      run {
        // Workaround for systems where SO_KEEPALIVE is true by default
        val connectionActor = createConnectionActor(options = Vector(SO.KeepAlive(false)))
        val clientChannel = connectionActor.underlyingActor.channel
        clientChannel.socket.getKeepAlive should ===(true) // only set after connection is established
        EventFilter.warning(pattern = "registration timeout", occurrences = 1).intercept {
          selector.send(connectionActor, ChannelConnectable)
          clientChannel.socket.getKeepAlive should ===(false)
        }
      }
    }

    "send incoming data to the connection handler" in new EstablishedConnectionTest() {
      run {
        serverSideChannel.write(ByteBuffer.wrap("testdata".getBytes("ASCII")))

        expectReceivedString("testdata")

        // have two packets in flight before the selector notices
        serverSideChannel.write(ByteBuffer.wrap("testdata2".getBytes("ASCII")))
        serverSideChannel.write(ByteBuffer.wrap("testdata3".getBytes("ASCII")))

        expectReceivedString("testdata2testdata3")
      }
    }

    "forward incoming data as Received messages instantly as long as more data is available" in
    new EstablishedConnectionTest() { // to make sure enough data gets through
      override lazy val connectionActor = createConnectionActor(options = List(Inet.SO.ReceiveBufferSize(1000000)))
      run {
        val bufferSize = Tcp(system).Settings.DirectBufferSize
        val DataSize = bufferSize + 1500
        val bigData = new Array[Byte](DataSize)
        val buffer = ByteBuffer.wrap(bigData)

        serverSideChannel.socket.setSendBufferSize(150000)
        val wrote = serverSideChannel.write(buffer)
        wrote should ===(DataSize)

        expectNoMessage(1000.millis) // data should have been transferred fully by now

        selector.send(connectionActor, ChannelReadable)

        connectionHandler.expectMsgType[Received].data.length should ===(bufferSize)
        connectionHandler.expectMsgType[Received].data.length should ===(1500)
      }
    }

    "receive data directly when the connection is established" in new UnacceptedConnectionTest() {
      run {
        val serverSideChannel = acceptServerSideConnection(localServerChannel)

        serverSideChannel.write(ByteBuffer.wrap("immediatedata".getBytes("ASCII")))
        serverSideChannel.configureBlocking(false)
        interestCallReceiver.expectMsg(OP_CONNECT)

        selector.send(connectionActor, ChannelConnectable)
        userHandler.expectMsg(
          Connected(serverAddress, clientSideChannel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress]))

        userHandler.send(connectionActor, Register(userHandler.ref))
        interestCallReceiver.expectMsg(OP_READ)
        selector.send(connectionActor, ChannelReadable)
        userHandler.expectMsgType[Received].data.decodeString("ASCII") should ===("immediatedata")
        ignoreWindowsWorkaroundForTicket15766()
        interestCallReceiver.expectMsg(OP_READ)
      }
    }

    "write data to network (and acknowledge)" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()

        // reply to write commander with Ack
        val ackedWrite = Write(ByteString("testdata"), Ack)
        val buffer = ByteBuffer.allocate(100)
        serverSideChannel.read(buffer) should ===(0)
        writer.send(connectionActor, ackedWrite)
        writer.expectMsg(Ack)
        pullFromServerSide(remaining = 8, into = buffer)
        buffer.flip()
        buffer.limit() should ===(8)

        // not reply to write commander for writes without Ack
        val unackedWrite = Write(ByteString("morestuff!"))
        buffer.clear()
        serverSideChannel.read(buffer) should ===(0)
        writer.send(connectionActor, unackedWrite)
        writer.expectNoMessage(500.millis)
        pullFromServerSide(remaining = 10, into = buffer)
        buffer.flip()
        ByteString(buffer).utf8String should ===("morestuff!")
      }
    }

    "send big buffers to network correctly" in new EstablishedConnectionTest() {
      run {
        val bufferSize = 512 * 1024 // 256kB are too few
        val random = new Random(0)
        val testBytes = new Array[Byte](bufferSize)
        random.nextBytes(testBytes)
        val testData = ByteString(testBytes)

        val writer = TestProbe()

        val write = Write(testData, Ack)
        val buffer = ByteBuffer.allocate(bufferSize)
        serverSideChannel.read(buffer) should ===(0)
        writer.send(connectionActor, write)
        pullFromServerSide(remaining = bufferSize, into = buffer)
        buffer.flip()
        buffer.limit() should ===(bufferSize)

        ByteString(buffer) should ===(testData)
      }
    }

    "write data after not acknowledged data" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()
        writer.send(connectionActor, Write(ByteString(42.toByte)))
        writer.expectNoMessage(500.millis)
      }
    }

    "acknowledge the completion of an ACKed empty write" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()
        writer.send(connectionActor, Write(ByteString.empty, Ack))
        writer.expectMsg(Ack)
      }
    }

    "not acknowledge the completion of a NACKed empty write" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()
        writer.send(connectionActor, Write(ByteString.empty, NoAck))
        writer.expectNoMessage(250.millis)
        writer.send(connectionActor, Write(ByteString.empty, NoAck(42)))
        writer.expectNoMessage(250.millis)
      }
    }

    "write file to network" in new EstablishedConnectionTest() {
      run {
        val fs = Jimfs.newFileSystem("write-file-in-network", Configuration.unix())
        val tmpFile = Files.createTempFile(fs.getPath("/"), "whatever", ".dat")
        val writer = Files.newBufferedWriter(tmpFile)
        val oneKByteOfF = Array.fill[Char](1000)('F')
        // 10 mb of f:s in a file
        for (_ <- 0 to 10000) {
          writer.write(oneKByteOfF)
        }
        writer.flush()
        writer.close()
        try {
          val writer = TestProbe()
          val size = Files.size(tmpFile).toInt
          writer.send(connectionActor, WritePath(tmpFile, 0, size, Ack))
          pullFromServerSide(size, 1000000)
          writer.expectMsg(Ack)
        } finally {
          fs.close()
        }
      }
    }

    "write a CompoundWrite to the network and produce correct ACKs" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()
        val compoundWrite =
          Write(ByteString("test1"), Ack(1)) +:
          Write(ByteString("test2")) +:
          Write(ByteString.empty, Ack(3)) +:
          Write(ByteString("test4"), Ack(4))

        // reply to write commander with Ack
        val buffer = ByteBuffer.allocate(100)
        serverSideChannel.read(buffer) should ===(0)
        writer.send(connectionActor, compoundWrite)

        pullFromServerSide(remaining = 15, into = buffer)
        buffer.flip()
        ByteString(buffer).utf8String should ===("test1test2test4")
        writer.expectMsg(Ack(1))
        writer.expectMsg(Ack(3))
        writer.expectMsg(Ack(4))
      }
    }

    /*
     * Disabled on Windows: https://support.microsoft.com/kb/214397
     *
     * "To optimize performance at the application layer, Winsock copies data buffers from application send calls
     * to a Winsock kernel buffer. Then, the stack uses its own heuristics (such as Nagle algorithm) to determine
     * when to actually put the packet on the wire. You can change the amount of Winsock kernel buffer allocated to
     * the socket using the SO_SNDBUF option (it is 8K by default). If necessary, Winsock can buffer significantly more
     * than the SO_SNDBUF buffer size. In most cases, the send completion in the application only indicates the data
     * buffer in an application send call is copied to the Winsock kernel buffer and does not indicate that the data
     * has hit the network medium. The only exception is when you disable the Winsock buffering by setting
     * SO_SNDBUF to 0."
     */
    "stop writing in cases of backpressure and resume afterwards" in
    new EstablishedConnectionTest() {
      override lazy val connectionActor = createConnectionActor(options = List(Inet.SO.ReceiveBufferSize(1000000)))
      run {
        info(
          "Currently ignored as SO_SNDBUF is usually a lower bound on the send buffer so the test fails as no real " +
          "backpressure present.")
        pending
        ignoreIfWindows()
        object Ack1 extends Event
        object Ack2 extends Event

        clientSideChannel.socket.setSendBufferSize(1024)

        awaitCond(clientSideChannel.socket.getSendBufferSize == 1024)

        val writer = TestProbe()

        // producing backpressure by sending much more than currently fits into
        // our send buffer
        val firstWrite = writeCmd(Ack1)

        // try to write the buffer but since the SO_SNDBUF is too small
        // it will have to keep the rest of the piece and send it
        // when possible
        writer.send(connectionActor, firstWrite)
        interestCallReceiver.expectMsg(OP_WRITE)

        // send another write which should fail immediately
        // because we don't store more than one piece in flight
        val secondWrite = writeCmd(Ack2)
        writer.send(connectionActor, secondWrite)
        writer.expectMsg(CommandFailed(secondWrite))

        // reject even empty writes
        writer.send(connectionActor, Write.empty)
        writer.expectMsg(CommandFailed(Write.empty))

        // there will be immediately more space in the send buffer because
        // some data will have been sent by now, so we assume we can write
        // again, but still it can't write everything
        selector.send(connectionActor, ChannelWritable)

        // both buffers should now be filled so no more writing
        // is possible
        pullFromServerSide(TestSize)
        writer.expectMsg(Ack1)
      }
    }

    "respect StopReading and ResumeReading" in new EstablishedConnectionTest() {
      run {
        serverSideChannel.write(ByteBuffer.wrap("testdata".getBytes("ASCII")))
        connectionHandler.send(connectionActor, SuspendReading)

        // the selector interprets SuspendReading to deregister interest for reading
        interestCallReceiver.expectMsg(-OP_READ)

        // this simulates a race condition where ChannelReadable was already underway while
        // processing SuspendReading
        selector.send(connectionActor, ChannelReadable)

        // this ChannelReadable should be properly ignored, even if data is already pending
        interestCallReceiver.expectNoMessage(100.millis)
        connectionHandler.expectNoMessage(100.millis)

        connectionHandler.send(connectionActor, ResumeReading)
        interestCallReceiver.expectMsg(OP_READ)

        // data should be received only after ResumeReading
        expectReceivedString("testdata")
      }
    }

    "respect pull mode" in new EstablishedConnectionTest(pullMode = true) {
      // override config to decrease default buffer size
      def config =
        ConfigFactory.parseString("akka.io.tcp.direct-buffer-size = 1k").withFallback(AkkaSpec.testConf)
      override implicit lazy val system: ActorSystem = ActorSystem("respectPullModeTest", config)

      try run {
        val maxBufferSize = 1 * 1024
        val ts = "t" * maxBufferSize
        val us = "u" * (maxBufferSize / 2)

        // send a batch that is bigger than the default buffer to make sure we don't recurse and
        // send more than one Received messages
        serverSideChannel.write(ByteBuffer.wrap((ts ++ us).getBytes("ASCII")))
        connectionHandler.expectNoMessage(100.millis)

        connectionActor ! ResumeReading
        connectionHandler.expectMsgType[Received].data.decodeString("ASCII") should ===(ts)

        connectionHandler.expectNoMessage(100.millis)

        connectionActor ! ResumeReading
        connectionHandler.expectMsgType[Received].data.decodeString("ASCII") should ===(us)

        connectionHandler.expectNoMessage(100.millis)

        val vs = "v" * (maxBufferSize / 2)
        serverSideChannel.write(ByteBuffer.wrap(vs.getBytes("ASCII")))

        connectionActor ! ResumeReading

        connectionHandler.expectMsgType[Received].data.decodeString("ASCII") should ===(vs)
      } finally shutdown(system)
    }

    "close the connection and reply with `Closed` upon reception of a `Close` command" in
    new EstablishedConnectionTest() with SmallRcvBuffer {
      run {
        // we should test here that a pending write command is properly finished first

        // set an artificially small send buffer size so that the write is queued
        // inside the connection actor
        clientSideChannel.socket.setSendBufferSize(1024)

        // we send a write and a close command directly afterwards
        connectionHandler.send(connectionActor, writeCmd(Ack))
        val closeCommander = TestProbe()
        closeCommander.send(connectionActor, Close)

        pullFromServerSide(TestSize)
        connectionHandler.expectMsg(Ack)
        connectionHandler.expectMsg(Closed)
        closeCommander.expectMsg(Closed)
        assertThisConnectionActorTerminated()

        serverSelectionKey should be(selectedAs(OP_READ, 2.seconds))

        val buffer = ByteBuffer.allocate(1)
        serverSideChannel.read(buffer) should ===(-1)
      }
    }

    "send only one `Closed` event to the handler, if the handler commanded the Close" in
    new EstablishedConnectionTest() {
      run {
        connectionHandler.send(connectionActor, Close)
        connectionHandler.expectMsg(Closed)
        connectionHandler.expectNoMessage(500.millis)
      }
    }

    "abort the connection and reply with `Aborted` upon reception of an `Abort` command" in
    new EstablishedConnectionTest() {
      run {
        connectionHandler.send(connectionActor, Abort)
        connectionHandler.expectMsg(Aborted)

        assertThisConnectionActorTerminated()

        val buffer = ByteBuffer.allocate(1)
        val thrown = the[IOException] thrownBy {
          windowsWorkaroundToDetectAbort()
          serverSideChannel.read(buffer)
        }
        thrown.getMessage should ===(ConnectionResetByPeerMessage)
      }
    }

    /*
     * Partly disabled on Windows: https://support.microsoft.com/kb/214397
     *
     * "To optimize performance at the application layer, Winsock copies data buffers from application send calls
     * to a Winsock kernel buffer. Then, the stack uses its own heuristics (such as Nagle algorithm) to determine
     * when to actually put the packet on the wire. You can change the amount of Winsock kernel buffer allocated to
     * the socket using the SO_SNDBUF option (it is 8K by default). If necessary, Winsock can buffer significantly more
     * than the SO_SNDBUF buffer size. In most cases, the send completion in the application only indicates the data
     * buffer in an application send call is copied to the Winsock kernel buffer and does not indicate that the data
     * has hit the network medium. The only exception is when you disable the Winsock buffering by setting
     * SO_SNDBUF to 0."
     */
    "close the connection and reply with `ConfirmedClosed` upon reception of an `ConfirmedClose` command (simplified)" in
    new EstablishedConnectionTest() with SmallRcvBuffer {
      run {
        // we should test here that a pending write command is properly finished first

        // set an artificially small send buffer size so that the write is queued
        // inside the connection actor
        clientSideChannel.socket.setSendBufferSize(1024)

        // we send a write and a close command directly afterwards
        connectionHandler.send(connectionActor, writeCmd(Ack))
        connectionHandler.send(connectionActor, ConfirmedClose)

        pullFromServerSide(TestSize)
        connectionHandler.expectMsg(Ack)

        selector.send(connectionActor, ChannelReadable)

        val buffer = ByteBuffer.allocate(1)
        serverSelectionKey should be(selectedAs(OP_READ, 2.seconds))
        serverSideChannel.read(buffer) should ===(-1)

        closeServerSideAndWaitForClientReadable()

        selector.send(connectionActor, ChannelReadable)
        connectionHandler.expectMsg(ConfirmedClosed)

        assertThisConnectionActorTerminated()
      }
    }

    "close the connection and reply with `ConfirmedClosed` upon reception of an `ConfirmedClose` command" in
    new EstablishedConnectionTest() with SmallRcvBuffer {
      run {
        ignoreIfWindows()

        // we should test here that a pending write command is properly finished first

        // set an artificially small send buffer size so that the write is queued
        // inside the connection actor
        clientSideChannel.socket.setSendBufferSize(1024)

        // we send a write and a close command directly afterwards
        connectionHandler.send(connectionActor, writeCmd(Ack))
        connectionHandler.send(connectionActor, ConfirmedClose)

        connectionHandler.expectNoMessage(100.millis)
        pullFromServerSide(TestSize)
        connectionHandler.expectMsg(Ack)

        selector.send(connectionActor, ChannelReadable)
        connectionHandler.expectNoMessage(100.millis) // not yet

        val buffer = ByteBuffer.allocate(1)
        serverSelectionKey should be(selectedAs(SelectionKey.OP_READ, 2.seconds))
        serverSideChannel.read(buffer) should ===(-1)

        closeServerSideAndWaitForClientReadable()

        selector.send(connectionActor, ChannelReadable)
        connectionHandler.expectMsg(ConfirmedClosed)

        assertThisConnectionActorTerminated()
      }
    }

    "report when peer closed the connection" in new EstablishedConnectionTest() {
      run {
        closeServerSideAndWaitForClientReadable()

        selector.send(connectionActor, ChannelReadable)
        connectionHandler.expectMsg(PeerClosed)

        assertThisConnectionActorTerminated()
      }
    }

    "report when peer closed the connection but allow further writes and acknowledge normal close" in
    new EstablishedConnectionTest(keepOpenOnPeerClosed = true) {
      run {
        closeServerSideAndWaitForClientReadable(fullClose = false) // send EOF (fin) from the server side

        selector.send(connectionActor, ChannelReadable)
        connectionHandler.expectMsg(PeerClosed)

        connectionHandler.send(connectionActor, writeCmd(Ack))
        pullFromServerSide(TestSize)
        connectionHandler.expectMsg(Ack)
        connectionHandler.send(connectionActor, Close)
        connectionHandler.expectMsg(Closed)

        assertThisConnectionActorTerminated()
      }
    }

    "report when peer closed the connection but allow further writes and acknowledge confirmed close" in
    new EstablishedConnectionTest(keepOpenOnPeerClosed = true) {
      run {
        closeServerSideAndWaitForClientReadable(fullClose = false) // send EOF (fin) from the server side

        selector.send(connectionActor, ChannelReadable)
        connectionHandler.expectMsg(PeerClosed)

        connectionHandler.send(connectionActor, writeCmd(Ack))
        pullFromServerSide(TestSize)
        connectionHandler.expectMsg(Ack)
        connectionHandler.send(connectionActor, ConfirmedClose)
        connectionHandler.expectMsg(ConfirmedClosed)

        assertThisConnectionActorTerminated()
      }
    }

    "report when peer aborted the connection" in new EstablishedConnectionTest() {
      run {
        abortClose(serverSideChannel)
        selector.send(connectionActor, ChannelReadable)
        val err = connectionHandler.expectMsgType[ErrorClosed]
        err.cause should ===(ConnectionResetByPeerMessage)

        // wait a while
        connectionHandler.expectNoMessage(200.millis)

        assertThisConnectionActorTerminated()
      }
    }

    "report when peer closed the connection when trying to write" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()

        abortClose(serverSideChannel)
        writer.send(connectionActor, Write(ByteString("testdata")))
        // both writer and handler should get the message
        writer.expectMsgType[ErrorClosed]
        connectionHandler.expectMsgType[ErrorClosed]

        assertThisConnectionActorTerminated()
      }
    }

    val UnboundAddress = temporaryServerAddress()

    "report failed connection attempt when target is unreachable" in
    new UnacceptedConnectionTest() {
      override lazy val connectionActor = createConnectionActor(serverAddress = UnboundAddress)
      run {
        val sel = SelectorProvider.provider().openSelector()
        try {
          val key = clientSideChannel.register(sel, SelectionKey.OP_CONNECT | SelectionKey.OP_READ)
          // This timeout should be large enough to work on Windows
          sel.select(3000)

          key.isConnectable should ===(true)
          connectionActor.toString // force the lazy val
          Thread.sleep(300)
          selector.send(connectionActor, ChannelConnectable)
          userHandler.expectMsg(CommandFailed(Connect(UnboundAddress)))

          watch(connectionActor)
          expectTerminated(connectionActor)
        } finally sel.close()
      }
    }

    "report failed connection attempt when target cannot be resolved" in
    new UnacceptedConnectionTest() {
      val address = new InetSocketAddress("notthere.local", 666)
      override lazy val connectionActor = createConnectionActorWithoutRegistration(serverAddress = address)
      run {
        connectionActor ! newChannelRegistration
        userHandler.expectMsg(15.seconds, CommandFailed(Connect(address)))
      }
    }

    "report failed connection attempt when timing out" in
    new UnacceptedConnectionTest() {
      override lazy val connectionActor =
        createConnectionActor(serverAddress = UnboundAddress, timeout = Option(100.millis))
      run {
        connectionActor.toString should not be ("")
        userHandler.expectMsg(CommandFailed(Connect(UnboundAddress, timeout = Option(100.millis))))
        watch(connectionActor)
        expectTerminated(connectionActor)
      }
    }

    "time out when Connected isn't answered with Register" in new UnacceptedConnectionTest {
      run {
        localServerChannel.accept()

        selector.send(connectionActor, ChannelConnectable)
        userHandler.expectMsg(
          Connected(serverAddress, clientSideChannel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress]))

        watch(connectionActor)
        expectTerminated(connectionActor)
      }
    }

    "close the connection when user handler dies while connecting" in new UnacceptedConnectionTest {
      run {
        EventFilter[DeathPactException](occurrences = 1).intercept {
          userHandler.ref ! PoisonPill

          watch(connectionActor)
          expectTerminated(connectionActor)
        }
      }
    }

    "close the connection when connection handler dies while connected" in new EstablishedConnectionTest() {
      run {
        watch(connectionHandler.ref)
        watch(connectionActor)
        EventFilter[DeathPactException](occurrences = 1).intercept {
          system.stop(connectionHandler.ref)
          val deaths = Set(expectMsgType[Terminated].actor, expectMsgType[Terminated].actor)
          deaths should ===(Set(connectionHandler.ref, connectionActor))
        }
      }
    }

    "support ResumeWriting (backed up)" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()
        val write = writeCmd(NoAck)

        // fill up the write buffer until NACK
        var written = 0
        while (!writer.msgAvailable) {
          writer.send(connectionActor, write)
          written += 1
        }
        // dump the NACKs
        writer.receiveWhile(1.second) {
          case CommandFailed(_) => written -= 1
        }
        writer.msgAvailable should ===(false)

        // writes must fail now
        writer.send(connectionActor, write)
        writer.expectMsg(CommandFailed(write))
        writer.send(connectionActor, Write.empty)
        writer.expectMsg(CommandFailed(Write.empty))

        // resuming must not immediately work (queue still full)
        writer.send(connectionActor, ResumeWriting)
        writer.expectNoMessage(1.second)

        // so drain the queue until it works again
        while (!writer.msgAvailable) pullFromServerSide(TestSize)
        writer.expectMsg(Duration.Zero, WritingResumed)

        // now write should work again
        object works extends Event
        writer.send(connectionActor, writeCmd(works))
        writer.expectMsg(works)
      }
    }

    "support ResumeWriting (queue flushed)" in new EstablishedConnectionTest() {
      run {
        val writer = TestProbe()
        val write = writeCmd(NoAck)

        // fill up the write buffer until NACK
        var written = 0
        while (!writer.msgAvailable) {
          writer.send(connectionActor, write)
          written += 1
        }
        // dump the NACKs
        writer.receiveWhile(1.second) {
          case CommandFailed(_) => written -= 1
        }

        // drain the queue until it works again
        pullFromServerSide(TestSize * written)

        // writes must still fail
        writer.send(connectionActor, write)
        writer.expectMsg(CommandFailed(write))
        writer.send(connectionActor, Write.empty)
        writer.expectMsg(CommandFailed(Write.empty))

        // resuming must work immediately
        writer.send(connectionActor, ResumeWriting)
        writer.expectMsg(1.second, WritingResumed)

        // now write should work again
        object works extends Event
        writer.send(connectionActor, writeCmd(works))
        writer.expectMsg(works)
      }
    }

    "support useResumeWriting==false (backed up)" in new EstablishedConnectionTest(useResumeWriting = false) {
      run {
        val writer = TestProbe()
        val write = writeCmd(NoAck)

        // fill up the write buffer until NACK
        var written = 0
        while (!writer.msgAvailable) {
          writer.send(connectionActor, write)
          written += 1
        }
        // dump the NACKs
        writer.receiveWhile(1.second) {
          case CommandFailed(_) => written -= 1
        }
        writer.msgAvailable should ===(false)

        // writes must fail now
        writer.send(connectionActor, write)
        writer.expectMsg(CommandFailed(write))
        writer.send(connectionActor, Write.empty)
        writer.expectMsg(CommandFailed(Write.empty))

        // so drain the queue until it works again
        pullFromServerSide(TestSize * written)

        // now write should work again
        object works extends Event
        writer.send(connectionActor, writeCmd(works))
        writer.expectMsg(works)
      }
    }

    "support useResumeWriting==false (queue flushed)" in new EstablishedConnectionTest(useResumeWriting = false) {
      run {
        val writer = TestProbe()
        val write = writeCmd(NoAck)

        // fill up the write buffer until NACK
        var written = 0
        while (!writer.msgAvailable) {
          writer.send(connectionActor, write)
          written += 1
        }
        // dump the NACKs
        writer.receiveWhile(1.second) {
          case CommandFailed(_) => written -= 1
        }

        // drain the queue until it works again
        pullFromServerSide(TestSize * written)

        // now write should work again
        object works extends Event
        writer.send(connectionActor, writeCmd(works))
        writer.expectMsg(works)
      }
    }

    "report abort before handler is registered (reproducer from #15033)" in {
      // This test needs the OP_CONNECT workaround on Windows, see original report #15033 and parent ticket #15766

      val bindAddress = SocketUtil.temporaryServerAddress()
      val serverSocket = new ServerSocket(bindAddress.getPort, 100, bindAddress.getAddress)
      val connectionProbe = TestProbe()

      connectionProbe.send(IO(Tcp), Connect(bindAddress))

      IO(Tcp) ! Connect(bindAddress)

      try {
        serverSocket.setSoTimeout(remainingOrDefault.toMillis.toInt)
        val socket = serverSocket.accept()
        connectionProbe.expectMsgType[Tcp.Connected]
        val connectionActor = connectionProbe.sender()
        connectionActor ! PoisonPill
        watch(connectionActor)
        expectTerminated(connectionActor)
        an[IOException] should be thrownBy { socket.getInputStream.read() }
      } catch {
        case e: SocketTimeoutException =>
          // thrown by serverSocket.accept, this may happen if network is offline
          info(e.getMessage)
          pending
      }
    }
  }

  /////////////////////////////////////// TEST SUPPORT ////////////////////////////////////////////////

  def acceptServerSideConnection(localServer: ServerSocketChannel): SocketChannel = {
    @volatile var serverSideChannel: SocketChannel = null
    awaitCond {
      serverSideChannel = localServer.accept()
      serverSideChannel != null
    }
    serverSideChannel
  }

  abstract class LocalServerTest extends ChannelRegistry {

    /** Allows overriding the system used */
    implicit def system: ActorSystem = thisSpecs.system

    val serverAddress = temporaryServerAddress()
    val localServerChannel = ServerSocketChannel.open()
    val userHandler = TestProbe()
    val selector = TestProbe()

    var registerCallReceiver = TestProbe()
    var interestCallReceiver = TestProbe()

    def ignoreWindowsWorkaroundForTicket15766(): Unit = {
      // Due to the Windows workaround of #15766 we need to set an OP_CONNECT to reliably detect connection resets
      if (Helpers.isWindows) interestCallReceiver.expectMsg(OP_CONNECT)
    }

    def run(body: => Unit): Unit = {
      try {
        setServerSocketOptions()
        localServerChannel.socket.bind(serverAddress)
        localServerChannel.configureBlocking(false)
        body
      } finally localServerChannel.close()
    }

    def register(channel: SelectableChannel, initialOps: Int)(implicit channelActor: ActorRef): Unit =
      registerCallReceiver.ref.tell(Registration(channel, initialOps), channelActor)

    def setServerSocketOptions() = ()

    def createConnectionActor(
        serverAddress: InetSocketAddress = serverAddress,
        options: immutable.Seq[SocketOption] = Nil,
        timeout: Option[FiniteDuration] = None,
        pullMode: Boolean = false): TestActorRef[TcpOutgoingConnection] = {
      val ref = createConnectionActorWithoutRegistration(serverAddress, options, timeout, pullMode)
      ref ! newChannelRegistration
      ref
    }

    def newChannelRegistration: ChannelRegistration =
      new ChannelRegistration {
        def enableInterest(op: Int): Unit = interestCallReceiver.ref ! op
        def disableInterest(op: Int): Unit = interestCallReceiver.ref ! -op
        def cancelAndClose(andThen: () => Unit): Unit = onCancelAndClose(andThen)
      }

    protected def onCancelAndClose(andThen: () => Unit): Unit = andThen()

    def createConnectionActorWithoutRegistration(
        serverAddress: InetSocketAddress = serverAddress,
        options: immutable.Seq[SocketOption] = Nil,
        timeout: Option[FiniteDuration] = None,
        pullMode: Boolean = false): TestActorRef[TcpOutgoingConnection] =
      TestActorRef(
        new TcpOutgoingConnection(
          Tcp(system),
          this,
          userHandler.ref,
          Connect(serverAddress, options = options, timeout = timeout, pullMode = pullMode)) {
          override def postRestart(reason: Throwable): Unit = context.stop(self) // ensure we never restart
        })
  }

  trait SmallRcvBuffer { this: LocalServerTest =>
    override def setServerSocketOptions(): Unit = localServerChannel.socket.setReceiveBufferSize(1024)
  }

  abstract class UnacceptedConnectionTest(pullMode: Boolean = false) extends LocalServerTest {
    // lazy init since potential exceptions should not be triggered in the constructor but during execution of `run`
    private[io] lazy val connectionActor = createConnectionActor(serverAddress, pullMode = pullMode)
    // calling .underlyingActor ensures that the actor is actually created at this point
    lazy val clientSideChannel = connectionActor.underlyingActor.channel

    override def run(body: => Unit): Unit = super.run {
      registerCallReceiver.expectMsg(Registration(clientSideChannel, 0))
      registerCallReceiver.sender() should ===(connectionActor)
      body
    }
  }

  abstract class EstablishedConnectionTest(
      keepOpenOnPeerClosed: Boolean = false,
      useResumeWriting: Boolean = true,
      pullMode: Boolean = false)
      extends UnacceptedConnectionTest(pullMode) {

    // lazy init since potential exceptions should not be triggered in the constructor but during execution of `run`
    lazy val serverSideChannel = acceptServerSideConnection(localServerChannel)
    lazy val connectionHandler = TestProbe()
    lazy val nioSelector = SelectorProvider.provider().openSelector()
    lazy val clientSelectionKey = registerChannel(clientSideChannel, "client")
    lazy val serverSelectionKey = registerChannel(serverSideChannel, "server")
    lazy val defaultbuffer = ByteBuffer.allocate(TestSize)

    def windowsWorkaroundToDetectAbort(): Unit = {
      // Due to a Windows quirk we need to set an OP_CONNECT to reliably detect connection resets, see #1576
      if (Helpers.isWindows) {
        serverSelectionKey.interestOps(OP_CONNECT)
        nioSelector.select(10)
      }
    }

    override def ignoreWindowsWorkaroundForTicket15766(): Unit = {
      super.ignoreWindowsWorkaroundForTicket15766()
      if (Helpers.isWindows) clientSelectionKey.interestOps(OP_CONNECT)
    }

    override def run(body: => Unit): Unit = super.run {
      try {
        serverSideChannel.configureBlocking(false)
        serverSideChannel should not be (null)

        interestCallReceiver.expectMsg(OP_CONNECT)
        selector.send(connectionActor, ChannelConnectable)
        userHandler.expectMsg(
          Connected(serverAddress, clientSideChannel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress]))

        userHandler.send(connectionActor, Register(connectionHandler.ref, keepOpenOnPeerClosed, useResumeWriting))
        ignoreWindowsWorkaroundForTicket15766()
        if (!pullMode) interestCallReceiver.expectMsg(OP_READ)

        clientSelectionKey // trigger initialization
        serverSelectionKey // trigger initialization
        body
      } finally {
        serverSideChannel.close()
        nioSelector.close()
      }
    }

    final val TestSize = 10000 // compile-time constant

    def writeCmd(ack: Event) =
      Write(ByteString(Array.fill[Byte](TestSize)(0)), ack)

    def closeServerSideAndWaitForClientReadable(fullClose: Boolean = true): Unit = {
      if (fullClose) serverSideChannel.close() else serverSideChannel.socket.shutdownOutput()
      checkFor(clientSelectionKey, OP_READ, 3.seconds.toMillis.toInt) should ===(true)
    }

    def registerChannel(channel: SocketChannel, name: String): SelectionKey = {
      val res = channel.register(nioSelector, 0)
      res.attach(name)
      res
    }

    def checkFor(key: SelectionKey, interest: Int, millis: Int = 100): Boolean =
      if (key.isValid) {
        key.interestOps(interest)
        nioSelector.selectedKeys().clear()
        val ret = nioSelector.select(millis)
        key.interestOps(0)

        ret > 0 && nioSelector.selectedKeys().contains(key) && key.isValid &&
        (key.readyOps() & interest) != 0
      } else false

    def openSelectorFor(channel: SocketChannel, interests: Int): (Selector, SelectionKey) = {
      val sel = SelectorProvider.provider().openSelector()
      val key = channel.register(sel, interests)
      (sel, key)
    }

    override protected def onCancelAndClose(andThen: () => Unit): Unit =
      try {
        if (clientSideChannel.isOpen) clientSideChannel.close()
        if (nioSelector.isOpen) {
          nioSelector.selectNow()
          nioSelector.selectedKeys().clear()
        }
      } finally Try(andThen())

    /**
     * Tries to simultaneously act on client and server side to read from the server all pending data from the client.
     */
    @tailrec final def pullFromServerSide(
        remaining: Int,
        remainingTries: Int = 1000,
        into: ByteBuffer = defaultbuffer): Unit =
      if (remainingTries <= 0)
        throw new AssertionError("Pulling took too many loops,  remaining data: " + remaining)
      else if (remaining > 0) {
        if (interestCallReceiver.msgAvailable) {
          interestCallReceiver.expectMsg(OP_WRITE)
          clientSelectionKey.interestOps(OP_WRITE)
        }

        serverSelectionKey.interestOps(OP_READ)
        nioSelector.select(10)
        if (nioSelector.selectedKeys().contains(clientSelectionKey)) {
          clientSelectionKey.interestOps(0)
          selector.send(connectionActor, ChannelWritable)
        }

        val read =
          if (nioSelector.selectedKeys().contains(serverSelectionKey)) {
            if (into eq defaultbuffer) into.clear()
            serverSideChannel.read(into) match {
              case -1 =>
                throw new IllegalStateException("Connection was closed unexpectedly with remaining bytes " + remaining)
              case 0     => throw new IllegalStateException("Made no progress")
              case other => other
            }
          } else 0

        nioSelector.selectedKeys().clear()

        pullFromServerSide(remaining - read, remainingTries - 1, into)
      }

    @tailrec final def expectReceivedString(data: String): Unit = {
      data.length should be > 0

      selector.send(connectionActor, ChannelReadable)

      val gotReceived = connectionHandler.expectMsgType[Received]
      val receivedString = gotReceived.data.decodeString("ASCII")
      data.startsWith(receivedString) should ===(true)
      if (receivedString.length < data.length)
        expectReceivedString(data.drop(receivedString.length))
    }

    def assertThisConnectionActorTerminated(): Unit = {
      watch(connectionActor)
      expectTerminated(connectionActor)
      clientSideChannel.isOpen should be(false)
    }

    def selectedAs(interest: Int, duration: Duration): BeMatcher[SelectionKey] =
      new BeMatcher[SelectionKey] {
        def apply(key: SelectionKey) =
          MatchResult(
            checkFor(key, interest, duration.toMillis.toInt),
            "%s key was not selected for %s after %s".format(key.attachment(), interestsDesc(interest), duration),
            "%s key was selected for %s after %s".format(key.attachment(), interestsDesc(interest), duration))
      }

    val interestsNames =
      Seq(OP_ACCEPT -> "accepting", OP_CONNECT -> "connecting", OP_READ -> "reading", OP_WRITE -> "writing")
    def interestsDesc(interests: Int): String =
      interestsNames.filter(i => (i._1 & interests) != 0).map(_._2).mkString(", ")

    def abortClose(channel: SocketChannel): Unit = {
      try channel.socket.setSoLinger(true, 0) // causes the following close() to send TCP RST
      catch {
        case NonFatal(e) =>
          // setSoLinger can fail due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6799574
          // (also affected: OS/X Java 1.6.0_37)
          log.debug("setSoLinger(true, 0) failed with {}", e)
      }
      channel.close()
      nioSelector.selectNow()
      nioSelector.selectedKeys().clear()
    }
  }

}
