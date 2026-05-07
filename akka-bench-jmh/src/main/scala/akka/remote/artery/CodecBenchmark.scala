/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.Done
import akka.NotUsed
import akka.actor._
import akka.remote._
import akka.remote.artery.Decoder.InboundCompressionAccess
import akka.remote.artery.compress._
import akka.serialization.BaseSerializer
import akka.serialization.ByteBufferSerializer
import akka.serialization.SerializationExtension
import akka.stream.SystemMaterializer
import akka.stream.scaladsl._
import akka.util.OptionVal

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
@Warmup(iterations = 4)
@Measurement(iterations = 5)
class CodecBenchmark {
  import CodecBenchmark._

  @nowarn("msg=immutable val") // JMH updates this via reflection
  @Param(Array(Standard, RemoteInstrument))
  private var configType: String = _

  implicit var system: ActorSystem = _
  var systemB: ActorSystem = _

  private val envelopePool = new EnvelopeBufferPool(1024 * 1024, 128)
  private val inboundEnvelopePool = ReusableInboundEnvelope.createObjectPool(capacity = 16)
  private val outboundEnvelopePool = ReusableOutboundEnvelope.createObjectPool(capacity = 16)

  val headerIn = HeaderBuilder.in(NoInboundCompressions)
  val envelopeTemplateBuffer = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN)

  var uniqueLocalAddress: UniqueAddress = _
  val payload = DummyMessageInstance

  private val inboundContext: InboundContext = new InboundContext {
    override def localAddress: UniqueAddress = uniqueLocalAddress
    override def association(uid: Long): OptionVal[OutboundContext] = OptionVal.None
    // the following methods are not used by in this test
    override def sendControl(to: Address, message: ControlMessage): Unit = ???
    override def association(remoteAddress: Address): OutboundContext = ???
    override def completeHandshake(peer: UniqueAddress): Future[Done] = ???
    override lazy val settings: ArterySettings =
      ArterySettings(ConfigFactory.load().getConfig("akka.remote.artery"))
    override def publishDropped(inbound: InboundEnvelope, reason: String): Unit = ()
  }

  private var remoteRefB: RemoteActorRef = _
  @nowarn("msg=never used") private var resolvedRef: InternalActorRef = _
  @nowarn("msg=never used") private var senderStringA: String = _
  @nowarn("msg=never used") private var recipientStringB: String = _

  private var encodeGraph: Flow[String, Unit, NotUsed] = _
  private var decodeGraph: Flow[String, Unit, NotUsed] = _
  private var encodeDecodeGraph: Flow[String, Unit, NotUsed] = _

  @Setup(Level.Trial)
  def setupTrial(): Unit = {
    val commonConfig = ConfigFactory.parseString(s"""
    akka {
       loglevel = WARNING
       actor.provider = remote
       remote.artery.enabled = on
       remote.artery.canonical.hostname = localhost
       remote.artery.canonical.port = 0
       actor.serializers.codec-benchmark = "${classOf[DummyMessageSerializer].getName}"
       actor.serialization-identifiers { "${classOf[DummyMessageSerializer].getName}" = 4711 }
       actor.serialization-bindings {"${classOf[DummyMessage].getName}" = codec-benchmark }
    }
    """)
    val config = configType match {
      case RemoteInstrument =>
        ConfigFactory
          .parseString(s"""akka.remote.artery.advanced.instruments = [ "${classOf[DummyRemoteInstrument].getName}" ]""")
          .withFallback(commonConfig)
      case _ =>
        commonConfig
    }

    system = ActorSystem("CodecBenchmark", config)
    systemB = ActorSystem("systemB", system.settings.config)

    // eager init of materializer
    SystemMaterializer(system).materializer

    uniqueLocalAddress = UniqueAddress(
      system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress,
      system.asInstanceOf[ExtendedActorSystem].uid)

    val actorOnSystemA = system.actorOf(Props.empty, "a")
    senderStringA = actorOnSystemA.path.toSerializationFormatWithAddress(uniqueLocalAddress.address)

    systemB.actorOf(Props.empty, "b")
    val addressB = systemB.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    val rootB = RootActorPath(addressB)
    remoteRefB = Await
      .result(system.actorSelection(rootB / "user" / "b").resolveOne(5.seconds), 5.seconds)
      .asInstanceOf[RemoteActorRef]
    resolvedRef = actorOnSystemA.asInstanceOf[InternalActorRef]
    recipientStringB = remoteRefB.path.toSerializationFormatWithAddress(addressB)

    val remoteInstruments: RemoteInstruments = if (configType == RemoteInstrument) {
      new RemoteInstruments(system.asInstanceOf[ExtendedActorSystem], system.log, Vector(new DummyRemoteInstrument()))
    } else null
    val envelope = new EnvelopeBuffer(envelopeTemplateBuffer)
    val outboundEnvelope = OutboundEnvelope(OptionVal.None, payload, OptionVal.None)
    headerIn.setVersion(ArteryTransport.HighestVersion)
    headerIn.setUid(42)
    headerIn.setSenderActorRef(actorOnSystemA)
    headerIn.setRecipientActorRef(remoteRefB)
    headerIn.setManifest("")
    headerIn.setRemoteInstruments(remoteInstruments)
    MessageSerializer.serializeForArtery(SerializationExtension(system), outboundEnvelope, headerIn, envelope)
    envelope.byteBuffer.flip()

    // Now build up the graphs
    val encoder: Flow[OutboundEnvelope, EnvelopeBuffer, Encoder.OutboundCompressionAccess] =
      Flow.fromGraph(
        new Encoder(
          uniqueLocalAddress,
          system.asInstanceOf[ExtendedActorSystem],
          outboundEnvelopePool,
          envelopePool,
          streamId = 1,
          debugLogSend = false,
          version = ArteryTransport.HighestVersion))
    val encoderInput: Flow[String, OutboundEnvelope, NotUsed] =
      Flow[String].map(_ => outboundEnvelopePool.acquire().init(OptionVal.None, payload, OptionVal.Some(remoteRefB)))
    val compressions = new InboundCompressionsImpl(system, inboundContext, inboundContext.settings.Advanced.Compression)
    val decoder: Flow[EnvelopeBuffer, InboundEnvelope, InboundCompressionAccess] =
      Flow.fromGraph(
        new Decoder(
          inboundContext,
          system.asInstanceOf[ExtendedActorSystem],
          uniqueLocalAddress,
          inboundContext.settings,
          compressions,
          inboundEnvelopePool))
    val deserializer: Flow[InboundEnvelope, InboundEnvelope, NotUsed] =
      Flow.fromGraph(new Deserializer(inboundContext, system.asInstanceOf[ExtendedActorSystem], envelopePool))
    val decoderInput: Flow[String, EnvelopeBuffer, NotUsed] = Flow[String].map { _ =>
      val envelope = envelopePool.acquire()
      envelopeTemplateBuffer.rewind()
      envelope.byteBuffer.put(envelopeTemplateBuffer)
      envelope.byteBuffer.flip()
      envelope
    }

    encodeGraph = encoderInput.via(encoder).map(envelope => envelopePool.release(envelope))

    decodeGraph = decoderInput.via(decoder).via(deserializer).map {
      case env: ReusableInboundEnvelope => inboundEnvelopePool.release(env)
      case _                            =>
    }

    encodeDecodeGraph = encoderInput.via(encoder).via(decoder).via(deserializer).map {
      case env: ReusableInboundEnvelope => inboundEnvelopePool.release(env)
      case _                            =>
    }
  }

  @TearDown(Level.Trial)
  def tearDownTrial(): Unit = {
    Await.result(system.terminate(), 5.seconds)
    Await.result(systemB.terminate(), 5.seconds)
  }

  @Setup(Level.Iteration)
  def setupIteration(): Unit = {
    System.gc()
  }

  @TearDown(Level.Iteration)
  def tearDownIteration(): Unit = {}

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def reference(): Unit = {
    val latch = new CountDownLatch(1)
    val N = OperationsPerInvocation

    Source.fromGraph(new BenchTestSourceSameElement(N, "elem")).runWith(new LatchSink(N, latch))

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("Latch didn't complete in time")
  }

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def encode(): Unit = {
    val latch = new CountDownLatch(1)
    val N = OperationsPerInvocation

    Source.fromGraph(new BenchTestSourceSameElement(N, "elem")).via(encodeGraph).runWith(new LatchSink(N, latch))

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("Latch didn't complete in time")
  }

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def decode(): Unit = {
    val latch = new CountDownLatch(1)
    val N = OperationsPerInvocation

    Source.fromGraph(new BenchTestSourceSameElement(N, "elem")).via(decodeGraph).runWith(new LatchSink(N, latch))

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("Latch didn't complete in time")
  }

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def both(): Unit = {
    val latch = new CountDownLatch(1)
    val N = OperationsPerInvocation

    Source.fromGraph(new BenchTestSourceSameElement(N, "elem")).via(encodeDecodeGraph).runWith(new LatchSink(N, latch))

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("Latch didn't complete in time")
  }

}

object CodecBenchmark {
  // Configurations
  final val Standard = "Standard"
  final val RemoteInstrument = "RemoteInstrument"

  // How many iterations between materializations
  final val OperationsPerInvocation = 1000000

  // DummyMessage and serailizer that doesn't consume bytes during serialization/deserialization
  val DummyMessageInstance = new DummyMessage
  class DummyMessage

  class DummyMessageSerializer(val system: ExtendedActorSystem) extends BaseSerializer with ByteBufferSerializer {
    private val TheMagicConstant: Byte = 47
    private val Preserialized = {
      val buf = ByteBuffer.allocate(100)
      buf.put(TheMagicConstant)
      buf.flip()
      buf.array()
    }

    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = buf.put(TheMagicConstant)

    override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = {
      val b = buf.get()
      if (b == TheMagicConstant)
        DummyMessageInstance
      else
        throw new IOException(s"DummyMessage deserialization error. Expected $TheMagicConstant got $b")
    }

    override def toBinary(o: AnyRef): Array[Byte] = Preserialized

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef =
      fromBinary(ByteBuffer.wrap(bytes), "NoManifestForYou")
  }

  // DummyRemoteInstrument that doesn't allocate unnecessary bytes during serialization/deserialization
  class DummyRemoteInstrument extends RemoteInstrument {
    private val Metadata = "slevin".getBytes

    override def identifier: Byte = 7 // Lucky number slevin

    override def remoteWriteMetadata(
        recipient: ActorRef,
        message: Object,
        sender: ActorRef,
        buffer: ByteBuffer): Unit = {
      buffer.putInt(Metadata.length)
      buffer.put(Metadata)
    }

    override def remoteReadMetadata(
        recipient: ActorRef,
        message: Object,
        sender: ActorRef,
        buffer: ByteBuffer): Unit = {
      val length = Metadata.length
      val metaLength = buffer.getInt
      @tailrec
      def compare(pos: Int): Boolean = {
        if (pos == length) true
        else if (Metadata(pos) == buffer.get()) compare(pos + 1)
        else false
      }
      if (metaLength != length || !compare(0))
        throw new IOException(s"DummyInstrument deserialization error. Expected ${Metadata.toString}")
    }

    override def remoteMessageSent(
        recipient: ActorRef,
        message: Object,
        sender: ActorRef,
        size: Int,
        time: Long): Unit = ()

    override def remoteMessageReceived(
        recipient: ActorRef,
        message: Object,
        sender: ActorRef,
        size: Int,
        time: Long): Unit = ()
  }
}
