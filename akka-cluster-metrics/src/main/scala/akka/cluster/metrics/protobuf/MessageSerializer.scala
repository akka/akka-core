/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics.protobuf

import java.{ lang => jl }
import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream }
import java.io.NotSerializableException
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }

import scala.annotation.tailrec
import scala.collection.immutable

import akka.actor.{ Address, ExtendedActorSystem }
import akka.cluster.metrics._
import akka.cluster.metrics.protobuf.msg.{ ClusterMetricsMessages => cm }
import akka.dispatch.Dispatchers
import akka.protobufv3.internal.MessageLite
import akka.remote.ByteStringUtils
import akka.serialization.{ BaseSerializer, SerializationExtension, SerializerWithStringManifest, Serializers }
import akka.util.ClassLoaderObjectInputStream
import scala.jdk.CollectionConverters._

import akka.serialization.DisabledJavaSerializer

/**
 * Protobuf serializer for [[akka.cluster.metrics.ClusterMetricsMessage]] types.
 */
class MessageSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest with BaseSerializer {

  private final val BufferSize = 4 * 1024

  private val MetricsGossipEnvelopeManifest = "a"
  private val AdaptiveLoadBalancingPoolManifest = "b"
  private val MixMetricsSelectorManifest = "c"
  private val CpuMetricsSelectorManifest = "d"
  private val HeapMetricsSelectorManifest = "e"
  private val SystemLoadAverageMetricsSelectorManifest = "f"

  private lazy val serialization = SerializationExtension(system)

  override def manifest(obj: AnyRef): String = obj match {
    case _: MetricsGossipEnvelope         => MetricsGossipEnvelopeManifest
    case _: AdaptiveLoadBalancingPool     => AdaptiveLoadBalancingPoolManifest
    case _: MixMetricsSelector            => MixMetricsSelectorManifest
    case CpuMetricsSelector               => CpuMetricsSelectorManifest
    case HeapMetricsSelector              => HeapMetricsSelectorManifest
    case SystemLoadAverageMetricsSelector => SystemLoadAverageMetricsSelectorManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case m: MetricsGossipEnvelope         => compress(metricsGossipEnvelopeToProto(m))
    case alb: AdaptiveLoadBalancingPool   => adaptiveLoadBalancingPoolToBinary(alb)
    case mms: MixMetricsSelector          => mixMetricSelectorToBinary(mms)
    case CpuMetricsSelector               => Array.emptyByteArray
    case HeapMetricsSelector              => Array.emptyByteArray
    case SystemLoadAverageMetricsSelector => Array.emptyByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  def compress(msg: MessageLite): Array[Byte] = {
    val bos = new ByteArrayOutputStream(BufferSize)
    val zip = new GZIPOutputStream(bos)
    try msg.writeTo(zip)
    finally zip.close()
    bos.toByteArray
  }

  def decompress(bytes: Array[Byte]): Array[Byte] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](BufferSize)

    @tailrec def readChunk(): Unit = in.read(buffer) match {
      case -1 => ()
      case n =>
        out.write(buffer, 0, n)
        readChunk()
    }

    try readChunk()
    finally in.close()
    out.toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case MetricsGossipEnvelopeManifest            => metricsGossipEnvelopeFromBinary(bytes)
    case AdaptiveLoadBalancingPoolManifest        => adaptiveLoadBalancingPoolFromBinary(bytes)
    case MixMetricsSelectorManifest               => mixMetricSelectorFromBinary(bytes)
    case CpuMetricsSelectorManifest               => CpuMetricsSelector
    case HeapMetricsSelectorManifest              => HeapMetricsSelector
    case SystemLoadAverageMetricsSelectorManifest => SystemLoadAverageMetricsSelector
    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}")
  }

  private def addressToProto(address: Address): cm.Address.Builder = address match {
    case Address(protocol, actorSystem, Some(host), Some(port)) =>
      cm.Address.newBuilder().setSystem(actorSystem).setHostname(host).setPort(port).setProtocol(protocol)
    case _ => throw new IllegalArgumentException(s"Address [$address] could not be serialized: host or port missing.")
  }

  def adaptiveLoadBalancingPoolToBinary(alb: AdaptiveLoadBalancingPool): Array[Byte] = {
    val builder = cm.AdaptiveLoadBalancingPool.newBuilder()
    if (alb.metricsSelector != MixMetricsSelector) {
      builder.setMetricsSelector(metricsSelectorToProto(alb.metricsSelector))
    }
    if (alb.routerDispatcher != Dispatchers.DefaultDispatcherId) {
      builder.setRouterDispatcher(alb.routerDispatcher)
    }
    builder.setNrOfInstances(alb.nrOfInstances)
    builder.setUsePoolDispatcher(alb.usePoolDispatcher)

    builder.build().toByteArray
  }

  private def metricsSelectorToProto(selector: MetricsSelector): cm.MetricsSelector = {
    val builder = cm.MetricsSelector.newBuilder()
    val serializer = serialization.findSerializerFor(selector)

    builder
      .setData(ByteStringUtils.toProtoByteStringUnsafe(serializer.toBinary(selector)))
      .setSerializerId(serializer.identifier)

    val manifest = Serializers.manifestFor(serializer, selector)
    builder.setManifest(manifest)

    builder.build()
  }

  private def mixMetricSelectorToBinary(mms: MixMetricsSelector): Array[Byte] = {
    val builder = cm.MixMetricsSelector.newBuilder()
    mms.selectors.foreach { selector =>
      builder.addSelectors(metricsSelectorToProto(selector))
    }
    builder.build().toByteArray
  }

  @volatile
  private var protocolCache: String = null
  @volatile
  private var systemCache: String = null

  private def getProtocol(address: cm.Address): String = {
    val p = address.getProtocol
    val pc = protocolCache
    if (pc == p) pc
    else {
      protocolCache = p
      p
    }
  }

  private def getSystem(address: cm.Address): String = {
    val s = address.getSystem
    val sc = systemCache
    if (sc == s) sc
    else {
      systemCache = s
      s
    }
  }

  private def addressFromProto(address: cm.Address): Address =
    Address(getProtocol(address), getSystem(address), address.getHostname, address.getPort)

  private def mapWithErrorMessage[T](map: Map[T, Int], value: T, unknown: String): Int = map.get(value) match {
    case Some(x) => x
    case _       => throw new IllegalArgumentException(s"Unknown $unknown [$value] in cluster message")
  }

  private def metricsGossipEnvelopeToProto(envelope: MetricsGossipEnvelope): cm.MetricsGossipEnvelope = {
    val allNodeMetrics = envelope.gossip.nodes
    val allAddresses: Vector[Address] = allNodeMetrics.iterator.map(_.address).to(immutable.Vector)
    val addressMapping = allAddresses.zipWithIndex.toMap
    val allMetricNames: Vector[String] =
      allNodeMetrics.foldLeft(Set.empty[String])((s, n) => s ++ n.metrics.iterator.map(_.name)).toVector
    val metricNamesMapping = allMetricNames.zipWithIndex.toMap
    def mapAddress(address: Address) = mapWithErrorMessage(addressMapping, address, "address")
    def mapName(name: String) = mapWithErrorMessage(metricNamesMapping, name, "address")

    def ewmaToProto(ewma: Option[EWMA]): Option[cm.NodeMetrics.EWMA.Builder] = ewma.map { x =>
      cm.NodeMetrics.EWMA.newBuilder().setValue(x.value).setAlpha(x.alpha)
    }

    @tailrec def numberToProto(number: Number): cm.NodeMetrics.Number.Builder = {
      import cm.NodeMetrics.Number
      import cm.NodeMetrics.NumberType
      number match {
        case n: jl.Double  => Number.newBuilder().setType(NumberType.Double).setValue64(jl.Double.doubleToLongBits(n))
        case n: jl.Long    => Number.newBuilder().setType(NumberType.Long).setValue64(n)
        case n: jl.Float   => Number.newBuilder().setType(NumberType.Float).setValue32(jl.Float.floatToIntBits(n))
        case n: jl.Integer => Number.newBuilder().setType(NumberType.Integer).setValue32(n)
        case n: java.math.BigInteger =>
          numberToProto(n.longValue) // this truncation is anyway in MetricNumericConverter
        case n: BigInt => numberToProto(n.toLong) // this truncation is anyway in MetricNumericConverter
        case n: java.math.BigDecimal =>
          numberToProto(n.doubleValue) // this rounding is anyway in MetricNumericConverter
        case n: BigDecimal => numberToProto(n.toDouble) // this rounding is anyway in MetricNumericConverter
        case _ =>
          if (system.settings.AllowJavaSerialization) {
            val bos = new ByteArrayOutputStream
            val out = new ObjectOutputStream(bos)
            out.writeObject(number)
            out.close()
            Number
              .newBuilder()
              .setType(NumberType.Serialized)
              .setSerialized(ByteStringUtils.toProtoByteStringUnsafe(bos.toByteArray))
          } else {
            // this is the default, and it shouldn't happen since all number types should be covered above
            throw throw new DisabledJavaSerializer.JavaSerializationException(
              s"Unsupported number [${number.getClass.getName}], when Java serialization is disabled")
          }
      }
    }

    def metricToProto(metric: Metric): cm.NodeMetrics.Metric.Builder = {
      val builder =
        cm.NodeMetrics.Metric.newBuilder().setNameIndex(mapName(metric.name)).setNumber(numberToProto(metric.value))
      ewmaToProto(metric.average).map(builder.setEwma).getOrElse(builder)
    }

    def nodeMetricsToProto(nodeMetrics: NodeMetrics): cm.NodeMetrics.Builder =
      cm.NodeMetrics
        .newBuilder()
        .setAddressIndex(mapAddress(nodeMetrics.address))
        .setTimestamp(nodeMetrics.timestamp)
        .addAllMetrics(nodeMetrics.metrics.map(metricToProto(_).build).asJava)

    val nodeMetrics: Iterable[cm.NodeMetrics] = allNodeMetrics.map(nodeMetricsToProto(_).build)

    cm.MetricsGossipEnvelope
      .newBuilder()
      .setFrom(addressToProto(envelope.from))
      .setGossip(
        cm.MetricsGossip
          .newBuilder()
          .addAllAllAddresses(allAddresses.map(addressToProto(_).build()).asJava)
          .addAllAllMetricNames(allMetricNames.asJava)
          .addAllNodeMetrics(nodeMetrics.asJava))
      .setReply(envelope.reply)
      .build
  }

  private def metricsGossipEnvelopeFromBinary(bytes: Array[Byte]): MetricsGossipEnvelope =
    metricsGossipEnvelopeFromProto(cm.MetricsGossipEnvelope.parseFrom(decompress(bytes)))

  private def metricsGossipEnvelopeFromProto(envelope: cm.MetricsGossipEnvelope): MetricsGossipEnvelope = {
    val mgossip = envelope.getGossip
    val addressMapping: Vector[Address] = mgossip.getAllAddressesList.asScala.iterator.map(addressFromProto).to(Vector)
    val metricNameMapping: Vector[String] = mgossip.getAllMetricNamesList.asScala.toVector

    def ewmaFromProto(ewma: cm.NodeMetrics.EWMA): Option[EWMA] =
      Some(EWMA(ewma.getValue, ewma.getAlpha))

    def numberFromProto(number: cm.NodeMetrics.Number): Number = {
      import cm.NodeMetrics.NumberType
      number.getType.getNumber match {
        case NumberType.Double_VALUE  => jl.Double.longBitsToDouble(number.getValue64)
        case NumberType.Long_VALUE    => number.getValue64
        case NumberType.Float_VALUE   => jl.Float.intBitsToFloat(number.getValue32)
        case NumberType.Integer_VALUE => number.getValue32
        case NumberType.Serialized_VALUE =>
          if (system.settings.AllowJavaSerialization) {
            val in = new ClassLoaderObjectInputStream(
              system.dynamicAccess.classLoader,
              new ByteArrayInputStream(number.getSerialized.toByteArray))
            val obj = in.readObject
            in.close()
            obj.asInstanceOf[jl.Number]
          } else {
            // this is the default, and it shouldn't happen since all number types should be covered above
            throw throw new DisabledJavaSerializer.JavaSerializationException(
              s"Unsupported number [${number.getClass.getName}], when Java serialization is disabled")
          }
      }
    }

    def metricFromProto(metric: cm.NodeMetrics.Metric): Metric =
      Metric(
        metricNameMapping(metric.getNameIndex),
        numberFromProto(metric.getNumber),
        if (metric.hasEwma) ewmaFromProto(metric.getEwma) else None)

    def nodeMetricsFromProto(nodeMetrics: cm.NodeMetrics): NodeMetrics =
      NodeMetrics(
        addressMapping(nodeMetrics.getAddressIndex),
        nodeMetrics.getTimestamp,
        nodeMetrics.getMetricsList.asScala.iterator.map(metricFromProto).to(immutable.Set))

    val nodeMetrics: Set[NodeMetrics] =
      mgossip.getNodeMetricsList.asScala.iterator.map(nodeMetricsFromProto).to(immutable.Set)

    MetricsGossipEnvelope(addressFromProto(envelope.getFrom), MetricsGossip(nodeMetrics), envelope.getReply)
  }

  def adaptiveLoadBalancingPoolFromBinary(bytes: Array[Byte]): AdaptiveLoadBalancingPool = {
    val alb = cm.AdaptiveLoadBalancingPool.parseFrom(bytes)

    val selector =
      if (alb.hasMetricsSelector) {
        val ms = alb.getMetricsSelector
        serialization
          .deserialize(ms.getData.toByteArray, ms.getSerializerId, ms.getManifest)
          .get
          .asInstanceOf[MetricsSelector]
      } else MixMetricsSelector

    AdaptiveLoadBalancingPool(
      metricsSelector = selector,
      nrOfInstances = alb.getNrOfInstances,
      routerDispatcher =
        if (alb.hasRouterDispatcher) alb.getRouterDispatcher
        else Dispatchers.DefaultDispatcherId,
      usePoolDispatcher = alb.getUsePoolDispatcher)
  }

  def mixMetricSelectorFromBinary(bytes: Array[Byte]): MixMetricsSelector = {
    val mm = cm.MixMetricsSelector.parseFrom(bytes)
    MixMetricsSelector(
      mm.getSelectorsList.asScala
      // should be safe because we serialized only the right subtypes of MetricsSelector
        .map(s => metricSelectorFromProto(s).asInstanceOf[CapacityMetricsSelector])
        .toIndexedSeq)
  }

  def metricSelectorFromProto(selector: cm.MetricsSelector): MetricsSelector =
    serialization
      .deserialize(selector.getData.toByteArray, selector.getSerializerId, selector.getManifest)
      .get
      .asInstanceOf[MetricsSelector]

}
