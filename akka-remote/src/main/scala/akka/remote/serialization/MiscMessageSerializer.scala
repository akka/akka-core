/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{ FiniteDuration, TimeUnit }

import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }

import akka.{ Done, NotUsed }
import akka.actor._
import akka.dispatch.Dispatchers
import akka.pattern.StatusReply
import akka.remote._
import akka.remote.WireFormats.AddressData
import akka.remote.routing.RemoteRouterConfig
import akka.routing._
import akka.serialization.{ BaseSerializer, Serialization, SerializationExtension, SerializerWithStringManifest }
import scala.jdk.CollectionConverters._

class MiscMessageSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest with BaseSerializer {

  // WARNING! This must lazy otherwise it will deadlock the ActorSystem creation
  private lazy val serialization = SerializationExtension(system)
  private val payloadSupport = new WrappedPayloadSupport(system)
  private val throwableSupport = new ThrowableSupport(system)

  private val ParameterlessSerializedMessage = Array.empty[Byte]
  private val EmptyConfig = ConfigFactory.empty()

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case identify: Identify                    => serializeIdentify(identify)
    case identity: ActorIdentity               => serializeActorIdentity(identity)
    case Some(value)                           => serializeSome(value)
    case None                                  => ParameterlessSerializedMessage
    case o: Optional[_]                        => serializeOptional(o)
    case r: ActorRef                           => serializeActorRef(r)
    case s: Status.Success                     => serializeStatusSuccess(s)
    case f: Status.Failure                     => serializeStatusFailure(f)
    case StatusReply.Ack                       => Array.emptyByteArray
    case r @ StatusReply.Success(_)            => serializeStatusReplySuccess(r)
    case r @ StatusReply.Error(_)              => serializeStatusReplyError(r)
    case ex: ActorInitializationException      => serializeActorInitializationException(ex)
    case ex: ThrowableNotSerializableException => serializeThrowableNotSerializableException(ex)
    case t: Throwable                          => throwableSupport.serializeThrowable(t)
    case PoisonPill                            => ParameterlessSerializedMessage
    case Kill                                  => ParameterlessSerializedMessage
    case RemoteWatcher.Heartbeat               => ParameterlessSerializedMessage
    case Done                                  => ParameterlessSerializedMessage
    case NotUsed                               => ParameterlessSerializedMessage
    case hbrsp: RemoteWatcher.HeartbeatRsp     => serializeHeartbeatRsp(hbrsp)
    case rs: RemoteScope                       => serializeRemoteScope(rs)
    case LocalScope                            => ParameterlessSerializedMessage
    case a: Address                            => serializeAddressData(a)
    case u: UniqueAddress                      => serializeClassicUniqueAddress(u)
    case c: Config                             => serializeConfig(c)
    case dr: DefaultResizer                    => serializeDefaultResizer(dr)
    case fc: FromConfig                        => serializeFromConfig(fc)
    case bp: BalancingPool                     => serializeBalancingPool(bp)
    case bp: BroadcastPool                     => serializeBroadcastPool(bp)
    case rp: RandomPool                        => serializeRandomPool(rp)
    case rrp: RoundRobinPool                   => serializeRoundRobinPool(rrp)
    case sgp: ScatterGatherFirstCompletedPool  => serializeScatterGatherFirstCompletedPool(sgp)
    case tp: TailChoppingPool                  => serializeTailChoppingPool(tp)
    case rrc: RemoteRouterConfig               => serializeRemoteRouterConfig(rrc)
    case _                                     => throw new IllegalArgumentException(s"Cannot serialize object of type [${obj.getClass.getName}]")
  }

  private def serializeIdentify(identify: Identify): Array[Byte] =
    ContainerFormats.Identify
      .newBuilder()
      .setMessageId(payloadSupport.payloadBuilder(identify.messageId))
      .build()
      .toByteArray

  private def serializeActorIdentity(actorIdentity: ActorIdentity): Array[Byte] = {
    val builder =
      ContainerFormats.ActorIdentity
        .newBuilder()
        .setCorrelationId(payloadSupport.payloadBuilder(actorIdentity.correlationId))

    actorIdentity.ref.foreach { actorRef =>
      builder.setRef(actorRefBuilder(actorRef))
    }

    builder.build().toByteArray
  }

  private def serializeSome(someValue: Any): Array[Byte] =
    ContainerFormats.Option.newBuilder().setValue(payloadSupport.payloadBuilder(someValue)).build().toByteArray

  private def serializeOptional(opt: Optional[_]): Array[Byte] = {
    if (opt.isPresent)
      ContainerFormats.Option.newBuilder().setValue(payloadSupport.payloadBuilder(opt.get)).build().toByteArray
    else
      ParameterlessSerializedMessage
  }

  private def serializeActorRef(ref: ActorRef): Array[Byte] =
    actorRefBuilder(ref).build().toByteArray

  private def serializeHeartbeatRsp(hbrsp: RemoteWatcher.HeartbeatRsp): Array[Byte] = {
    ContainerFormats.WatcherHeartbeatResponse.newBuilder().setUid(hbrsp.addressUid).build().toByteArray
  }

  private def serializeRemoteScope(rs: RemoteScope): Array[Byte] = {
    val builder = WireFormats.RemoteScope.newBuilder()
    builder.setNode(buildAddressData(rs.node))
    builder.build().toByteArray
  }

  private def actorRefBuilder(actorRef: ActorRef): ContainerFormats.ActorRef.Builder =
    ContainerFormats.ActorRef.newBuilder().setPath(Serialization.serializedActorPath(actorRef))

  private def serializeStatusSuccess(success: Status.Success): Array[Byte] =
    payloadSupport.payloadBuilder(success.status).build().toByteArray

  private def serializeStatusFailure(failure: Status.Failure): Array[Byte] =
    payloadSupport.payloadBuilder(failure.cause).build().toByteArray

  def serializeStatusReplySuccess(r: StatusReply[Any]): Array[Byte] =
    // no specific message, serialized id and manifest together with payload is enough (no wrapping overhead)
    payloadSupport.payloadBuilder(r.getValue).build().toByteArray

  def serializeStatusReplyError(r: StatusReply[_]): Array[Byte] = {
    r.getError match {
      case em: StatusReply.ErrorMessage =>
        // somewhat optimized for the recommended usage, avoiding the additional payload metadata
        ContainerFormats.StatusReplyErrorMessage.newBuilder().setErrorMessage(em.getMessage).build().toByteArray
      case ex: Throwable =>
        // depends on user providing exception serializer
        // no specific message, serialized id and manifest together with payload is enough (less wrapping overhead)
        payloadSupport.payloadBuilder(ex).build().toByteArray
    }
  }

  private def serializeActorInitializationException(ex: ActorInitializationException): Array[Byte] = {
    val builder = ContainerFormats.ActorInitializationException.newBuilder()
    if (ex.getActor ne null)
      builder.setActor(actorRefBuilder(ex.getActor))
    builder.setMessage(ex.getMessage).setCause(payloadSupport.payloadBuilder(ex.getCause)).build().toByteArray
  }

  private def serializeThrowableNotSerializableException(ex: ThrowableNotSerializableException): Array[Byte] = {
    val builder = ContainerFormats.ThrowableNotSerializable.newBuilder()
    builder.setMessage(ex.getMessage)
    if (ex.originalMessage eq null)
      builder.setOriginalMessage("")
    else
      builder.setOriginalMessage(ex.originalMessage)
    builder.setOriginalClassName(ex.originalClassName)
    builder.build().toByteArray
  }

  private def serializeConfig(c: Config): Array[Byte] = {
    c.root.render(ConfigRenderOptions.concise()).getBytes(StandardCharsets.UTF_8)
  }

  private def protoForAddressData(address: Address): AddressData.Builder =
    address match {
      case Address(protocol, actorSystem, Some(host), Some(port)) =>
        WireFormats.AddressData
          .newBuilder()
          .setSystem(actorSystem)
          .setHostname(host)
          .setPort(port)
          .setProtocol(protocol)
      case _ => throw new IllegalArgumentException(s"Address [$address] could not be serialized: host or port missing.")
    }
  private def protoForAddress(address: Address): ArteryControlFormats.Address.Builder =
    address match {
      case Address(protocol, actorSystem, Some(host), Some(port)) =>
        ArteryControlFormats.Address
          .newBuilder()
          .setSystem(actorSystem)
          .setHostname(host)
          .setPort(port)
          .setProtocol(protocol)
      case _ => throw new IllegalArgumentException(s"Address [$address] could not be serialized: host or port missing.")
    }
  private def serializeAddressData(address: Address): Array[Byte] =
    protoForAddressData(address).build().toByteArray

  private def serializeClassicUniqueAddress(uniqueAddress: UniqueAddress): Array[Byte] =
    ArteryControlFormats.UniqueAddress
      .newBuilder()
      .setUid(uniqueAddress.uid)
      .setAddress(protoForAddress(uniqueAddress.address))
      .build()
      .toByteArray

  private def serializeDefaultResizer(dr: DefaultResizer): Array[Byte] = {
    val builder = WireFormats.DefaultResizer.newBuilder()
    builder.setBackoffRate(dr.backoffRate)
    builder.setBackoffThreshold(dr.backoffThreshold)
    builder.setLowerBound(dr.lowerBound)
    builder.setMessagesPerResize(dr.messagesPerResize)
    builder.setPressureThreshold(dr.pressureThreshold)
    builder.setRampupRate(dr.rampupRate)
    builder.setUpperBound(dr.upperBound)
    builder.build().toByteArray
  }

  private def serializeFromConfig(fc: FromConfig): Array[Byte] =
    if (fc == FromConfig) ParameterlessSerializedMessage
    else {
      val builder = WireFormats.FromConfig.newBuilder()
      if (fc.resizer.isDefined)
        builder.setResizer(payloadSupport.payloadBuilder(fc.resizer.get))
      if (fc.routerDispatcher != Dispatchers.DefaultDispatcherId)
        builder.setRouterDispatcher(fc.routerDispatcher)
      builder.build().toByteArray
    }

  private def serializeBalancingPool(bp: BalancingPool): Array[Byte] = {
    buildGenericRoutingPool(bp.nrOfInstances, bp.routerDispatcher, bp.usePoolDispatcher, bp.resizer).toByteArray
  }

  private def serializeBroadcastPool(bp: BroadcastPool): Array[Byte] = {
    buildGenericRoutingPool(bp.nrOfInstances, bp.routerDispatcher, bp.usePoolDispatcher, bp.resizer).toByteArray
  }

  private def serializeRandomPool(rp: RandomPool): Array[Byte] = {
    buildGenericRoutingPool(rp.nrOfInstances, rp.routerDispatcher, rp.usePoolDispatcher, rp.resizer).toByteArray
  }

  private def serializeRoundRobinPool(rp: RoundRobinPool): Array[Byte] = {
    buildGenericRoutingPool(rp.nrOfInstances, rp.routerDispatcher, rp.usePoolDispatcher, rp.resizer).toByteArray
  }

  private def serializeScatterGatherFirstCompletedPool(sgp: ScatterGatherFirstCompletedPool): Array[Byte] = {
    val builder = WireFormats.ScatterGatherPool.newBuilder()
    builder.setGeneric(
      buildGenericRoutingPool(sgp.nrOfInstances, sgp.routerDispatcher, sgp.usePoolDispatcher, sgp.resizer))
    builder.setWithin(buildFiniteDuration(sgp.within))
    builder.build().toByteArray
  }

  private def serializeTailChoppingPool(tp: TailChoppingPool): Array[Byte] = {
    val builder = WireFormats.TailChoppingPool.newBuilder()
    builder.setGeneric(buildGenericRoutingPool(tp.nrOfInstances, tp.routerDispatcher, tp.usePoolDispatcher, tp.resizer))
    builder.setWithin(buildFiniteDuration(tp.within))
    builder.setInterval(buildFiniteDuration(tp.interval))
    builder.build().toByteArray
  }

  private def serializeRemoteRouterConfig(rrc: RemoteRouterConfig): Array[Byte] = {
    val builder = WireFormats.RemoteRouterConfig.newBuilder()
    builder.setLocal(payloadSupport.payloadBuilder(rrc.local).build())
    builder.addAllNodes(rrc.nodes.map(buildAddressData).asJava)
    builder.build().toByteArray
  }

  private def buildGenericRoutingPool(
      nrOfInstances: Int,
      routerDispatcher: String,
      usePoolDispatcher: Boolean,
      resizer: Option[Resizer]): WireFormats.GenericRoutingPool = {
    val builder = WireFormats.GenericRoutingPool.newBuilder()
    builder.setNrOfInstances(nrOfInstances)
    if (routerDispatcher != Dispatchers.DefaultDispatcherId) {
      builder.setRouterDispatcher(routerDispatcher)
    }
    if (resizer.isDefined) {
      builder.setResizer(payloadSupport.payloadBuilder(resizer.get))
    }
    builder.setUsePoolDispatcher(usePoolDispatcher)
    builder.build()
  }

  private def timeUnitToWire(unit: TimeUnit): WireFormats.TimeUnit = unit match {
    case TimeUnit.NANOSECONDS  => WireFormats.TimeUnit.NANOSECONDS
    case TimeUnit.MICROSECONDS => WireFormats.TimeUnit.MICROSECONDS
    case TimeUnit.MILLISECONDS => WireFormats.TimeUnit.MILLISECONDS
    case TimeUnit.SECONDS      => WireFormats.TimeUnit.SECONDS
    case TimeUnit.MINUTES      => WireFormats.TimeUnit.MINUTES
    case TimeUnit.HOURS        => WireFormats.TimeUnit.HOURS
    case TimeUnit.DAYS         => WireFormats.TimeUnit.DAYS
  }

  private def buildFiniteDuration(duration: FiniteDuration): WireFormats.FiniteDuration = {
    WireFormats.FiniteDuration.newBuilder().setValue(duration.length).setUnit(timeUnitToWire(duration.unit)).build()
  }

  private def buildAddressData(address: Address): WireFormats.AddressData = {
    val builder = WireFormats.AddressData.newBuilder()
    address match {
      case Address(protocol, system, Some(host), Some(port)) =>
        builder.setProtocol(protocol)
        builder.setSystem(system)
        builder.setHostname(host)
        builder.setPort(port)
        builder.build()

      case _ => throw new IllegalArgumentException(s"Address [$address] could not be serialized: host or port missing.")
    }
  }

  private val IdentifyManifest = "A"
  private val ActorIdentityManifest = "B"
  private val OptionManifest = "C"
  private val StatusSuccessManifest = "D"
  private val StatusFailureManifest = "E"
  private val ThrowableManifest = "F"
  private val ActorRefManifest = "G"
  private val OptionalManifest = "H"
  private val PoisonPillManifest = "P"
  private val KillManifest = "K"
  private val RemoteWatcherHBManifest = "RWHB"
  private val DoneManifest = "DONE"
  private val NotUsedManifest = "NU"
  private val AddressManifest = "AD"
  private val UniqueAddressManifest = "UD"
  private val RemoteWatcherHBRespManifest = "RWHR"
  private val ActorInitializationExceptionManifest = "AIEX"
  private val ThrowableNotSerializableExceptionManifest = "TNSEX"
  private val LocalScopeManifest = "LS"
  private val RemoteScopeManifest = "RS"
  private val ConfigManifest = "CF"
  private val FromConfigManifest = "FC"
  private val DefaultResizerManifest = "DR"
  private val BalancingPoolManifest = "ROBAP"
  private val BroadcastPoolManifest = "ROBP"
  private val RandomPoolManifest = "RORP"
  private val RoundRobinPoolManifest = "RORRP"
  private val ScatterGatherPoolManifest = "ROSGP"
  private val TailChoppingPoolManifest = "ROTCP"
  private val RemoteRouterConfigManifest = "RORRC"
  private val StatusReplySuccessManifest = "S"
  private val StatusReplyErrorMessageManifest = "SM"
  private val StatusReplyErrorExceptionManifest = "SE"
  private val StatusReplyAckManifest = "SA"

  private val fromBinaryMap = Map[String, Array[Byte] => AnyRef](
    IdentifyManifest -> deserializeIdentify,
    ActorIdentityManifest -> deserializeActorIdentity,
    StatusSuccessManifest -> deserializeStatusSuccess,
    StatusFailureManifest -> deserializeStatusFailure,
    StatusReplyAckManifest -> ((_) => StatusReply.Ack),
    StatusReplySuccessManifest -> deserializeStatusReplySuccess,
    StatusReplyErrorMessageManifest -> deserializeStatusReplyErrorMessage,
    StatusReplyErrorExceptionManifest -> deserializeStatusReplyErrorException,
    ThrowableManifest -> throwableSupport.deserializeThrowable,
    ActorRefManifest -> deserializeActorRefBytes,
    OptionManifest -> deserializeOption,
    OptionalManifest -> deserializeOptional,
    PoisonPillManifest -> ((_) => PoisonPill),
    KillManifest -> ((_) => Kill),
    RemoteWatcherHBManifest -> ((_) => RemoteWatcher.Heartbeat),
    DoneManifest -> ((_) => Done),
    NotUsedManifest -> ((_) => NotUsed),
    AddressManifest -> deserializeAddressData,
    UniqueAddressManifest -> deserializeUniqueAddress,
    RemoteWatcherHBRespManifest -> deserializeHeartbeatRsp,
    ActorInitializationExceptionManifest -> deserializeActorInitializationException,
    ThrowableNotSerializableExceptionManifest -> deserializeThrowableNotSerializableException,
    LocalScopeManifest -> ((_) => LocalScope),
    RemoteScopeManifest -> deserializeRemoteScope,
    ConfigManifest -> deserializeConfig,
    FromConfigManifest -> deserializeFromConfig,
    DefaultResizerManifest -> deserializeDefaultResizer,
    BalancingPoolManifest -> deserializeBalancingPool,
    BroadcastPoolManifest -> deserializeBroadcastPool,
    RandomPoolManifest -> deserializeRandomPool,
    RoundRobinPoolManifest -> deserializeRoundRobinPool,
    ScatterGatherPoolManifest -> deserializeScatterGatherPool,
    TailChoppingPoolManifest -> deserializeTailChoppingPool,
    RemoteRouterConfigManifest -> deserializeRemoteRouterConfig)

  override def manifest(o: AnyRef): String =
    o match {
      case _: Identify                                    => IdentifyManifest
      case _: ActorIdentity                               => ActorIdentityManifest
      case _: Option[Any]                                 => OptionManifest
      case _: Optional[_]                                 => OptionalManifest
      case _: ActorRef                                    => ActorRefManifest
      case _: Status.Success                              => StatusSuccessManifest
      case _: Status.Failure                              => StatusFailureManifest
      case StatusReply.Ack                                => StatusReplyAckManifest
      case StatusReply.Success(_)                         => StatusReplySuccessManifest
      case StatusReply.Error(_: StatusReply.ErrorMessage) => StatusReplyErrorMessageManifest
      case StatusReply.Error(_)                           => StatusReplyErrorExceptionManifest
      case _: ActorInitializationException                => ActorInitializationExceptionManifest
      case _: ThrowableNotSerializableException           => ThrowableNotSerializableExceptionManifest
      case _: Throwable                                   => ThrowableManifest
      case PoisonPill                                     => PoisonPillManifest
      case Kill                                           => KillManifest
      case RemoteWatcher.Heartbeat                        => RemoteWatcherHBManifest
      case Done                                           => DoneManifest
      case NotUsed                                        => NotUsedManifest
      case _: Address                                     => AddressManifest
      case _: UniqueAddress                               => UniqueAddressManifest
      case _: RemoteWatcher.HeartbeatRsp                  => RemoteWatcherHBRespManifest
      case LocalScope                                     => LocalScopeManifest
      case _: RemoteScope                                 => RemoteScopeManifest
      case _: Config                                      => ConfigManifest
      case _: FromConfig                                  => FromConfigManifest
      case _: DefaultResizer                              => DefaultResizerManifest
      case _: BalancingPool                               => BalancingPoolManifest
      case _: BroadcastPool                               => BroadcastPoolManifest
      case _: RandomPool                                  => RandomPoolManifest
      case _: RoundRobinPool                              => RoundRobinPoolManifest
      case _: ScatterGatherFirstCompletedPool             => ScatterGatherPoolManifest
      case _: TailChoppingPool                            => TailChoppingPoolManifest
      case _: RemoteRouterConfig                          => RemoteRouterConfigManifest

      case _ =>
        throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    fromBinaryMap.get(manifest) match {
      case Some(deserializer) => deserializer(bytes)
      case None =>
        throw new NotSerializableException(
          s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
    }

  private def deserializeIdentify(bytes: Array[Byte]): Identify = {
    val identifyProto = ContainerFormats.Identify.parseFrom(bytes)
    val messageId = payloadSupport.deserializePayload(identifyProto.getMessageId)
    Identify(messageId)
  }

  private def deserializeActorIdentity(bytes: Array[Byte]): ActorIdentity = {
    val actorIdentityProto = ContainerFormats.ActorIdentity.parseFrom(bytes)
    val correlationId = payloadSupport.deserializePayload(actorIdentityProto.getCorrelationId)
    val actorRef =
      if (actorIdentityProto.hasRef)
        Some(deserializeActorRef(actorIdentityProto.getRef))
      else
        None
    ActorIdentity(correlationId, actorRef)
  }

  private def deserializeActorRefBytes(bytes: Array[Byte]): ActorRef =
    deserializeActorRef(ContainerFormats.ActorRef.parseFrom(bytes))

  private def deserializeActorRef(actorRef: ContainerFormats.ActorRef): ActorRef =
    serialization.system.provider.resolveActorRef(actorRef.getPath)

  private def deserializeOption(bytes: Array[Byte]): Option[Any] = {
    if (bytes.length == 0)
      None
    else {
      val optionProto = ContainerFormats.Option.parseFrom(bytes)
      Some(payloadSupport.deserializePayload(optionProto.getValue))
    }
  }

  private def deserializeOptional(bytes: Array[Byte]): Optional[Any] = {
    if (bytes.length == 0)
      Optional.empty()
    else {
      val optionProto = ContainerFormats.Option.parseFrom(bytes)
      Optional.of(payloadSupport.deserializePayload(optionProto.getValue))
    }
  }

  private def deserializeStatusSuccess(bytes: Array[Byte]): Status.Success =
    Status.Success(payloadSupport.deserializePayload(ContainerFormats.Payload.parseFrom(bytes)))

  private def deserializeStatusFailure(bytes: Array[Byte]): Status.Failure =
    Status.Failure(payloadSupport.deserializePayload(ContainerFormats.Payload.parseFrom(bytes)).asInstanceOf[Throwable])

  private def deserializeStatusReplySuccess(bytes: Array[Byte]): StatusReply[_] =
    StatusReply.success(payloadSupport.deserializePayload(ContainerFormats.Payload.parseFrom(bytes)))

  private def deserializeStatusReplyErrorMessage(bytes: Array[Byte]): StatusReply[_] =
    StatusReply.error(ContainerFormats.StatusReplyErrorMessage.parseFrom(bytes).getErrorMessage)

  private def deserializeStatusReplyErrorException(bytes: Array[Byte]): StatusReply[_] =
    StatusReply.error(
      payloadSupport.deserializePayload(ContainerFormats.Payload.parseFrom(bytes)).asInstanceOf[Throwable])

  private def deserializeAddressData(bytes: Array[Byte]): Address =
    addressFromDataProto(WireFormats.AddressData.parseFrom(bytes))

  private def addressFromDataProto(a: WireFormats.AddressData): Address = {
    Address(
      a.getProtocol,
      a.getSystem,
      // technically the presence of hostname and port are guaranteed, see our serializeAddressData
      if (a.hasHostname) Some(a.getHostname) else None,
      if (a.hasPort) Some(a.getPort) else None)
  }
  private def addressFromProto(a: ArteryControlFormats.Address): Address = {
    Address(
      a.getProtocol,
      a.getSystem,
      // technically the presence of hostname and port are guaranteed, see our serializeAddressData
      if (a.hasHostname) Some(a.getHostname) else None,
      if (a.hasPort) Some(a.getPort) else None)
  }

  private def deserializeUniqueAddress(bytes: Array[Byte]): UniqueAddress = {
    val u = ArteryControlFormats.UniqueAddress.parseFrom(bytes)
    UniqueAddress(addressFromProto(u.getAddress), u.getUid)
  }

  private def deserializeHeartbeatRsp(bytes: Array[Byte]): RemoteWatcher.HeartbeatRsp = {
    RemoteWatcher.HeartbeatRsp(ContainerFormats.WatcherHeartbeatResponse.parseFrom(bytes).getUid.toInt)
  }

  private def deserializeActorInitializationException(bytes: Array[Byte]): ActorInitializationException = {
    val serializedEx = ContainerFormats.ActorInitializationException.parseFrom(bytes)
    val ref = deserializeActorRef(serializedEx.getActor)
    val refString = ref.path.toString
    val message = serializedEx.getMessage

    val reconstructedMessage =
      if (message.startsWith(refString)) message.drop(refString.length + 2)
      else message

    ActorInitializationException(
      if (serializedEx.hasActor) ref else null,
      reconstructedMessage,
      payloadSupport.deserializePayload(serializedEx.getCause).asInstanceOf[Throwable])
  }

  private def deserializeThrowableNotSerializableException(bytes: Array[Byte]): ThrowableNotSerializableException = {
    val serializedEx = ContainerFormats.ThrowableNotSerializable.parseFrom(bytes)
    new ThrowableNotSerializableException(serializedEx.getOriginalMessage, serializedEx.getOriginalClassName)
  }

  private def deserializeRemoteScope(bytes: Array[Byte]): RemoteScope = {
    val rs = WireFormats.RemoteScope.parseFrom(bytes)
    RemoteScope(deserializeAddressData(rs.getNode))
  }

  private def deserializeConfig(bytes: Array[Byte]): Config = {
    if (bytes.isEmpty) EmptyConfig
    else ConfigFactory.parseString(new String(bytes, StandardCharsets.UTF_8))
  }

  private def deserializeFromConfig(bytes: Array[Byte]): FromConfig =
    if (bytes.isEmpty) FromConfig
    else {
      val fc = WireFormats.FromConfig.parseFrom(bytes)
      FromConfig(
        resizer =
          if (fc.hasResizer) Some(payloadSupport.deserializePayload(fc.getResizer).asInstanceOf[Resizer]) else None,
        routerDispatcher = if (fc.hasRouterDispatcher) fc.getRouterDispatcher else Dispatchers.DefaultDispatcherId)
    }

  private def deserializeBalancingPool(bytes: Array[Byte]): BalancingPool = {
    val bp = WireFormats.GenericRoutingPool.parseFrom(bytes)
    BalancingPool(
      nrOfInstances = bp.getNrOfInstances,
      routerDispatcher = if (bp.hasRouterDispatcher) bp.getRouterDispatcher else Dispatchers.DefaultDispatcherId)
  }

  private def deserializeBroadcastPool(bytes: Array[Byte]): BroadcastPool = {
    val bp = WireFormats.GenericRoutingPool.parseFrom(bytes)
    BroadcastPool(
      nrOfInstances = bp.getNrOfInstances,
      resizer =
        if (bp.hasResizer) Some(payloadSupport.deserializePayload(bp.getResizer).asInstanceOf[Resizer])
        else None,
      routerDispatcher = if (bp.hasRouterDispatcher) bp.getRouterDispatcher else Dispatchers.DefaultDispatcherId,
      usePoolDispatcher = bp.getUsePoolDispatcher)
  }

  private def deserializeRandomPool(bytes: Array[Byte]): RandomPool = {
    val rp = WireFormats.GenericRoutingPool.parseFrom(bytes)
    RandomPool(
      nrOfInstances = rp.getNrOfInstances,
      resizer =
        if (rp.hasResizer) Some(payloadSupport.deserializePayload(rp.getResizer).asInstanceOf[Resizer])
        else None,
      routerDispatcher = if (rp.hasRouterDispatcher) rp.getRouterDispatcher else Dispatchers.DefaultDispatcherId,
      usePoolDispatcher = rp.getUsePoolDispatcher)
  }

  private def deserializeRoundRobinPool(bytes: Array[Byte]): RoundRobinPool = {
    val rp = WireFormats.GenericRoutingPool.parseFrom(bytes)
    RoundRobinPool(
      nrOfInstances = rp.getNrOfInstances,
      resizer =
        if (rp.hasResizer) Some(payloadSupport.deserializePayload(rp.getResizer).asInstanceOf[Resizer])
        else None,
      routerDispatcher = if (rp.hasRouterDispatcher) rp.getRouterDispatcher else Dispatchers.DefaultDispatcherId,
      usePoolDispatcher = rp.getUsePoolDispatcher)
  }

  private def deserializeScatterGatherPool(bytes: Array[Byte]): ScatterGatherFirstCompletedPool = {
    val sgp = WireFormats.ScatterGatherPool.parseFrom(bytes)
    ScatterGatherFirstCompletedPool(
      nrOfInstances = sgp.getGeneric.getNrOfInstances,
      resizer =
        if (sgp.getGeneric.hasResizer)
          Some(payloadSupport.deserializePayload(sgp.getGeneric.getResizer).asInstanceOf[Resizer])
        else None,
      within = deserializeFiniteDuration(sgp.getWithin),
      routerDispatcher =
        if (sgp.getGeneric.hasRouterDispatcher) sgp.getGeneric.getRouterDispatcher
        else Dispatchers.DefaultDispatcherId)
  }

  private def deserializeTailChoppingPool(bytes: Array[Byte]): TailChoppingPool = {
    val tcp = WireFormats.TailChoppingPool.parseFrom(bytes)
    TailChoppingPool(
      nrOfInstances = tcp.getGeneric.getNrOfInstances,
      resizer =
        if (tcp.getGeneric.hasResizer)
          Some(payloadSupport.deserializePayload(tcp.getGeneric.getResizer).asInstanceOf[Resizer])
        else None,
      routerDispatcher =
        if (tcp.getGeneric.hasRouterDispatcher) tcp.getGeneric.getRouterDispatcher
        else Dispatchers.DefaultDispatcherId,
      usePoolDispatcher = tcp.getGeneric.getUsePoolDispatcher,
      within = deserializeFiniteDuration(tcp.getWithin),
      interval = deserializeFiniteDuration(tcp.getInterval))
  }

  private def deserializeRemoteRouterConfig(bytes: Array[Byte]): RemoteRouterConfig = {
    val rrc = WireFormats.RemoteRouterConfig.parseFrom(bytes)
    RemoteRouterConfig(
      local = payloadSupport.deserializePayload(rrc.getLocal).asInstanceOf[Pool],
      nodes = rrc.getNodesList.asScala.map(deserializeAddressData))
  }

  private def deserializeDefaultResizer(bytes: Array[Byte]): DefaultResizer = {
    val dr = WireFormats.DefaultResizer.parseFrom(bytes)
    DefaultResizer(
      lowerBound = dr.getLowerBound,
      upperBound = dr.getUpperBound,
      pressureThreshold = dr.getPressureThreshold,
      rampupRate = dr.getRampupRate,
      backoffThreshold = dr.getBackoffThreshold,
      backoffRate = dr.getBackoffRate,
      messagesPerResize = dr.getMessagesPerResize)
  }

  private def deserializeTimeUnit(unit: WireFormats.TimeUnit): TimeUnit = unit match {
    case WireFormats.TimeUnit.NANOSECONDS  => TimeUnit.NANOSECONDS
    case WireFormats.TimeUnit.MICROSECONDS => TimeUnit.MICROSECONDS
    case WireFormats.TimeUnit.MILLISECONDS => TimeUnit.MILLISECONDS
    case WireFormats.TimeUnit.SECONDS      => TimeUnit.SECONDS
    case WireFormats.TimeUnit.MINUTES      => TimeUnit.MINUTES
    case WireFormats.TimeUnit.HOURS        => TimeUnit.HOURS
    case WireFormats.TimeUnit.DAYS         => TimeUnit.DAYS
  }

  private def deserializeFiniteDuration(duration: WireFormats.FiniteDuration): FiniteDuration =
    FiniteDuration(duration.getValue, deserializeTimeUnit(duration.getUnit))

  private def deserializeAddressData(address: WireFormats.AddressData): Address = {
    Address(address.getProtocol, address.getSystem, address.getHostname, address.getPort)
  }
}
