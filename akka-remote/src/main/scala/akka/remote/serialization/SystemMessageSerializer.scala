/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import akka.actor.{ ActorInitializationException, ActorRef, ExtendedActorSystem, InternalActorRef }
import akka.dispatch.sysmsg._
import akka.remote.{ ContainerFormats, SystemMessageFormats }
import akka.serialization.{ BaseSerializer, Serialization, SerializationExtension }

class SystemMessageSerializer(val system: ExtendedActorSystem) extends BaseSerializer {
  import SystemMessageFormats.SystemMessage.Type._

  // WARNING! This must lazy otherwise it will deadlock the ActorSystem creation
  private lazy val serialization = SerializationExtension(system)
  private val payloadSupport = new WrappedPayloadSupport(system)

  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = {
    val builder = SystemMessageFormats.SystemMessage.newBuilder()

    o.asInstanceOf[SystemMessage] match {
      case Create(failure) =>
        builder.setType(CREATE)
        failure match {
          case Some(throwable) => builder.setCauseData(serializeThrowable(throwable))
          case None            => // Nothing to set
        }

      case Recreate(cause) =>
        builder.setType(RECREATE)
        builder.setCauseData(serializeThrowable(cause))

      case Suspend() =>
        builder.setType(SUSPEND)

      case Resume(cause) =>
        builder.setType(RESUME)
        builder.setCauseData(serializeThrowable(cause))

      case Terminate() =>
        builder.setType(TERMINATE)

      case Supervise(child, async) =>
        builder.setType(SUPERVISE)
        val superviseData =
          SystemMessageFormats.SuperviseData.newBuilder().setChild(serializeActorRef(child)).setAsync(async)
        builder.setSuperviseData(superviseData)

      case Watch(watchee, watcher) =>
        builder.setType(WATCH)
        val watchData = SystemMessageFormats.WatchData
          .newBuilder()
          .setWatchee(serializeActorRef(watchee))
          .setWatcher(serializeActorRef(watcher))
        builder.setWatchData(watchData)

      case Unwatch(watchee, watcher) =>
        builder.setType(UNWATCH)
        val watchData = SystemMessageFormats.WatchData
          .newBuilder()
          .setWatchee(serializeActorRef(watchee))
          .setWatcher(serializeActorRef(watcher))
        builder.setWatchData(watchData)

      case Failed(child, cause, uid) =>
        builder.setType(FAILED)
        val failedData = SystemMessageFormats.FailedData.newBuilder().setChild(serializeActorRef(child)).setUid(uid)
        builder.setCauseData(serializeThrowable(cause))
        builder.setFailedData(failedData)

      case DeathWatchNotification(actor, existenceConfirmed, addressTerminated) =>
        builder.setType(DEATHWATCH_NOTIFICATION)
        val deathWatchNotificationData = SystemMessageFormats.DeathWatchNotificationData
          .newBuilder()
          .setActor(serializeActorRef(actor))
          .setExistenceConfirmed(existenceConfirmed)
          .setAddressTerminated(addressTerminated)
        builder.setDwNotificationData(deathWatchNotificationData)

      case NoMessage =>
        throw new IllegalArgumentException("NoMessage should never be serialized or deserialized")
    }

    builder.build().toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    deserializeSystemMessage(SystemMessageFormats.SystemMessage.parseFrom(bytes))
  }

  private def deserializeSystemMessage(sysmsg: SystemMessageFormats.SystemMessage): SystemMessage =
    sysmsg.getType match {
      case CREATE =>
        val cause =
          if (sysmsg.hasCauseData)
            Some(getCauseThrowable(sysmsg).asInstanceOf[ActorInitializationException])
          else
            None

        Create(cause)

      case RECREATE =>
        Recreate(getCauseThrowable(sysmsg))

      case SUSPEND =>
        // WARNING!! Must always create a new instance!
        Suspend()

      case RESUME =>
        Resume(getCauseThrowable(sysmsg))

      case TERMINATE =>
        // WARNING!! Must always create a new instance!
        Terminate()

      case SUPERVISE =>
        Supervise(deserializeActorRef(sysmsg.getSuperviseData.getChild), sysmsg.getSuperviseData.getAsync)

      case WATCH =>
        Watch(
          deserializeActorRef(sysmsg.getWatchData.getWatchee).asInstanceOf[InternalActorRef],
          deserializeActorRef(sysmsg.getWatchData.getWatcher).asInstanceOf[InternalActorRef])

      case UNWATCH =>
        Unwatch(
          deserializeActorRef(sysmsg.getWatchData.getWatchee).asInstanceOf[InternalActorRef],
          deserializeActorRef(sysmsg.getWatchData.getWatcher).asInstanceOf[InternalActorRef])

      case FAILED =>
        Failed(
          deserializeActorRef(sysmsg.getFailedData.getChild),
          getCauseThrowable(sysmsg),
          sysmsg.getFailedData.getUid.toInt)

      case DEATHWATCH_NOTIFICATION =>
        DeathWatchNotification(
          deserializeActorRef(sysmsg.getDwNotificationData.getActor),
          sysmsg.getDwNotificationData.getExistenceConfirmed,
          sysmsg.getDwNotificationData.getAddressTerminated)
    }

  private def serializeThrowable(throwable: Throwable): ContainerFormats.Payload.Builder = {
    payloadSupport.payloadBuilder(throwable)
  }

  private def getCauseThrowable(msg: SystemMessageFormats.SystemMessage): Throwable = {
    payloadSupport.deserializePayload(msg.getCauseData).asInstanceOf[Throwable]
  }

  private def serializeActorRef(actorRef: ActorRef): ContainerFormats.ActorRef.Builder = {
    ContainerFormats.ActorRef.newBuilder().setPath(Serialization.serializedActorPath(actorRef))
  }

  private def deserializeActorRef(serializedRef: ContainerFormats.ActorRef): ActorRef = {
    serialization.system.provider.resolveActorRef(serializedRef.getPath)
  }

}
