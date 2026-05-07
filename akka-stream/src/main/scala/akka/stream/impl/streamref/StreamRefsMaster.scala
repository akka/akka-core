/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.streamref

import akka.actor.{ ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.stream.impl.SeqActorName

/** INTERNAL API */
@InternalApi
private[stream] object StreamRefsMaster extends ExtensionId[StreamRefsMaster] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): StreamRefsMaster =
    new StreamRefsMaster

  override def lookup: StreamRefsMaster.type = this

  override def get(system: ActorSystem): StreamRefsMaster = super.get(system)
  override def get(system: ClassicActorSystemProvider): StreamRefsMaster = super.get(system)
}

/** INTERNAL API */
@InternalApi
private[stream] final class StreamRefsMaster extends Extension {

  private[this] val sourceRefStageNames = SeqActorName("SourceRef") // "local target"
  private[this] val sinkRefStageNames = SeqActorName("SinkRef") // "remote sender"

  // TODO introduce a master with which all stages running the streams register themselves?

  def nextSourceRefStageName(): String =
    sourceRefStageNames.next()

  def nextSinkRefStageName(): String =
    sinkRefStageNames.next()

}
