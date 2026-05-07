/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Deploy
import akka.actor.Props
import akka.actor.Terminated
import akka.persistence._
import akka.persistence.journal.leveldb.SharedLeveldbJournal
import akka.persistence.journal.leveldb.SharedLeveldbStore

/**
 * Utility program that removes the internal data stored with Akka Persistence
 * by the Cluster `ShardCoordinator`. The data contains the locations of the
 * shards using Akka Persistence and it can safely be removed when restarting
 * the whole Akka Cluster. Note that this is not application data.
 *
 * <b>Never use this program while there are running Akka Cluster that is
 * using Cluster Sharding. Stop all Cluster nodes before using this program.</b>
 *
 * It can be needed to remove the data if the Cluster `ShardCoordinator`
 * cannot startup because of corrupt data, which may happen if accidentally
 * two clusters were running at the same time, e.g. caused by using auto-down
 * and there was a network partition.
 *
 * Use this program as a standalone Java main program:
 * {{{
 * java -classpath <jar files, including akka-cluster-sharding>
 *   akka.cluster.sharding.RemoveInternalClusterShardingData
 *     -2.3 entityType1 entityType2 entityType3
 * }}}
 *
 * The program is included in the `akka-cluster-sharding` jar file. It
 * is easiest to run it with same classpath and configuration as your ordinary
 * application. It can be run from sbt or maven in similar way.
 *
 * Specify the entity type names (same as you use in the `start` method
 * of `ClusterSharding`) as program arguments.
 *
 * If you specify `-2.3` as the first program argument it will also try
 * to remove data that was stored by Cluster Sharding in Akka 2.3.x using
 * different persistenceId.
 */
object RemoveInternalClusterShardingData {

  /**
   * @see [[RemoveInternalClusterShardingData$ RemoveInternalClusterShardingData companion object]]
   */
  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      println("Specify the Cluster Sharding type names to remove in program arguments")
    else {
      val system = ActorSystem("RemoveInternalClusterShardingData")
      val remove2dot3Data = (args(0) == "-2.3")
      val typeNames = if (remove2dot3Data) args.tail.toSet else args.toSet
      if (typeNames.isEmpty)
        println("Specify the Cluster Sharding type names to remove in program arguments")
      else {
        val journalPluginId = system.settings.config.getString("akka.cluster.sharding.journal-plugin-id")
        import system.dispatcher
        remove(system, journalPluginId, typeNames, remove2dot3Data).onComplete { _ =>
          system.terminate()
        }
      }
    }
  }

  /**
   * API corresponding to the [[#main]] method as described in the
   * [[RemoveInternalClusterShardingData$ RemoveInternalClusterShardingData companion object]]
   */
  def remove(
      system: ActorSystem,
      journalPluginId: String,
      typeNames: Set[String],
      remove2dot3Data: Boolean): Future[Unit] = {

    val resolvedJournalPluginId =
      if (journalPluginId == "") system.settings.config.getString("akka.persistence.journal.plugin")
      else journalPluginId
    if (resolvedJournalPluginId == "akka.persistence.journal.leveldb-shared") {
      @nowarn("msg=deprecated")
      val store = system.actorOf(Props(new SharedLeveldbStore), "store")
      SharedLeveldbJournal.setStore(store, system)
    }

    val completion = Promise[Unit]()
    system.actorOf(
      props(journalPluginId, typeNames, completion, remove2dot3Data),
      name = "removeInternalClusterShardingData")
    completion.future
  }

  /**
   * INTERNAL API: `Props` for [[RemoveInternalClusterShardingData]] actor.
   */
  private[akka] def props(
      journalPluginId: String,
      typeNames: Set[String],
      completion: Promise[Unit],
      remove2dot3Data: Boolean): Props =
    Props(new RemoveInternalClusterShardingData(journalPluginId, typeNames, completion, remove2dot3Data))
      .withDeploy(Deploy.local)

  /**
   * INTERNAL API
   */
  private[akka] object RemoveOnePersistenceId {
    def props(journalPluginId: String, persistenceId: String, replyTo: ActorRef): Props =
      Props(new RemoveOnePersistenceId(journalPluginId, persistenceId: String, replyTo))

    case class Result(removals: Try[Removals])
    case class Removals(events: Boolean, snapshots: Boolean)
  }

  /**
   * INTERNAL API: Remove all events and snapshots for one specific
   * `persistenceId`. It will reply with `RemoveOnePersistenceId.Result`
   * when done.
   */
  private[akka] class RemoveOnePersistenceId(
      override val journalPluginId: String,
      override val persistenceId: String,
      replyTo: ActorRef)
      extends PersistentActor {

    import RemoveInternalClusterShardingData.RemoveOnePersistenceId._

    var hasSnapshots = false

    override def receiveRecover: Receive = {
      case _: ShardCoordinator.Internal.DomainEvent =>
      case SnapshotOffer(_, _) =>
        hasSnapshots = true

      case RecoveryCompleted =>
        deleteMessages(Long.MaxValue)
        if (hasSnapshots)
          deleteSnapshots(SnapshotSelectionCriteria())
        else
          context.become(waitDeleteMessagesSuccess)
    }

    override def receiveCommand: Receive =
      ({
        case DeleteSnapshotsSuccess(_) =>
          context.become(waitDeleteMessagesSuccess)
        case DeleteMessagesSuccess(_) =>
          context.become(waitDeleteSnapshotsSuccess)
      }: Receive).orElse(handleFailure)

    def waitDeleteSnapshotsSuccess: Receive =
      ({
        case DeleteSnapshotsSuccess(_) => done()
      }: Receive).orElse(handleFailure)

    def waitDeleteMessagesSuccess: Receive =
      ({
        case DeleteMessagesSuccess(_) => done()
      }: Receive).orElse(handleFailure)

    def handleFailure: Receive = {
      case DeleteMessagesFailure(cause, _)  => failure(cause)
      case DeleteSnapshotsFailure(_, cause) => failure(cause)
    }

    def done(): Unit = {
      replyTo ! Result(Success(Removals(lastSequenceNr > 0, hasSnapshots)))
      context.stop(self)
    }

    def failure(cause: Throwable): Unit = {
      replyTo ! Result(Failure(cause))
      context.stop(self)
    }

  }

}

/**
 * @see [[RemoveInternalClusterShardingData$ RemoveInternalClusterShardingData companion object]]
 */
class RemoveInternalClusterShardingData(
    journalPluginId: String,
    typeNames: Set[String],
    completion: Promise[Unit],
    remove2dot3Data: Boolean)
    extends Actor
    with ActorLogging {
  import RemoveInternalClusterShardingData._
  import RemoveOnePersistenceId.Result

  var currentPid: String = _
  var currentRef: ActorRef = _
  var remainingPids = typeNames.map(persistenceId) ++
    (if (remove2dot3Data) typeNames.map(persistenceId2dot3) else Set.empty)

  def persistenceId(typeName: String): String = s"/sharding/${typeName}Coordinator"

  def persistenceId2dot3(typeName: String): String = s"/user/sharding/${typeName}Coordinator/singleton/coordinator"

  override def preStart(): Unit = {
    removeNext()
  }

  def removeNext(): Unit = {
    currentPid = remainingPids.head
    log.info("Removing data for persistenceId [{}]", currentPid)
    currentRef = context.actorOf(RemoveOnePersistenceId.props(journalPluginId, currentPid, self))
    context.watch(currentRef)
    remainingPids -= currentPid
  }

  def receive = {
    case Result(Success(_)) =>
      log.info("Removed data for persistenceId [{}]", currentPid)
      if (remainingPids.isEmpty) done()
      else removeNext()

    case Result(Failure(cause)) =>
      log.error("Failed to remove data for persistenceId [{}]", currentPid)
      failure(cause)

    case Terminated(ref) =>
      if (ref == currentRef) {
        val msg = s"Failed to remove data for persistenceId [$currentPid], unexpected termination"
        log.error(msg)
        failure(new IllegalStateException(msg))
      }
  }

  def done(): Unit = {
    context.stop(self)
    completion.success(())
  }

  def failure(cause: Throwable): Unit = {
    context.stop(self)
    completion.failure(cause)
  }
}
