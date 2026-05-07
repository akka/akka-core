/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.snapshot

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.persistence._
import akka.persistence.SnapshotProtocol._
import akka.persistence.scalatest.{ MayVerb, OptionalTests }
import akka.testkit.TestProbe

object SnapshotStoreSpec {
  val config: Config = ConfigFactory.parseString(s"""
    akka.persistence.publish-plugin-commands = on
    akka.actor {
      serializers {
        persistence-tck-test = "${classOf[TestSerializer].getName}"
      }
      serialization-bindings {
        "${classOf[TestPayload].getName}" = persistence-tck-test
      }
    }
    """)
}

/**
 * This spec aims to verify custom akka-persistence [[SnapshotStore]] implementations.
 * Plugin authors are highly encouraged to include it in their plugin's test suites.
 *
 * In case your journal plugin needs some kind of setup or teardown, override the `beforeAll` or `afterAll`
 * methods (don't forget to call `super` in your overridden methods).
 *
 * For a Java and JUnit consumable version of the TCK please refer to [[akka.persistence.japi.snapshot.JavaSnapshotStoreSpec]].
 *
 * @see [[akka.persistence.japi.snapshot.JavaSnapshotStoreSpec]]
 */
abstract class SnapshotStoreSpec(config: Config)
    extends PluginSpec(config)
    with MayVerb
    with OptionalTests
    with SnapshotStoreCapabilityFlags {
  implicit lazy val system: ActorSystem =
    ActorSystem("SnapshotStoreSpec", config.withFallback(SnapshotStoreSpec.config))

  private var senderProbe: TestProbe = _
  private var metadata: Seq[SnapshotMetadata] = Nil

  override protected def supportsSerialization: CapabilityFlag = true
  override protected def supportsMetadata: CapabilityFlag = false

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    senderProbe = TestProbe()
    metadata = writeSnapshots()
  }

  def snapshotStore: ActorRef =
    extension.snapshotStoreFor(null)

  def writeSnapshots(): Seq[SnapshotMetadata] = {
    (1 to 5).map { i =>
      val metadata = SnapshotMetadata(pid, i + 10)
      snapshotStore.tell(SaveSnapshot(metadata, s"s-${i}"), senderProbe.ref)
      senderProbe.expectMsgPF() { case SaveSnapshotSuccess(md) => md }
    }
  }

  /**
   * The limit defines a number of bytes persistence plugin can support to store the snapshot.
   * If plugin does not support persistence of the snapshots of 10000 bytes or may support more than default size,
   * the value can be overridden by the SnapshotStoreSpec implementation with a note in a plugin documentation.
   */
  def snapshotByteSizeLimit = 10000

  "A snapshot store" must {
    "not load a snapshot given an invalid persistenceId" in {
      snapshotStore.tell(LoadSnapshot("invalid", SnapshotSelectionCriteria.Latest, Long.MaxValue), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
    }
    "not load a snapshot given non-matching timestamp criteria" in {
      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria.Latest.copy(maxTimestamp = 100), Long.MaxValue),
        senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
    }
    "not load a snapshot given non-matching sequence number criteria" in {
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria(7), Long.MaxValue), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, 7), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, 7))
    }
    "load the most recent snapshot" in {
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(4), s"s-5")), Long.MaxValue))
    }
    "load the most recent snapshot matching an upper sequence number bound" in {
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria(13), Long.MaxValue), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(2), s"s-3")), Long.MaxValue))
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, 13), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(2), s"s-3")), 13))
    }
    "load the most recent snapshot matching upper sequence number and timestamp bounds" in {
      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria(13, metadata(2).timestamp), Long.MaxValue),
        senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(2), s"s-3")), Long.MaxValue))
      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria.Latest.copy(maxTimestamp = metadata(2).timestamp), 13),
        senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(2), s"s-3")), 13))
    }
    "delete a single snapshot identified by sequenceNr in snapshot metadata" in {
      val md = metadata(2).copy(timestamp = 0L) // don't care about timestamp for delete of single snap
      val cmd = DeleteSnapshot(md)
      val sub = TestProbe()

      subscribe[DeleteSnapshot](sub.ref)
      snapshotStore.tell(cmd, senderProbe.ref)
      sub.expectMsg(cmd)
      senderProbe.expectMsg(DeleteSnapshotSuccess(md))

      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria(md.sequenceNr), Long.MaxValue), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(1), s"s-2")), Long.MaxValue))
    }
    "delete all snapshots matching upper sequence number and timestamp bounds" in {
      val md = metadata(2)
      val criteria = SnapshotSelectionCriteria(md.sequenceNr, md.timestamp)
      val cmd = DeleteSnapshots(pid, criteria)
      val sub = TestProbe()

      subscribe[DeleteSnapshots](sub.ref)
      snapshotStore.tell(cmd, senderProbe.ref)
      sub.expectMsg(cmd)
      senderProbe.expectMsg(DeleteSnapshotsSuccess(criteria))

      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria(md.sequenceNr, md.timestamp), Long.MaxValue),
        senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria(metadata(3).sequenceNr, metadata(3).timestamp), Long.MaxValue),
        senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(3), s"s-4")), Long.MaxValue))
    }
    "not delete snapshots with non-matching upper timestamp bounds" in {
      val md = metadata(3)
      val criteria = SnapshotSelectionCriteria(md.sequenceNr, md.timestamp - 1)
      val cmd = DeleteSnapshots(pid, criteria)
      val sub = TestProbe()

      subscribe[DeleteSnapshots](sub.ref)
      snapshotStore.tell(cmd, senderProbe.ref)
      sub.expectMsg(cmd)
      senderProbe.expectMsg(DeleteSnapshotsSuccess(criteria))

      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria(metadata(3).sequenceNr, metadata(3).timestamp), Long.MaxValue),
        senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(3), s"s-4")), Long.MaxValue))
    }
    "save and overwrite snapshot with same sequence number" in {
      val md = metadata(4)
      snapshotStore.tell(SaveSnapshot(md, s"s-5-modified"), senderProbe.ref)
      val md2 = senderProbe.expectMsgPF() { case SaveSnapshotSuccess(md2) => md2 }
      md2.sequenceNr should be(md.sequenceNr)
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria(md.sequenceNr), Long.MaxValue), senderProbe.ref)
      val result = senderProbe.expectMsgType[LoadSnapshotResult]
      result.snapshot.get.snapshot should be("s-5-modified")
      result.snapshot.get.metadata.sequenceNr should be(md.sequenceNr)
      // metadata timestamp may have been changed
    }
    s"save bigger size snapshot ($snapshotByteSizeLimit bytes)" in {
      val metadata = SnapshotMetadata(pid, 100)
      val bigSnapshot = "0" * snapshotByteSizeLimit
      snapshotStore.tell(SaveSnapshot(metadata, bigSnapshot), senderProbe.ref)
      senderProbe.expectMsgPF() { case SaveSnapshotSuccess(md) => md }
    }
  }

  "A snapshot store optionally".may {
    optional(flag = supportsSerialization) {
      "serialize snapshots" in {
        val probe = TestProbe()
        val metadata = SnapshotMetadata(pid, 100)
        val snap = TestPayload(probe.ref)
        snapshotStore.tell(SaveSnapshot(metadata, snap), senderProbe.ref)
        senderProbe.expectMsgPF() { case SaveSnapshotSuccess(md) => md }

        val Pid = pid
        snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), senderProbe.ref)
        senderProbe.expectMsgPF() {
          case LoadSnapshotResult(Some(SelectedSnapshot(SnapshotMetadata(Pid, 100, _), payload)), Long.MaxValue) =>
            payload should be(snap)
        }
      }
    }
    optional(flag = supportsMetadata) {
      "store metadata" in {
        // we do not have the actual ReplicatedSnapshot metadata on classpath, but since
        // the plugin should defer to serialization defined by Akka, so in general the type
        // should not really be important to the plugin
        val fictionalMeta = "fictional metadata"
        val metadata = SnapshotMetadata(pid, 100).withMetadata(fictionalMeta)
        val snap = "snap"
        snapshotStore.tell(SaveSnapshot(metadata, snap), senderProbe.ref)
        senderProbe.expectMsgPF() { case SaveSnapshotSuccess(md) => md }

        val Pid = pid
        snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), senderProbe.ref)
        senderProbe.expectMsgPF() {
          case LoadSnapshotResult(
              Some(SelectedSnapshot(meta @ SnapshotMetadata(Pid, 100, _), payload)),
              Long.MaxValue) =>
            payload should be(snap)
            meta.metadata should ===(Some(fictionalMeta))
        }
      }
    }

  }
}
