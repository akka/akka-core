/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence;

// #plugin-imports
import akka.dispatch.Futures;
import akka.persistence.*;
import akka.persistence.journal.japi.*;
import akka.persistence.snapshot.japi.*;
// #plugin-imports

import akka.actor.*;
import akka.persistence.journal.leveldb.SharedLeveldbJournal;
import akka.persistence.journal.leveldb.SharedLeveldbStore;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.runner.RunWith;
import org.scalatestplus.junit.JUnitRunner;
import scala.concurrent.Future;
import java.util.function.Consumer;
import org.iq80.leveldb.util.FileUtils;
import java.util.Optional;

import akka.persistence.japi.journal.JavaJournalSpec;
import akka.persistence.japi.snapshot.JavaSnapshotStoreSpec;

public class LambdaPersistencePluginDocTest {

  static Object o1 =
      new Object() {
        final ActorSystem system = null;
        // #shared-store-creation
        final ActorRef store = system.actorOf(Props.create(SharedLeveldbStore.class), "store");
        // #shared-store-creation

        // #shared-store-usage
        class SharedStorageUsage extends AbstractActor {
          @Override
          public void preStart() throws Exception {
            String path = "akka://example@127.0.0.1:2552/user/store";
            ActorSelection selection = getContext().actorSelection(path);
            selection.tell(new Identify(1), getSelf());
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    ActorIdentity.class,
                    ai -> {
                      if (ai.correlationId().equals(1)) {
                        Optional<ActorRef> store = ai.getActorRef();
                        if (store.isPresent()) {
                          SharedLeveldbJournal.setStore(store.get(), getContext().getSystem());
                        } else {
                          throw new RuntimeException("Couldn't identify store");
                        }
                      }
                    })
                .build();
          }
        }
        // #shared-store-usage
      };

  class MySnapshotStore extends SnapshotStore {
    @Override
    public Future<Optional<SelectedSnapshot>> doLoadAsync(
        String persistenceId, SnapshotSelectionCriteria criteria) {
      return null;
    }

    @Override
    public Future<Void> doSaveAsync(SnapshotMetadata metadata, Object snapshot) {
      return null;
    }

    @Override
    public Future<Void> doDeleteAsync(SnapshotMetadata metadata) {
      return Futures.successful(null);
    }

    @Override
    public Future<Void> doDeleteAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
      return Futures.successful(null);
    }
  }

  class MyAsyncJournal extends AsyncWriteJournal {
    // #sync-journal-plugin-api
    @Override
    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(
        Iterable<AtomicWrite> messages) {
      try {
        Iterable<Optional<Exception>> result = new ArrayList<Optional<Exception>>();
        // blocking call here...
        // result.add(..)
        return Futures.successful(result);
      } catch (Exception e) {
        return Futures.failed(e);
      }
    }
    // #sync-journal-plugin-api

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
      return null;
    }

    @Override
    public Future<Void> doAsyncReplayMessages(
        String persistenceId,
        long fromSequenceNr,
        long toSequenceNr,
        long max,
        Consumer<PersistentRepr> replayCallback) {
      return null;
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
      return null;
    }
  }

  static Object o2 =
      new Object() {
        // #journal-tck-java
        @RunWith(JUnitRunner.class)
        class MyJournalSpecTest extends JavaJournalSpec {

          public MyJournalSpecTest() {
            super(
                ConfigFactory.parseString(
                    "akka.persistence.journal.plugin = "
                        + "\"akka.persistence.journal.leveldb-shared\""));
          }

          @Override
          public CapabilityFlag supportsRejectingNonSerializableObjects() {
            return CapabilityFlag.off();
          }
        }
        // #journal-tck-java
      };

  static Object o3 =
      new Object() {
        // #snapshot-store-tck-java
        @RunWith(JUnitRunner.class)
        class MySnapshotStoreTest extends JavaSnapshotStoreSpec {

          public MySnapshotStoreTest() {
            super(
                ConfigFactory.parseString(
                    "akka.persistence.snapshot-store.plugin = "
                        + "\"akka.persistence.snapshot-store.local\""));
          }
        }
        // #snapshot-store-tck-java
      };

  static Object o4 =
      new Object() {
        // https://github.com/akka/akka/issues/26826
        // #journal-tck-before-after-java
        @RunWith(JUnitRunner.class)
        class MyJournalSpecTest extends JavaJournalSpec {

          List<File> storageLocations = new ArrayList<File>();

          public MyJournalSpecTest() {
            super(
                ConfigFactory.parseString(
                    "persistence.journal.plugin = "
                        + "\"akka.persistence.journal.leveldb-shared\""));

            Config config = system().settings().config();
            storageLocations.add(
                new File(config.getString("akka.persistence.journal.leveldb.dir")));
            storageLocations.add(
                new File(config.getString("akka.persistence.snapshot-store.local.dir")));
          }

          @Override
          public CapabilityFlag supportsRejectingNonSerializableObjects() {
            return CapabilityFlag.on();
          }

          @Override
          public void beforeAll() {
            for (File storageLocation : storageLocations) {
              FileUtils.deleteRecursively(storageLocation);
            }
            super.beforeAll();
          }

          @Override
          public void afterAll() {
            super.afterAll();
            for (File storageLocation : storageLocations) {
              FileUtils.deleteRecursively(storageLocation);
            }
          }
        }
        // #journal-tck-before-after-java
      };
}
