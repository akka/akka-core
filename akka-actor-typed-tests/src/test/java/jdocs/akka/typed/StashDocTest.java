/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import static jdocs.akka.typed.StashDocSample.DB;
import static jdocs.akka.typed.StashDocSample.DataAccess;

import akka.Done;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class StashDocTest extends JUnitSuite {

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void stashingExample() throws Exception {
    final DB db =
        new DB() {
          public CompletionStage<Done> save(String id, String value) {
            return CompletableFuture.completedFuture(Done.getInstance());
          }

          public CompletionStage<String> load(String id) {
            return CompletableFuture.completedFuture("TheValue");
          }
        };

    final ActorRef<DataAccess.Command> dataAccess = testKit.spawn(DataAccess.create("17", db));
    TestProbe<String> getInbox = testKit.createTestProbe(String.class);
    dataAccess.tell(new DataAccess.Get(getInbox.getRef()));
    getInbox.expectMessage("TheValue");

    TestProbe<Done> saveInbox = testKit.createTestProbe(Done.class);
    dataAccess.tell(new DataAccess.Save("UpdatedValue", saveInbox.getRef()));
    dataAccess.tell(new DataAccess.Get(getInbox.getRef()));
    saveInbox.expectMessage(Done.getInstance());
    getInbox.expectMessage("UpdatedValue");

    dataAccess.tell(new DataAccess.Get(getInbox.getRef()));
    getInbox.expectMessage("UpdatedValue");
  }
}
