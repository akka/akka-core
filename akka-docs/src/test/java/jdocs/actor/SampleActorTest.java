/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

import static org.junit.Assert.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SampleActorTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("SampleActorTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void testSampleActor() {
    new TestKit(system) {
      {
        final ActorRef subject = system.actorOf(Props.create(SampleActor.class), "sample-actor");
        final ActorRef probeRef = getRef();

        subject.tell(47.11, probeRef);
        subject.tell("and no guard in the beginning", probeRef);
        subject.tell("guard is a good thing", probeRef);
        subject.tell(47.11, probeRef);
        subject.tell(4711, probeRef);
        subject.tell("and no guard in the beginning", probeRef);
        subject.tell(4711, probeRef);
        subject.tell("and an unmatched message", probeRef);

        expectMsgEquals(47.11);
        assertTrue(expectMsgClass(String.class).startsWith("startsWith(guard):"));
        assertTrue(expectMsgClass(String.class).startsWith("contains(guard):"));
        expectMsgEquals(47110);
        expectNoMessage();
      }
    };
  }
}
