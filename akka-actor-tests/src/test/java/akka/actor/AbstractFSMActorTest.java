/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.testkit.TestProbe;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class AbstractFSMActorTest extends JUnitSuite {

  // javac produces an `unchecked` warning about `akka$actor$FSM$$transitionEvent`
  // https://github.com/lampepfl/dotty/issues/6350
  public static class MyFSM extends AbstractFSM<String, String> {

    private final ActorRef probe;

    MyFSM(ActorRef probe) {
      this.probe = probe;
      onTransition(this::logTransition);
      startWith("start", "data");
      when("start", matchEventEquals("next", (newState, data) -> goTo(newState)));
      when("next", AbstractFSM.NullFunction());
      initialize();
    }

    private void logTransition(final String s1, final String s2) {
      probe.tell(String.format("Transitioning from %1$s to %2$s.", s1, s2), getSelf());
    }
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("AbstractFSMActorTest", AkkaSpec.testConf());

  private final ActorSystem system = actorSystemResource.getSystem();

  @Test
  public void canCreateFSM() {
    // Coverage for #22887 (failed with Scala 2.12 before fix)
    TestProbe probe = new TestProbe(system);

    ActorRef ref = system.actorOf(Props.create(MyFSM.class, probe.ref()));
    probe.expectMsg("Transitioning from start to start.");

    ref.tell("next", ActorRef.noSender());

    probe.expectMsg("Transitioning from start to next.");
  }
}
