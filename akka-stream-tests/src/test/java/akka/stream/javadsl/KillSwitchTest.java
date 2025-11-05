/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static akka.Done.done;
import static org.junit.Assert.*;

import akka.Done;
import akka.stream.*;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.Utils;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;

public class KillSwitchTest extends StreamTest {
  public KillSwitchTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("KillSwitchTest", AkkaSpec.testConf());

  @Test
  public void beAbleToUseKillSwitch() throws Exception {
    final TestPublisher.Probe<Integer> upstream = TestPublisher.probe(0, system);
    final TestSubscriber.Probe<Integer> downstream = TestSubscriber.probe(system);
    final SharedKillSwitch killSwitch = KillSwitches.shared("testSwitch");

    final SharedKillSwitch k =
        Source.fromPublisher(upstream)
            .viaMat(killSwitch.flow(), Keep.right())
            .to(Sink.fromSubscriber(downstream))
            .run(system);

    final CompletionStage<Done> completionStage =
        Source.single(1).via(killSwitch.flow()).runWith(Sink.ignore(), system);

    downstream.request(1);
    upstream.sendNext(1);
    downstream.expectNext(1);

    assertEquals(killSwitch, k);

    killSwitch.shutdown();

    upstream.expectCancellation();
    downstream.expectComplete();

    assertEquals(completionStage.toCompletableFuture().get(3, TimeUnit.SECONDS), done());
  }

  @Test
  public void beAbleToUseKillSwitchAbort() throws Exception {
    final TestPublisher.Probe<Integer> upstream = TestPublisher.probe(0, system);
    final TestSubscriber.Probe<Integer> downstream = TestSubscriber.probe(system);
    final SharedKillSwitch killSwitch = KillSwitches.shared("testSwitch");

    Source.fromPublisher(upstream)
        .viaMat(killSwitch.flow(), Keep.right())
        .runWith(Sink.fromSubscriber(downstream), system);

    downstream.request(1);
    upstream.sendNext(1);
    downstream.expectNext(1);

    final Exception te = new Utils.TE("Testy");
    killSwitch.abort(te);

    upstream.expectCancellation();
    final Throwable te2 = downstream.expectError();

    assertEquals(te, te2);
  }

  @Test
  public void beAbleToUseSingleKillSwitch() throws Exception {
    final TestPublisher.Probe<Integer> upstream = TestPublisher.probe(0, system);
    final TestSubscriber.Probe<Integer> downstream = TestSubscriber.probe(system);
    final Graph<FlowShape<Integer, Integer>, UniqueKillSwitch> killSwitchFlow =
        KillSwitches.single();

    final UniqueKillSwitch killSwitch =
        Source.fromPublisher(upstream)
            .viaMat(killSwitchFlow, Keep.right())
            .to(Sink.fromSubscriber(downstream))
            .run(system);

    downstream.request(1);
    upstream.sendNext(1);
    downstream.expectNext(1);

    killSwitch.shutdown();

    upstream.expectCancellation();
    downstream.expectComplete();
  }
}
