/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import static akka.Done.done;
import static jdocs.akka.cluster.sharding.typed.AccountExampleWithEventHandlersInState.AccountEntity;
import static jdocs.akka.cluster.sharding.typed.AccountExampleWithEventHandlersInState.AccountEntity.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.Done;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class AccountExampleTest extends JUnitSuite {

  public static final Config config =
      ConfigFactory.parseString(
          "akka.actor.provider = cluster \n"
              + "akka.remote.artery.canonical.port = 0 \n"
              + "akka.remote.artery.canonical.hostname = 127.0.0.1 \n"
              + "akka.persistence.journal.plugin = \"akka.persistence.journal.inmem\" \n"
              + "akka.persistence.journal.inmem.test-serialization = on \n");

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource(config);

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  private ClusterSharding _sharding = null;

  private ClusterSharding sharding() {
    if (_sharding == null) {
      // initialize first time only
      Cluster cluster = Cluster.get(testKit.system());
      cluster.manager().tell(new Join(cluster.selfMember().address()));

      ClusterSharding sharding = ClusterSharding.get(testKit.system());
      sharding.init(
          Entity.of(
              AccountEntity.ENTITY_TYPE_KEY,
              entityContext ->
                  AccountEntity.create(
                      entityContext.getEntityId(),
                      PersistenceId.of(
                          entityContext.getEntityTypeKey().name(), entityContext.getEntityId()))));
      _sharding = sharding;
    }
    return _sharding;
  }

  @Test
  public void handleDeposit() {
    EntityRef<Command> ref = sharding().entityRefFor(AccountEntity.ENTITY_TYPE_KEY, "1");
    TestProbe<StatusReply<Done>> probe = testKit.createTestProbe();
    ref.tell(new CreateAccount(probe.getRef()));
    probe.expectMessage(StatusReply.ack());
    ref.tell(new Deposit(BigDecimal.valueOf(100), probe.getRef()));
    probe.expectMessage(StatusReply.ack());
    ref.tell(new Deposit(BigDecimal.valueOf(10), probe.getRef()));
    probe.expectMessage(StatusReply.ack());
  }

  @Test
  public void handleWithdraw() {
    EntityRef<Command> ref = sharding().entityRefFor(AccountEntity.ENTITY_TYPE_KEY, "2");
    TestProbe<StatusReply<Done>> probe = testKit.createTestProbe();
    ref.tell(new CreateAccount(probe.getRef()));
    probe.expectMessage(StatusReply.ack());
    ref.tell(new Deposit(BigDecimal.valueOf(100), probe.getRef()));
    probe.expectMessage(StatusReply.ack());
    ref.tell(new Withdraw(BigDecimal.valueOf(10), probe.getRef()));
    probe.expectMessage(StatusReply.ack());
  }

  @Test
  public void rejectWithdrawOverdraft() {
    EntityRef<Command> ref = sharding().entityRefFor(AccountEntity.ENTITY_TYPE_KEY, "3");
    TestProbe<StatusReply<Done>> probe = testKit.createTestProbe();
    ref.tell(new CreateAccount(probe.getRef()));
    probe.expectMessage(StatusReply.ack());
    ref.tell(new Deposit(BigDecimal.valueOf(100), probe.getRef()));
    probe.expectMessage(StatusReply.ack());
    ref.tell(new Withdraw(BigDecimal.valueOf(110), probe.getRef()));
    assertTrue(probe.receiveMessage().isError());
  }

  @Test
  public void handleGetBalance() {
    EntityRef<Command> ref = sharding().entityRefFor(AccountEntity.ENTITY_TYPE_KEY, "4");
    TestProbe<StatusReply<Done>> opProbe = testKit.createTestProbe();
    ref.tell(new CreateAccount(opProbe.getRef()));
    opProbe.expectMessage(StatusReply.ack());
    ref.tell(new Deposit(BigDecimal.valueOf(100), opProbe.getRef()));
    opProbe.expectMessage(StatusReply.ack());

    TestProbe<CurrentBalance> getProbe = testKit.createTestProbe(CurrentBalance.class);
    ref.tell(new GetBalance(getProbe.getRef()));
    assertEquals(
        BigDecimal.valueOf(100), getProbe.expectMessageClass(CurrentBalance.class).balance);
  }

  @Test
  public void beUsableWithAsk() throws Exception {
    Duration timeout = Duration.ofSeconds(3);
    EntityRef<Command> ref = sharding().entityRefFor(AccountEntity.ENTITY_TYPE_KEY, "5");
    CompletionStage<Done> createResult = ref.askWithStatus(CreateAccount::new, timeout);
    assertEquals(done(), createResult.toCompletableFuture().get(3, TimeUnit.SECONDS));

    // above works because then the response type is inferred by the lhs type
    // below requires explicit typing

    assertEquals(
        done(),
        ref.<Done>askWithStatus(replyTo -> new Deposit(BigDecimal.valueOf(100), replyTo), timeout)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS));

    assertEquals(
        done(),
        ref.<Done>askWithStatus(replyTo -> new Withdraw(BigDecimal.valueOf(10), replyTo), timeout)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS));

    BigDecimal balance =
        ref.ask(GetBalance::new, timeout)
            .thenApply(currentBalance -> currentBalance.balance)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);
    assertEquals(BigDecimal.valueOf(90), balance);
  }

  @Test
  public void verifySerialization() {
    TestProbe<StatusReply<Done>> opProbe = testKit.createTestProbe();
    testKit.serializationTestKit().verifySerialization(new CreateAccount(opProbe.getRef()), false);
    Deposit deposit2 =
        testKit
            .serializationTestKit()
            .verifySerialization(new Deposit(BigDecimal.valueOf(100), opProbe.getRef()), false);
    assertEquals(BigDecimal.valueOf(100), deposit2.amount);
    assertEquals(opProbe.getRef(), deposit2.replyTo);
    testKit
        .serializationTestKit()
        .verifySerialization(new Withdraw(BigDecimal.valueOf(90), opProbe.getRef()), false);
    testKit.serializationTestKit().verifySerialization(new CloseAccount(opProbe.getRef()), false);

    TestProbe<CurrentBalance> getProbe = testKit.createTestProbe();
    testKit.serializationTestKit().verifySerialization(new GetBalance(getProbe.getRef()), false);

    testKit
        .serializationTestKit()
        .verifySerialization(new CurrentBalance(BigDecimal.valueOf(100)), false);

    testKit.serializationTestKit().verifySerialization(AccountCreated.INSTANCE, false);
    testKit
        .serializationTestKit()
        .verifySerialization(new Deposited(BigDecimal.valueOf(100)), false);
    testKit
        .serializationTestKit()
        .verifySerialization(new Withdrawn(BigDecimal.valueOf(90)), false);
    testKit.serializationTestKit().verifySerialization(new AccountClosed(), false);

    testKit.serializationTestKit().verifySerialization(new EmptyAccount(), false);
    OpenedAccount openedAccount2 =
        testKit
            .serializationTestKit()
            .verifySerialization(new OpenedAccount(BigDecimal.valueOf(100)), false);
    assertEquals(BigDecimal.valueOf(100), openedAccount2.balance);
    testKit.serializationTestKit().verifySerialization(new ClosedAccount(), false);
  }
}
