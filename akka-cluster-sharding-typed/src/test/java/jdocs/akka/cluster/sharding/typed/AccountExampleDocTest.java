/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import akka.Done;
import akka.pattern.StatusReply;
import org.scalatestplus.junit.JUnitSuite;

import static jdocs.akka.cluster.sharding.typed.AccountExampleWithEventHandlersInState.AccountEntity;
import static org.junit.Assert.*;

// #test
import java.math.BigDecimal;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.typed.ActorRef;
import akka.persistence.testkit.javadsl.EventSourcedBehaviorTestKit;
import akka.persistence.testkit.javadsl.EventSourcedBehaviorTestKit.CommandResultWithReply;
import akka.persistence.typed.PersistenceId;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

// #test

// #test
public class AccountExampleDocTest
    // #test
    extends JUnitSuite
// #test
{

  // #testkit
  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(EventSourcedBehaviorTestKit.config());

  private EventSourcedBehaviorTestKit<
          AccountEntity.Command, AccountEntity.Event, AccountEntity.Account>
      eventSourcedTestKit =
          EventSourcedBehaviorTestKit.create(
              testKit.system(), AccountEntity.create("1", PersistenceId.of("Account", "1")));
  // #testkit

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Before
  public void beforeEach() {
    eventSourcedTestKit.clear();
  }

  @Test
  public void createWithEmptyBalance() {
    CommandResultWithReply<
            AccountEntity.Command, AccountEntity.Event, AccountEntity.Account, StatusReply<Done>>
        result = eventSourcedTestKit.runCommand(AccountEntity.CreateAccount::new);
    assertEquals(StatusReply.ack(), result.reply());
    assertEquals(AccountEntity.AccountCreated.INSTANCE, result.event());
    assertEquals(BigDecimal.ZERO, result.stateOfType(AccountEntity.OpenedAccount.class).balance);
  }

  @Test
  public void createWithUnHandle() {
    CommandResultWithReply<
            AccountEntity.Command, AccountEntity.Event, AccountEntity.Account, StatusReply<Done>>
        result = eventSourcedTestKit.runCommand(AccountEntity.CreateAccount::new);
    assertFalse(result.hasNoReply());
  }

  @Test
  public void handleWithdraw() {
    eventSourcedTestKit.runCommand(AccountEntity.CreateAccount::new);

    CommandResultWithReply<
            AccountEntity.Command, AccountEntity.Event, AccountEntity.Account, StatusReply<Done>>
        result1 =
            eventSourcedTestKit.runCommand(
                replyTo -> new AccountEntity.Deposit(BigDecimal.valueOf(100), replyTo));
    assertEquals(StatusReply.ack(), result1.reply());
    assertEquals(
        BigDecimal.valueOf(100), result1.eventOfType(AccountEntity.Deposited.class).amount);
    assertEquals(
        BigDecimal.valueOf(100), result1.stateOfType(AccountEntity.OpenedAccount.class).balance);

    CommandResultWithReply<
            AccountEntity.Command, AccountEntity.Event, AccountEntity.Account, StatusReply<Done>>
        result2 =
            eventSourcedTestKit.runCommand(
                replyTo -> new AccountEntity.Withdraw(BigDecimal.valueOf(10), replyTo));
    assertEquals(StatusReply.ack(), result2.reply());
    assertEquals(BigDecimal.valueOf(10), result2.eventOfType(AccountEntity.Withdrawn.class).amount);
    assertEquals(
        BigDecimal.valueOf(90), result2.stateOfType(AccountEntity.OpenedAccount.class).balance);
  }

  @Test
  public void rejectWithdrawOverdraft() {
    eventSourcedTestKit.runCommand(AccountEntity.CreateAccount::new);
    eventSourcedTestKit.runCommand(
        (ActorRef<StatusReply<Done>> replyTo) ->
            new AccountEntity.Deposit(BigDecimal.valueOf(100), replyTo));

    CommandResultWithReply<
            AccountEntity.Command, AccountEntity.Event, AccountEntity.Account, StatusReply<Done>>
        result =
            eventSourcedTestKit.runCommand(
                replyTo -> new AccountEntity.Withdraw(BigDecimal.valueOf(110), replyTo));
    assertTrue(result.reply().isError());
    assertTrue(result.hasNoEvents());
  }

  @Test
  public void handleGetBalance() {
    eventSourcedTestKit.runCommand(AccountEntity.CreateAccount::new);
    eventSourcedTestKit.runCommand(
        (ActorRef<StatusReply<Done>> replyTo) ->
            new AccountEntity.Deposit(BigDecimal.valueOf(100), replyTo));

    CommandResultWithReply<
            AccountEntity.Command,
            AccountEntity.Event,
            AccountEntity.Account,
            AccountEntity.CurrentBalance>
        result = eventSourcedTestKit.runCommand(AccountEntity.GetBalance::new);
    assertEquals(BigDecimal.valueOf(100), result.reply().balance);
  }
}
// #test
