/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import akka.Done;
import akka.pattern.StatusReply;
import org.scalatestplus.junit.JUnitSuite;

import static jdocs.akka.cluster.sharding.typed.AccountExampleWithEventHandlersInState.AccountEntity;
import static org.junit.Assert.*;

// #test
import java.math.BigDecimal;
import akka.actor.testkit.typed.javadsl.BehaviorTestKit;
import akka.actor.testkit.typed.javadsl.ReplyInbox;
import akka.actor.testkit.typed.javadsl.StatusReplyInbox;
import akka.actor.testkit.typed.javadsl.TestInbox;
import akka.persistence.testkit.javadsl.UnpersistentBehavior;
import akka.persistence.testkit.javadsl.PersistenceEffect;
import akka.persistence.typed.PersistenceId;

import org.junit.Test;

public class AccountExampleUnpersistentDocTest
    // #test
    extends JUnitSuite
// #test
{
  @Test
  public void createWithEmptyBalance() {
    UnpersistentBehavior<AccountEntity.Command, AccountEntity.Event, AccountEntity.Account>
        unpersistent = emptyAccount();

    BehaviorTestKit<AccountEntity.Command> testkit = unpersistent.getBehaviorTestKit();

    StatusReplyInbox<Done> ackInbox = testkit.runAskWithStatus(AccountEntity.CreateAccount::new);

    ackInbox.expectValue(Done.getInstance());
    unpersistent.getEventProbe().expectPersisted(AccountEntity.AccountCreated.INSTANCE);

    // internal state is only exposed by the behavior via responses to messages or if it happens
    //  to snapshot.  This particular behavior never snapshots, so we query within the actor's
    //  protocol
    assertFalse(unpersistent.getSnapshotProbe().hasEffects());

    ReplyInbox<AccountEntity.CurrentBalance> currentBalanceInbox =
        testkit.runAsk(AccountEntity.GetBalance::new);

    assertEquals(BigDecimal.ZERO, currentBalanceInbox.receiveReply().balance);
  }

  @Test
  public void handleDepositAndWithdraw() {
    UnpersistentBehavior<AccountEntity.Command, AccountEntity.Event, AccountEntity.Account>
        unpersistent = openedAccount();

    BehaviorTestKit<AccountEntity.Command> testkit = unpersistent.getBehaviorTestKit();
    BigDecimal currentBalance;

    testkit
        .runAskWithStatus(
            Done.class, replyTo -> new AccountEntity.Deposit(BigDecimal.valueOf(100), replyTo))
        .expectValue(Done.getInstance());

    assertEquals(
        BigDecimal.valueOf(100),
        unpersistent
            .getEventProbe()
            .expectPersistedClass(AccountEntity.Deposited.class)
            .persistedObject()
            .amount);

    currentBalance =
        testkit
            .runAsk(AccountEntity.CurrentBalance.class, AccountEntity.GetBalance::new)
            .receiveReply()
            .balance;

    assertEquals(BigDecimal.valueOf(100), currentBalance);

    testkit
        .runAskWithStatus(
            Done.class, replyTo -> new AccountEntity.Withdraw(BigDecimal.valueOf(10), replyTo))
        .expectValue(Done.getInstance());

    // can save the persistence effect for in-depth inspection
    PersistenceEffect<AccountEntity.Withdrawn> withdrawEffect =
        unpersistent.getEventProbe().expectPersistedClass(AccountEntity.Withdrawn.class);
    assertEquals(BigDecimal.valueOf(10), withdrawEffect.persistedObject().amount);
    assertEquals(3L, withdrawEffect.sequenceNr());
    assertTrue(withdrawEffect.tags().isEmpty());

    currentBalance =
        testkit
            .runAsk(AccountEntity.CurrentBalance.class, AccountEntity.GetBalance::new)
            .receiveReply()
            .balance;

    assertEquals(BigDecimal.valueOf(90), currentBalance);
  }

  @Test
  public void rejectWithdrawOverdraft() {
    UnpersistentBehavior<AccountEntity.Command, AccountEntity.Event, AccountEntity.Account>
        unpersistent = accountWithBalance(BigDecimal.valueOf(100));

    BehaviorTestKit<AccountEntity.Command> testkit = unpersistent.getBehaviorTestKit();

    testkit
        .runAskWithStatus(
            Done.class, replyTo -> new AccountEntity.Withdraw(BigDecimal.valueOf(110), replyTo))
        .expectErrorMessage("not enough funds to withdraw 110");

    assertFalse(unpersistent.getEventProbe().hasEffects());
  }

  // #test
  private UnpersistentBehavior<AccountEntity.Command, AccountEntity.Event, AccountEntity.Account>
      emptyAccount() {
    return
    // #unpersistent-behavior
    UnpersistentBehavior.fromEventSourced(
        AccountEntity.create("1", PersistenceId.of("Account", "1")),
        null, // use the initial state
        0 // initial sequence number
        );
    // #unpersistent-behavior
  }

  private UnpersistentBehavior<AccountEntity.Command, AccountEntity.Event, AccountEntity.Account>
      openedAccount() {
    return
    // #unpersistent-behavior-provided-state
    UnpersistentBehavior.fromEventSourced(
        AccountEntity.create("1", PersistenceId.of("Account", "1")),
        new AccountEntity.EmptyAccount()
            .openedAccount(), // duplicate the event handler for AccountCreated on an EmptyAccount
        1 // assume that CreateAccount was the first command
        );
    // #unpersistent-behavior-provided-state
  }

  private UnpersistentBehavior<AccountEntity.Command, AccountEntity.Event, AccountEntity.Account>
      accountWithBalance(BigDecimal balance) {
    return UnpersistentBehavior.fromEventSourced(
        AccountEntity.create("1", PersistenceId.of("Account", "1")),
        new AccountEntity.OpenedAccount(balance),
        2);
  }
  // #test
}
// #test
