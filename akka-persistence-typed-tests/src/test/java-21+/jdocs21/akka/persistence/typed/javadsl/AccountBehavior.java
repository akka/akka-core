/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs21.akka.persistence.typed.javadsl;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;
import akka.serialization.jackson.CborSerializable;

import java.math.BigDecimal;

/**
 * Bank account example illustrating usage of Java 21 features
 */
// #account-behavior
public class AccountBehavior
    extends EventSourcedOnCommandBehavior<
        AccountBehavior.Command, AccountBehavior.Event, AccountBehavior.Account> {

  public sealed interface Command extends CborSerializable {
  }

  public record CreateAccount(ActorRef<StatusReply<Done>> replyTo) implements Command {
  }

  public record Deposit(BigDecimal amount, ActorRef<StatusReply<Done>> replyTo) implements Command {
  }

  public record Withdraw(BigDecimal amount, ActorRef<StatusReply<Done>> replyTo) implements Command { }

  public record GetBalance(ActorRef<CurrentBalance> replyTo) implements Command {
  }

  public record CloseAccount(ActorRef<StatusReply<Done>> replyTo) implements Command {
  }

  // Reply
  public record CurrentBalance(BigDecimal balance) implements CborSerializable {
  }


  public sealed interface Event extends CborSerializable {
  }

  public record AccountCreated() implements Event {
  }

  public record Deposited(BigDecimal amount) implements Event {
  }

  public record Withdrawn(BigDecimal amount) implements Event {
  }

  public record AccountClosed() implements Event {
  }

  // State
  public sealed interface Account extends CborSerializable {}

  public record OpenedAccount(BigDecimal balance) implements Account {
    OpenedAccount makeDeposit(BigDecimal amount) {
      return new OpenedAccount(balance.add(amount));
    }

    boolean canWithdraw(BigDecimal amount) {
      return (balance.subtract(amount).compareTo(BigDecimal.ZERO) >= 0);
    }

    OpenedAccount makeWithdraw(BigDecimal amount) {
      if (!canWithdraw(amount))
        throw new IllegalStateException("Account balance can't be negative");
      return new OpenedAccount(balance.subtract(amount));
    }

    ClosedAccount closedAccount() {
      return new ClosedAccount();
    }
  }

  public record ClosedAccount() implements Account {
  }

  public static AccountBehavior create(String accountNumber, PersistenceId persistenceId) {
    return new AccountBehavior(accountNumber, persistenceId);
  }

  private final String accountNumber;

  private AccountBehavior(String accountNumber, PersistenceId persistenceId) {
    super(persistenceId);
    this.accountNumber = accountNumber;
  }

  @Override
  public Account emptyState() {
    return null;
  }

  @Override
  public Effect<Event, Account> onCommand(Account account, Command command) {
    return switch (account) {
      case null -> switch (command) {
        case CreateAccount create -> onCreateAccount(create);
        default -> Effect().unhandled();
      };
      case OpenedAccount opened -> switch (command) {
        case Deposit deposit -> onDeposit(opened, deposit);
        case Withdraw withdraw -> onWithdraw(opened, withdraw);
        case GetBalance getBalance -> onGetBalance(opened, getBalance);
        case CloseAccount closeAccount -> onCloseAccount(opened, closeAccount);
        case CreateAccount createAccount ->
            Effect().reply(createAccount.replyTo, StatusReply.error("Account already opened"));
      };
      case ClosedAccount ignore -> Effect().unhandled();
    };
  }

  private ReplyEffect<Event, Account> onCreateAccount(CreateAccount command) {
    return Effect()
        .persist(new AccountCreated())
        .thenReply(command.replyTo, account2 -> StatusReply.ack());
  }

  private ReplyEffect<Event, Account> onDeposit(OpenedAccount account, Deposit command) {
    return Effect()
        .persist(new Deposited(command.amount))
        .thenReply(command.replyTo, account2 -> StatusReply.ack());
  }

  private ReplyEffect<Event, Account> onWithdraw(OpenedAccount account, Withdraw command) {
    if (!account.canWithdraw(command.amount)) {
      return Effect()
          .reply(
              command.replyTo,
              StatusReply.error("not enough funds to withdraw " + command.amount));
    } else {
      return Effect()
          .persist(new Withdrawn(command.amount))
          .thenReply(command.replyTo, account2 -> StatusReply.ack());
    }
  }

  private ReplyEffect<Event, Account> onGetBalance(OpenedAccount account, GetBalance command) {
    return Effect().reply(command.replyTo, new CurrentBalance(account.balance));
  }

  private ReplyEffect<Event, Account> onCloseAccount(OpenedAccount account, CloseAccount command) {
    if (account.balance.equals(BigDecimal.ZERO)) {
      return Effect()
          .persist(new AccountClosed())
          .thenReply(command.replyTo, account2 -> StatusReply.ack());
    } else {
      return Effect()
          .reply(command.replyTo, StatusReply.error("balance must be zero for closing account"));
    }
  }


  @Override
  public Account onEvent(Account state, Event event) {
    return switch (state) {
      case null -> switch (event) {
        case AccountCreated ignored -> new OpenedAccount(BigDecimal.ZERO);
        default -> throw new IllegalStateException("Unexpected event for null account " + event);
      };
      case OpenedAccount account -> switch (event) {
        case Deposited deposited -> account.makeDeposit(deposited.amount);
        case Withdrawn withdrawn -> account.makeWithdraw(withdrawn.amount);
        case AccountClosed ignored -> account.closedAccount();
        case AccountCreated ignored ->
            throw new IllegalStateException("AccountCreated event for already open account");
      };
      case ClosedAccount ignored ->
          throw new IllegalStateException("ClosedAccount does not accept any events but saw event " + event);
    };
  }
}
// #account-behavior
