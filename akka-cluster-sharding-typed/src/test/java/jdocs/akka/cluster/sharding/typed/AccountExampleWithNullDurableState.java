/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.state.javadsl.CommandHandlerWithReply;
import akka.persistence.typed.state.javadsl.CommandHandlerWithReplyBuilder;
import akka.persistence.typed.state.javadsl.DurableStateBehaviorWithEnforcedReplies;
import akka.persistence.typed.state.javadsl.ReplyEffect;
import akka.serialization.jackson.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.math.BigDecimal;

/**
 * This bank account example illustrates the following: - different state classes representing the
 * lifecycle of the account - null as emptyState - command handlers that delegate to methods in the
 * DurableStateBehavior class, and - replies of various types that use
 * DurableStateBehaviorWithEnforcedReplies
 */
public interface AccountExampleWithNullDurableState {

  // #account-entity
  // #withEnforcedReplies
  public class AccountEntity
      extends DurableStateBehaviorWithEnforcedReplies<
          AccountEntity.Command, AccountEntity.Account> {
    // #withEnforcedReplies

    public static final EntityTypeKey<Command> ENTITY_TYPE_KEY =
        EntityTypeKey.create(Command.class, "Account");

    // Command
    // #reply-command
    interface Command extends CborSerializable {}
    // #reply-command

    public static class CreateAccount implements Command {
      public final ActorRef<StatusReply<Done>> replyTo;

      @JsonCreator
      public CreateAccount(ActorRef<StatusReply<Done>> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public static class Deposit implements Command {
      public final BigDecimal amount;
      public final ActorRef<StatusReply<Done>> replyTo;

      public Deposit(BigDecimal amount, ActorRef<StatusReply<Done>> replyTo) {
        this.replyTo = replyTo;
        this.amount = amount;
      }
    }

    public static class Withdraw implements Command {
      public final BigDecimal amount;
      public final ActorRef<StatusReply<Done>> replyTo;

      public Withdraw(BigDecimal amount, ActorRef<StatusReply<Done>> replyTo) {
        this.amount = amount;
        this.replyTo = replyTo;
      }
    }

    public static class GetBalance implements Command {
      public final ActorRef<CurrentBalance> replyTo;

      @JsonCreator
      public GetBalance(ActorRef<CurrentBalance> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public static class CloseAccount implements Command {
      public final ActorRef<StatusReply<Done>> replyTo;

      @JsonCreator
      public CloseAccount(ActorRef<StatusReply<Done>> replyTo) {
        this.replyTo = replyTo;
      }
    }

    // Reply
    public static class CurrentBalance implements CborSerializable {
      public final BigDecimal balance;

      @JsonCreator
      public CurrentBalance(BigDecimal balance) {
        this.balance = balance;
      }
    }

    // State
    interface Account extends CborSerializable {}

    public static class OpenedAccount implements Account {
      public final BigDecimal balance;

      public OpenedAccount() {
        this.balance = BigDecimal.ZERO;
      }

      @JsonCreator
      public OpenedAccount(BigDecimal balance) {
        this.balance = balance;
      }

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

    public static class ClosedAccount implements Account {}

    public static AccountEntity create(String accountNumber, PersistenceId persistenceId) {
      return new AccountEntity(accountNumber, persistenceId);
    }

    private final String accountNumber;

    private AccountEntity(String accountNumber, PersistenceId persistenceId) {
      super(persistenceId);
      this.accountNumber = accountNumber;
    }

    @Override
    public Account emptyState() {
      return null;
    }

    @Override
    public CommandHandlerWithReply<Command, Account> commandHandler() {
      CommandHandlerWithReplyBuilder<Command, Account> builder =
          newCommandHandlerWithReplyBuilder();

      builder.forNullState().onCommand(CreateAccount.class, this::createAccount);

      builder
          .forStateType(OpenedAccount.class)
          .onCommand(Deposit.class, this::deposit)
          .onCommand(Withdraw.class, this::withdraw)
          .onCommand(GetBalance.class, this::getBalance)
          .onCommand(CloseAccount.class, this::closeAccount);

      builder
          .forStateType(ClosedAccount.class)
          .onAnyCommand(() -> Effect().unhandled().thenNoReply());

      return builder.build();
    }

    private ReplyEffect<Account> createAccount(CreateAccount command) {
      return Effect()
          .persist(new OpenedAccount())
          .thenReply(command.replyTo, account2 -> StatusReply.ack());
    }

    private ReplyEffect<Account> deposit(OpenedAccount account, Deposit command) {
      return Effect()
          .persist(account.makeDeposit(command.amount))
          .thenReply(command.replyTo, account2 -> StatusReply.ack());
    }

    // #reply
    private ReplyEffect<Account> withdraw(OpenedAccount account, Withdraw command) {
      if (!account.canWithdraw(command.amount)) {
        return Effect()
            .reply(
                command.replyTo,
                StatusReply.error("not enough funds to withdraw " + command.amount));
      } else {
        return Effect()
            .persist(account.makeWithdraw(command.amount))
            .thenReply(command.replyTo, account2 -> StatusReply.ack());
      }
    }
    // #reply

    private ReplyEffect<Account> getBalance(OpenedAccount account, GetBalance command) {
      return Effect().reply(command.replyTo, new CurrentBalance(account.balance));
    }

    private ReplyEffect<Account> closeAccount(OpenedAccount account, CloseAccount command) {
      if (account.balance.equals(BigDecimal.ZERO)) {
        return Effect()
            .persist(account.closedAccount())
            .thenReply(command.replyTo, account2 -> StatusReply.ack());
      } else {
        return Effect()
            .reply(command.replyTo, StatusReply.error("balance must be zero for closing account"));
      }
    }
  }

  // #account-entity
}
