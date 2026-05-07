/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.Effect;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import java.time.Duration;
import java.util.Optional;

public interface StashingExample {

  // #stashing
  public class TaskManager
      extends EventSourcedBehavior<TaskManager.Command, TaskManager.Event, TaskManager.State> {

    public interface Command {}

    public static final class StartTask implements Command {
      public final String taskId;

      public StartTask(String taskId) {
        this.taskId = taskId;
      }
    }

    public static final class NextStep implements Command {
      public final String taskId;
      public final String instruction;

      public NextStep(String taskId, String instruction) {
        this.taskId = taskId;
        this.instruction = instruction;
      }
    }

    public static final class EndTask implements Command {
      public final String taskId;

      public EndTask(String taskId) {
        this.taskId = taskId;
      }
    }

    public interface Event {}

    public static final class TaskStarted implements Event {
      public final String taskId;

      public TaskStarted(String taskId) {
        this.taskId = taskId;
      }
    }

    public static final class TaskStep implements Event {
      public final String taskId;
      public final String instruction;

      public TaskStep(String taskId, String instruction) {
        this.taskId = taskId;
        this.instruction = instruction;
      }
    }

    public static final class TaskCompleted implements Event {
      public final String taskId;

      public TaskCompleted(String taskId) {
        this.taskId = taskId;
      }
    }

    public static class State {
      public final Optional<String> taskIdInProgress;

      public State(Optional<String> taskIdInProgress) {
        this.taskIdInProgress = taskIdInProgress;
      }
    }

    public static Behavior<Command> create(PersistenceId persistenceId) {
      return new TaskManager(persistenceId);
    }

    public TaskManager(PersistenceId persistenceId) {
      super(
          persistenceId,
          SupervisorStrategy.restartWithBackoff(
              Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2));
    }

    @Override
    public State emptyState() {
      return new State(Optional.empty());
    }

    @Override
    public CommandHandler<Command, Event, State> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(StartTask.class, this::onStartTask)
          .onCommand(NextStep.class, this::onNextStep)
          .onCommand(EndTask.class, this::onEndTask)
          .build();
    }

    private Effect<Event, State> onStartTask(State state, StartTask command) {
      if (state.taskIdInProgress.isPresent()) {
        if (state.taskIdInProgress.get().equals(command.taskId))
          return Effect().none(); // duplicate, already in progress
        else return Effect().stash(); // other task in progress, wait with new task until later
      } else {
        return Effect().persist(new TaskStarted(command.taskId));
      }
    }

    private Effect<Event, State> onNextStep(State state, NextStep command) {
      if (state.taskIdInProgress.isPresent()) {
        if (state.taskIdInProgress.get().equals(command.taskId))
          return Effect().persist(new TaskStep(command.taskId, command.instruction));
        else return Effect().stash(); // other task in progress, wait with new task until later
      } else {
        return Effect().unhandled();
      }
    }

    private Effect<Event, State> onEndTask(State state, EndTask command) {
      if (state.taskIdInProgress.isPresent()) {
        if (state.taskIdInProgress.get().equals(command.taskId))
          return Effect()
              .persist(new TaskCompleted(command.taskId))
              .thenUnstashAll(); // continue with next task
        else return Effect().stash(); // other task in progress, wait with new task until later
      } else {
        return Effect().unhandled();
      }
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
      return newEventHandlerBuilder()
          .forAnyState()
          .onEvent(TaskStarted.class, (state, event) -> new State(Optional.of(event.taskId)))
          .onEvent(TaskStep.class, (state, event) -> state)
          .onEvent(TaskCompleted.class, (state, event) -> new State(Optional.empty()))
          .build();
    }
  }
  // #stashing
}
