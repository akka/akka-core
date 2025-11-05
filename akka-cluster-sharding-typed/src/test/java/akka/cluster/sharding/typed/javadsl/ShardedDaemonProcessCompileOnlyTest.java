/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.javadsl;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.ShardCoordinator;
import akka.cluster.sharding.typed.ClusterShardingSettings;
import akka.cluster.sharding.typed.ShardedDaemonProcessCommand;
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ShardedDaemonProcessCompileOnlyTest {

  interface Command {}

  enum Stop implements Command {
    INSTANCE
  }

  {
    ActorSystem<Void> system = null;
    ShardedDaemonProcess.get(system).init(Command.class, "MyName", 8, id -> Behaviors.empty());

    ShardedDaemonProcess.get(system)
        .init(
            Command.class,
            "MyName",
            8,
            id -> Behaviors.empty(),
            ShardedDaemonProcessSettings.create(system),
            Optional.of(Stop.INSTANCE));

    // #tag-processing
    List<String> tags = Arrays.asList("tag-1", "tag-2", "tag-3");
    ShardedDaemonProcess.get(system)
        .init(
            TagProcessor.Command.class,
            "TagProcessors",
            tags.size(),
            id -> TagProcessor.create(tags.get(id)));
    // #tag-processing
  }

  static class TagProcessor {
    interface Command {}

    static Behavior<Command> create(String tag) {
      return Behaviors.setup(
          context -> {
            // ... start the tag processing ...
            return Behaviors.empty();
          });
    }
  }

  void coverAllFactories(ActorSystem<Void> system) {
    ShardedDaemonProcess.get(system)
        .init(TagProcessor.Command.class, "name", 8, id -> Behaviors.empty());

    ShardedDaemonProcessSettings settings =
        ShardedDaemonProcessSettings.create(system)
            .withKeepAliveFromNumberOfNodes(7)
            .withKeepAliveInterval(Duration.ofSeconds(2))
            .withKeepAliveThrottleInterval(Duration.ofMillis(200))
            .withShardingSettings(ClusterShardingSettings.create(system));

    ShardedDaemonProcess.get(system)
        .init(
            TagProcessor.Command.class,
            "name",
            8,
            id -> Behaviors.empty(),
            settings,
            Optional.empty());

    ShardedDaemonProcess.get(system)
        .init(
            TagProcessor.Command.class,
            "name",
            8,
            id -> Behaviors.empty(),
            settings,
            Optional.empty(),
            Optional.of(new ShardCoordinator.LeastShardAllocationStrategy(10, 100)));

    ActorRef<ShardedDaemonProcessCommand> control1 =
        ShardedDaemonProcess.get(system)
            .initWithContext(TagProcessor.Command.class, "name", 8, id -> Behaviors.empty());

    ActorRef<ShardedDaemonProcessCommand> control2 =
        ShardedDaemonProcess.get(system)
            .initWithContext(
                TagProcessor.Command.class,
                "name",
                8,
                id -> Behaviors.empty(),
                settings,
                Optional.empty());

    ActorRef<ShardedDaemonProcessCommand> control3 =
        ShardedDaemonProcess.get(system)
            .initWithContext(
                TagProcessor.Command.class,
                "name",
                8,
                id -> Behaviors.empty(),
                settings,
                Optional.empty(),
                Optional.empty());
  }
}
