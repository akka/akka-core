/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.typed.tutorial_5;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// #query-full
// #query-outline
public class DeviceGroupQuery extends AbstractBehavior<DeviceGroupQuery.Command> {

  public interface Command {}

  private static enum CollectionTimeout implements Command {
    INSTANCE
  }

  static class WrappedRespondTemperature implements Command {
    final Device.RespondTemperature response;

    WrappedRespondTemperature(Device.RespondTemperature response) {
      this.response = response;
    }
  }

  private static class DeviceTerminated implements Command {
    final String deviceId;

    private DeviceTerminated(String deviceId) {
      this.deviceId = deviceId;
    }
  }

  public static Behavior<Command> create(
      Map<String, ActorRef<Device.Command>> deviceIdToActor,
      long requestId,
      ActorRef<DeviceManager.RespondAllTemperatures> requester,
      Duration timeout) {
    return Behaviors.setup(
        context ->
            Behaviors.withTimers(
                timers ->
                    new DeviceGroupQuery(
                        deviceIdToActor, requestId, requester, timeout, context, timers)));
  }

  private final long requestId;
  private final ActorRef<DeviceManager.RespondAllTemperatures> requester;
  // #query-outline
  // #query-state
  private Map<String, DeviceManager.TemperatureReading> repliesSoFar = new HashMap<>();
  private final Set<String> stillWaiting;

  // #query-state
  // #query-outline

  private DeviceGroupQuery(
      Map<String, ActorRef<Device.Command>> deviceIdToActor,
      long requestId,
      ActorRef<DeviceManager.RespondAllTemperatures> requester,
      Duration timeout,
      ActorContext<Command> context,
      TimerScheduler<Command> timers) {
    super(context);
    this.requestId = requestId;
    this.requester = requester;

    timers.startSingleTimer(CollectionTimeout.INSTANCE, timeout);

    ActorRef<Device.RespondTemperature> respondTemperatureAdapter =
        context.messageAdapter(Device.RespondTemperature.class, WrappedRespondTemperature::new);

    for (Map.Entry<String, ActorRef<Device.Command>> entry : deviceIdToActor.entrySet()) {
      context.watchWith(entry.getValue(), new DeviceTerminated(entry.getKey()));
      entry.getValue().tell(new Device.ReadTemperature(0L, respondTemperatureAdapter));
    }
    stillWaiting = new HashSet<>(deviceIdToActor.keySet());
  }

  // #query-outline
  // #query-state
  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(WrappedRespondTemperature.class, this::onRespondTemperature)
        .onMessage(DeviceTerminated.class, this::onDeviceTerminated)
        .onMessage(CollectionTimeout.class, this::onCollectionTimeout)
        .build();
  }

  private Behavior<Command> onRespondTemperature(WrappedRespondTemperature r) {
    DeviceManager.TemperatureReading reading =
        r.response
            .value
            .map(v -> (DeviceManager.TemperatureReading) new DeviceManager.Temperature(v))
            .orElse(DeviceManager.TemperatureNotAvailable.INSTANCE);

    String deviceId = r.response.deviceId;
    repliesSoFar.put(deviceId, reading);
    stillWaiting.remove(deviceId);

    return respondWhenAllCollected();
  }

  private Behavior<Command> onDeviceTerminated(DeviceTerminated terminated) {
    if (stillWaiting.contains(terminated.deviceId)) {
      repliesSoFar.put(terminated.deviceId, DeviceManager.DeviceNotAvailable.INSTANCE);
      stillWaiting.remove(terminated.deviceId);
    }
    return respondWhenAllCollected();
  }

  private Behavior<Command> onCollectionTimeout(CollectionTimeout timeout) {
    for (String deviceId : stillWaiting) {
      repliesSoFar.put(deviceId, DeviceManager.DeviceTimedOut.INSTANCE);
    }
    stillWaiting.clear();
    return respondWhenAllCollected();
  }
  // #query-state

  // #query-collect-reply
  private Behavior<Command> respondWhenAllCollected() {
    if (stillWaiting.isEmpty()) {
      requester.tell(new DeviceManager.RespondAllTemperatures(requestId, repliesSoFar));
      return Behaviors.stopped();
    } else {
      return this;
    }
  }
  // #query-collect-reply
  // #query-outline

}
// #query-outline
// #query-full
