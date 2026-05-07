/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.typed.tutorial_5;

import static jdocs.typed.tutorial_5.DeviceManager.DeviceNotAvailable;
import static jdocs.typed.tutorial_5.DeviceManager.DeviceTimedOut;
import static jdocs.typed.tutorial_5.DeviceManager.RespondAllTemperatures;
import static jdocs.typed.tutorial_5.DeviceManager.Temperature;
import static jdocs.typed.tutorial_5.DeviceManager.TemperatureNotAvailable;
import static jdocs.typed.tutorial_5.DeviceManager.TemperatureReading;
import static org.junit.Assert.assertEquals;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class DeviceGroupQueryTest extends JUnitSuite {

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  // #query-test-normal
  @Test
  public void testReturnTemperatureValueForWorkingDevices() {
    TestProbe<RespondAllTemperatures> requester =
        testKit.createTestProbe(RespondAllTemperatures.class);
    TestProbe<Device.Command> device1 = testKit.createTestProbe(Device.Command.class);
    TestProbe<Device.Command> device2 = testKit.createTestProbe(Device.Command.class);

    Map<String, ActorRef<Device.Command>> deviceIdToActor = new HashMap<>();
    deviceIdToActor.put("device1", device1.getRef());
    deviceIdToActor.put("device2", device2.getRef());

    ActorRef<DeviceGroupQuery.Command> queryActor =
        testKit.spawn(
            DeviceGroupQuery.create(
                deviceIdToActor, 1L, requester.getRef(), Duration.ofSeconds(3)));

    device1.expectMessageClass(Device.ReadTemperature.class);
    device2.expectMessageClass(Device.ReadTemperature.class);

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device1", Optional.of(1.0))));

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device2", Optional.of(2.0))));

    RespondAllTemperatures response = requester.receiveMessage();
    assertEquals(1L, response.requestId);

    Map<String, TemperatureReading> expectedTemperatures = new HashMap<>();
    expectedTemperatures.put("device1", new Temperature(1.0));
    expectedTemperatures.put("device2", new Temperature(2.0));

    assertEquals(expectedTemperatures, response.temperatures);
  }
  // #query-test-normal

  // #query-test-no-reading
  @Test
  public void testReturnTemperatureNotAvailableForDevicesWithNoReadings() {
    TestProbe<RespondAllTemperatures> requester =
        testKit.createTestProbe(RespondAllTemperatures.class);
    TestProbe<Device.Command> device1 = testKit.createTestProbe(Device.Command.class);
    TestProbe<Device.Command> device2 = testKit.createTestProbe(Device.Command.class);

    Map<String, ActorRef<Device.Command>> deviceIdToActor = new HashMap<>();
    deviceIdToActor.put("device1", device1.getRef());
    deviceIdToActor.put("device2", device2.getRef());

    ActorRef<DeviceGroupQuery.Command> queryActor =
        testKit.spawn(
            DeviceGroupQuery.create(
                deviceIdToActor, 1L, requester.getRef(), Duration.ofSeconds(3)));

    assertEquals(0L, device1.expectMessageClass(Device.ReadTemperature.class).requestId);
    assertEquals(0L, device2.expectMessageClass(Device.ReadTemperature.class).requestId);

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device1", Optional.empty())));

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device2", Optional.of(2.0))));

    RespondAllTemperatures response = requester.receiveMessage();
    assertEquals(1L, response.requestId);

    Map<String, TemperatureReading> expectedTemperatures = new HashMap<>();
    expectedTemperatures.put("device1", TemperatureNotAvailable.INSTANCE);
    expectedTemperatures.put("device2", new Temperature(2.0));

    assertEquals(expectedTemperatures, response.temperatures);
  }
  // #query-test-no-reading

  // #query-test-stopped
  @Test
  public void testReturnDeviceNotAvailableIfDeviceStopsBeforeAnswering() {
    TestProbe<RespondAllTemperatures> requester =
        testKit.createTestProbe(RespondAllTemperatures.class);
    TestProbe<Device.Command> device1 = testKit.createTestProbe(Device.Command.class);
    TestProbe<Device.Command> device2 = testKit.createTestProbe(Device.Command.class);

    Map<String, ActorRef<Device.Command>> deviceIdToActor = new HashMap<>();
    deviceIdToActor.put("device1", device1.getRef());
    deviceIdToActor.put("device2", device2.getRef());

    ActorRef<DeviceGroupQuery.Command> queryActor =
        testKit.spawn(
            DeviceGroupQuery.create(
                deviceIdToActor, 1L, requester.getRef(), Duration.ofSeconds(3)));

    assertEquals(0L, device1.expectMessageClass(Device.ReadTemperature.class).requestId);
    assertEquals(0L, device2.expectMessageClass(Device.ReadTemperature.class).requestId);

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device1", Optional.of(1.0))));

    device2.stop();

    RespondAllTemperatures response = requester.receiveMessage();
    assertEquals(1L, response.requestId);

    Map<String, TemperatureReading> expectedTemperatures = new HashMap<>();
    expectedTemperatures.put("device1", new Temperature(1.0));
    expectedTemperatures.put("device2", DeviceNotAvailable.INSTANCE);

    assertEquals(expectedTemperatures, response.temperatures);
  }
  // #query-test-stopped

  // #query-test-stopped-later
  @Test
  public void testReturnTemperatureReadingEvenIfDeviceStopsAfterAnswering() {
    TestProbe<RespondAllTemperatures> requester =
        testKit.createTestProbe(RespondAllTemperatures.class);
    TestProbe<Device.Command> device1 = testKit.createTestProbe(Device.Command.class);
    TestProbe<Device.Command> device2 = testKit.createTestProbe(Device.Command.class);

    Map<String, ActorRef<Device.Command>> deviceIdToActor = new HashMap<>();
    deviceIdToActor.put("device1", device1.getRef());
    deviceIdToActor.put("device2", device2.getRef());

    ActorRef<DeviceGroupQuery.Command> queryActor =
        testKit.spawn(
            DeviceGroupQuery.create(
                deviceIdToActor, 1L, requester.getRef(), Duration.ofSeconds(3)));

    assertEquals(0L, device1.expectMessageClass(Device.ReadTemperature.class).requestId);
    assertEquals(0L, device2.expectMessageClass(Device.ReadTemperature.class).requestId);

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device1", Optional.of(1.0))));

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device2", Optional.of(2.0))));

    device2.stop();

    RespondAllTemperatures response = requester.receiveMessage();
    assertEquals(1L, response.requestId);

    Map<String, TemperatureReading> expectedTemperatures = new HashMap<>();
    expectedTemperatures.put("device1", new Temperature(1.0));
    expectedTemperatures.put("device2", new Temperature(2.0));

    assertEquals(expectedTemperatures, response.temperatures);
  }
  // #query-test-stopped-later

  // #query-test-timeout
  @Test
  public void testReturnDeviceTimedOutIfDeviceDoesNotAnswerInTime() {
    TestProbe<RespondAllTemperatures> requester =
        testKit.createTestProbe(RespondAllTemperatures.class);
    TestProbe<Device.Command> device1 = testKit.createTestProbe(Device.Command.class);
    TestProbe<Device.Command> device2 = testKit.createTestProbe(Device.Command.class);

    Map<String, ActorRef<Device.Command>> deviceIdToActor = new HashMap<>();
    deviceIdToActor.put("device1", device1.getRef());
    deviceIdToActor.put("device2", device2.getRef());

    ActorRef<DeviceGroupQuery.Command> queryActor =
        testKit.spawn(
            DeviceGroupQuery.create(
                deviceIdToActor, 1L, requester.getRef(), Duration.ofMillis(200)));

    assertEquals(0L, device1.expectMessageClass(Device.ReadTemperature.class).requestId);
    assertEquals(0L, device2.expectMessageClass(Device.ReadTemperature.class).requestId);

    queryActor.tell(
        new DeviceGroupQuery.WrappedRespondTemperature(
            new Device.RespondTemperature(0L, "device1", Optional.of(1.0))));

    // no reply from device2

    RespondAllTemperatures response = requester.receiveMessage();
    assertEquals(1L, response.requestId);

    Map<String, TemperatureReading> expectedTemperatures = new HashMap<>();
    expectedTemperatures.put("device1", new Temperature(1.0));
    expectedTemperatures.put("device2", DeviceTimedOut.INSTANCE);

    assertEquals(expectedTemperatures, response.temperatures);
  }
  // #query-test-timeout
}
