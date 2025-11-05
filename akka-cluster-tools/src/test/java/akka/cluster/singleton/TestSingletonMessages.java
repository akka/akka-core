/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton;

// #singleton-message-classes
public class TestSingletonMessages {
  public static class UnregistrationOk {}

  public static class End {}

  public static class Ping {}

  public static class Pong {}

  public static class GetCurrent {}

  // #singleton-message-classes
  public static class RegisterConsumer {}

  public static class UnregisterConsumer {}

  public static class RegistrationOk {}

  public static class UnexpectedRegistration {}

  public static class UnexpectedUnregistration {}

  public static class Reset {}

  public static class ResetOk {}

  public static RegisterConsumer registerConsumer() {
    return new RegisterConsumer();
  }

  public static UnregisterConsumer unregisterConsumer() {
    return new UnregisterConsumer();
  }

  public static RegistrationOk registrationOk() {
    return new RegistrationOk();
  }

  public static UnexpectedRegistration unexpectedRegistration() {
    return new UnexpectedRegistration();
  }

  public static UnexpectedUnregistration unexpectedUnregistration() {
    return new UnexpectedUnregistration();
  }

  public static Reset reset() {
    return new Reset();
  }

  public static ResetOk resetOk() {
    return new ResetOk();
  }

  // #singleton-message-classes
  public static UnregistrationOk unregistrationOk() {
    return new UnregistrationOk();
  }

  public static End end() {
    return new End();
  }

  public static Ping ping() {
    return new Ping();
  }

  public static Pong pong() {
    return new Pong();
  }

  public static GetCurrent getCurrent() {
    return new GetCurrent();
  }
}
// #singleton-message-classes
