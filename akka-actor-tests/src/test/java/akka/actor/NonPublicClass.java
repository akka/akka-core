/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.actor;

public class NonPublicClass {
  public static Props createProps() {
    return Props.create(MyNonPublicActorClass.class);
  }
}

class MyNonPublicActorClass extends UntypedAbstractActor {
  @Override
  public void onReceive(Object msg) {
    getSender().tell(msg, getSelf());
  }
}
