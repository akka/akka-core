/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import akka.annotation.InternalApi;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/** INTERNAL API */
@InternalApi
final class AbstractActorRef {

  static final VarHandle cellHandle;
  static final VarHandle lookupHandle;

  static {
    try {
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(RepointableActorRef.class, MethodHandles.lookup());
      cellHandle =
          lookup.unreflectVarHandle(
              RepointableActorRef.class.getDeclaredField("_cellDoNotCallMeDirectly"));
      lookupHandle =
          lookup.unreflectVarHandle(
              RepointableActorRef.class.getDeclaredField("_lookupDoNotCallMeDirectly"));
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
