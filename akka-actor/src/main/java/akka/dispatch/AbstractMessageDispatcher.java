/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

abstract class AbstractMessageDispatcher {
    private final static VarHandle shutdownScheduleHandle;
    private final static VarHandle inhabitantsHandle;

    static {
        try {
          MethodHandles.Lookup lookup =
              MethodHandles.privateLookupIn(MessageDispatcher.class, MethodHandles.lookup());
          shutdownScheduleHandle = lookup.unreflectVarHandle(MessageDispatcher.class.getDeclaredField("_shutdownScheduleDoNotCallMeDirectly"));
          inhabitantsHandle = lookup.unreflectVarHandle(MessageDispatcher.class.getDeclaredField("_inhabitantsDoNotCallMeDirectly"));
        } catch(Throwable t){
            throw new ExceptionInInitializerError(t);
        }
    }

    // Note: manual forwarders to be certain we avoid boxing the ints/longs
    long volatileGetInhabitants() {
      return (long) inhabitantsHandle.getVolatile(this);
    }

    long getAndAddInhabitants(long add) {
      return (long) inhabitantsHandle.getAndAdd(this, add);
    }

    int volatileGetShutdownSchedule() {
      return (int) shutdownScheduleHandle.getVolatile(this);
    }

    boolean compareAndSetShutdownSchedule(int expect, int update) {
      return shutdownScheduleHandle.compareAndSet(this, expect, update);
    }

}
