/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch.affinity;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import akka.annotation.InternalApi;
import static java.lang.invoke.MethodType.methodType;

/**
 * INTERNAL API
 */
@InternalApi
final class OnSpinWait {
    private final static MethodHandle handle;

    public final static void spinWait() throws Throwable {
        handle.invoke(); // Will be inlined as an invokeExact since the callsite matches the MH definition of () -> void
    }

    static {
        final MethodHandle noop = MethodHandles.constant(Object.class, null).asType(methodType(Void.TYPE));
        MethodHandle impl;
        try {
          impl = MethodHandles.lookup().findStatic(Thread.class, "onSpinWait", methodType(Void.TYPE));
        } catch (NoSuchMethodException nsme) {
          impl = noop;
        } catch (IllegalAccessException iae) {
          impl = noop;
        }
        handle = impl;
  };
}
