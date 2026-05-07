/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks APIs that are considered internal to Akka and should not be accessed by user code but that
 * are used across Akka project boundaries and therefore shouldn't be changed without considering
 * possible usage outside of the Akka core modules.
 *
 * <p>If a method/class annotated with this annotation is part of a public API and has the Scala
 * {@code private[akka]} access restriction, which leads to a public method from Java, there should
 * be a javadoc/scaladoc comment where the first line MUST include {@code INTERNAL API} in order to
 * be easily identifiable from generated documentation. Additional information may be put on the
 * same line as the INTERNAL API comment in order to clarify further.
 */
@Documented
@Retention(RetentionPolicy.CLASS) // to be accessible by MiMa
@Target({
  ElementType.METHOD,
  ElementType.CONSTRUCTOR,
  ElementType.FIELD,
  ElementType.TYPE,
  ElementType.PACKAGE
})
public @interface InternalStableApi {}
