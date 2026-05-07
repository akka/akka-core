/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.annotation;

import java.lang.annotation.*;

/**
 * Marks APIs that are considered internal to Akka and may change at any point in time without any
 * warning.
 *
 * <p>For example, this annotation should be used when the Scala {@code private[akka]} access
 * restriction is used, as Java has no way of representing this package restricted access and such
 * methods and classes are represented as {@code public} in byte-code.
 *
 * <p>If a method/class annotated with this method has a javadoc/scaladoc comment, the first line
 * MUST include {@code INTERNAL API} in order to be easily identifiable from generated
 * documentation. Additional information may be put on the same line as the INTERNAL API comment in
 * order to clarify further.
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
public @interface InternalApi {}
