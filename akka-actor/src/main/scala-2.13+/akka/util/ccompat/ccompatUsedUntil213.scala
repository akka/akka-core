/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package akka.util.ccompat

import scala.annotation.Annotation

import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * Annotation to mark files that need ccompat to be imported for Scala 2.11 and/or 2.12,
 * but not 2.13. Gets rid of the 'unused import' warning on 2.13.
 */
@InternalApi
private[akka] class ccompatUsedUntil213 extends Annotation
