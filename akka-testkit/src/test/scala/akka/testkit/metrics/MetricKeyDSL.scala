/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.testkit.metrics

trait MetricKeyDSL {

  case class MetricKey private[MetricKeyDSL] (path: String) {

    import MetricKey._

    def /(key: String): MetricKey = MetricKey(path + "." + sanitizeMetricKeyPart(key))

    override def toString = path
  }

  object MetricKey {
    def fromString(root: String) = MetricKey(sanitizeMetricKeyPart(root))

    // todo not sure what else needs replacing, while keeping key as readable as can be
    private def sanitizeMetricKeyPart(keyPart: String) =
      keyPart
        .replaceAll("""\.\.\.""", "\u2026") // ... => …
        .replaceAll("""\.""", "-")
        .replaceAll("""[\]\[\(\)\<\>]""", "|")
        .replaceAll(" ", "-")
        .replaceAll("/", "-")
  }

}

object MetricKeyDSL extends MetricKeyDSL
