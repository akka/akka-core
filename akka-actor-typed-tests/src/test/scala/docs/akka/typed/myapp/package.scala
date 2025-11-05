/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

//#loggerops-package-implicit
import scala.language.implicitConversions
import akka.actor.typed.scaladsl.LoggerOps
import org.slf4j.Logger

package object myapp {

  implicit def loggerOps(logger: Logger): LoggerOps =
    LoggerOps(logger)

}
//#loggerops-package-implicit
