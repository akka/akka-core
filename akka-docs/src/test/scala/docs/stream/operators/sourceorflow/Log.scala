/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.stream.scaladsl.Flow
//#log
import akka.stream.Attributes

//#log

object Log {
  def logExample(): Unit = {
    Flow[String]
    //#log
      .log(name = "myStream")
      .addAttributes(
        Attributes.logLevels(
          onElement = Attributes.LogLevels.Off,
          onFinish = Attributes.LogLevels.Info,
          onFailure = Attributes.LogLevels.Error))
    //#log
  }
}
