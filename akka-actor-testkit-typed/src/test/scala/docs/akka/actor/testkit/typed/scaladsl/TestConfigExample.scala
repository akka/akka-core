/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.actor.testkit.typed.scaladsl

object TestConfigExample {

  def illustrateApplicationConfig(): Unit = {

    //#default-application-conf
    import com.typesafe.config.ConfigFactory

    ConfigFactory.load()
    //#default-application-conf

    //#parse-string
    ConfigFactory.parseString("""
      akka.loglevel = DEBUG
      akka.log-config-on-start = on
      """)
    //#parse-string

    //#fallback-application-conf
    ConfigFactory.parseString("""
      akka.loglevel = DEBUG
      akka.log-config-on-start = on
      """).withFallback(ConfigFactory.load())
    //#fallback-application-conf
  }
}
