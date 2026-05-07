/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.actor.testkit.typed.javadsl;

// #default-application-conf
import com.typesafe.config.ConfigFactory;

// #default-application-conf

public class TestConfigExample {

  void illustrateApplicationConfig() {

    // #default-application-conf
    ConfigFactory.load()
    // #default-application-conf
    ;

    // #parse-string
    ConfigFactory.parseString("akka.loglevel = DEBUG \n" + "akka.log-config-on-start = on \n")
    // #parse-string
    ;

    // #fallback-application-conf
    ConfigFactory.parseString("akka.loglevel = DEBUG \n" + "akka.log-config-on-start = on \n")
        .withFallback(ConfigFactory.load())
    // #fallback-application-conf
    ;
  }
}
