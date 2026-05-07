/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence.testkit;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;

import com.typesafe.config.ConfigFactory;
import jdocs.AbstractJavaTest;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;

// #imports
import akka.persistence.testkit.javadsl.PersistenceInit;
import akka.Done;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

// #imports

public class PersistenceInitTest extends AbstractJavaTest {
  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(
          ConfigFactory.parseString(
                  "akka.persistence.journal.plugin = \"akka.persistence.journal.inmem\" \n"
                      + "akka.persistence.journal.inmem.test-serialization = on \n"
                      + "akka.persistence.snapshot-store.plugin = \"akka.persistence.snapshot-store.local\" \n"
                      + "akka.persistence.snapshot-store.local.dir = \"target/snapshot-"
                      + UUID.randomUUID().toString()
                      + "\" \n")
              .withFallback(ConfigFactory.defaultApplication()));

  @Test
  public void testInit() throws Exception {
    // #init
    Duration timeout = Duration.ofSeconds(5);
    CompletionStage<Done> done =
        PersistenceInit.initializeDefaultPlugins(testKit.system(), timeout);
    done.toCompletableFuture().get(timeout.getSeconds(), TimeUnit.SECONDS);
    // #init
  }
}
