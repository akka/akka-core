/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.Attributes;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.DebugFilter;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import jdocs.stream.SilenceSystemOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.runtime.AbstractFunction0;

public class RecipeLoggingElements extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system =
        ActorSystem.create(
            "RecipeLoggingElements",
            ConfigFactory.parseString(
                "akka.loglevel=DEBUG\nakka.loggers = [akka.testkit.TestEventListener]"));
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void workWithPrintln() throws Exception {
    new TestKit(system) {
      final SilenceSystemOut.System System = SilenceSystemOut.get(getTestActor());

      {
        final Source<String, NotUsed> mySource = Source.from(Arrays.asList("1", "2", "3"));

        // #println-debug
        mySource.map(
            elem -> {
              System.out.println(elem);
              return elem;
            });
        // #println-debug
      }
    };
  }

  @Test
  public void workWithLog() throws Exception {
    new TestKit(system) {
      private <T> T analyse(T i) {
        return i;
      }

      {
        final Source<String, NotUsed> mySource = Source.from(Arrays.asList("1", "2", "3"));

        // #log-custom
        // customise log levels
        mySource
            .log("before-map")
            .withAttributes(
                Attributes.createLogLevels(
                    Logging.WarningLevel(), // onElement
                    Logging.InfoLevel(), // onFinish
                    Logging.DebugLevel() // onFailure
                    ))
            .map(i -> analyse(i));

        // or provide custom logging adapter
        final LoggingAdapter adapter = Logging.getLogger(system, "customLogger");
        mySource.log("custom", adapter);
        // #log-custom

        new DebugFilter("customLogger", "[custom] Element: ", false, false, 3)
            .intercept(
                new AbstractFunction0<Object>() {
                  public Void apply() {
                    mySource.log("custom", adapter).runWith(Sink.ignore(), system);
                    return null;
                  }
                },
                system);
      }
    };
  }

  @Test
  public void errorLog() throws Exception {
    new TestKit(system) {
      {
        // #log-error
        Source.from(Arrays.asList(-1, 0, 1))
            .map(x -> 1 / x) // throwing ArithmeticException: / by zero
            .log("error logging")
            .runWith(Sink.ignore(), system);
        // #log-error
      }
    };
  }
}
