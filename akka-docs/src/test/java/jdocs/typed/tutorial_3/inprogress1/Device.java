/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.typed.tutorial_3.inprogress1;

/*
//#read-protocol-1
package com.example;

//#read-protocol-1
*/

// #read-protocol-1
import akka.actor.typed.ActorRef;
import java.util.Optional;

public class Device {

  public interface Command {}

  public static final class ReadTemperature implements Command {
    final ActorRef<RespondTemperature> replyTo;

    public ReadTemperature(ActorRef<RespondTemperature> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static final class RespondTemperature {
    final Optional<Double> value;

    public RespondTemperature(Optional<Double> value) {
      this.value = value;
    }
  }
}
// #read-protocol-1
