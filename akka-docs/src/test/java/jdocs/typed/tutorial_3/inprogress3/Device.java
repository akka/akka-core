/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.typed.tutorial_3.inprogress3;

public class Device {

  public interface Command {}

  // #write-protocol-1
  public static final class RecordTemperature implements Command {
    final double value;

    public RecordTemperature(double value) {
      this.value = value;
    }
  }
  // #write-protocol-1
}
