/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Messages {
  public
  // #immutable-message
  static class ImmutableMessage {
    private final int sequenceNumber;
    private final List<String> values;

    public ImmutableMessage(int sequenceNumber, List<String> values) {
      this.sequenceNumber = sequenceNumber;
      this.values = Collections.unmodifiableList(new ArrayList<String>(values));
    }

    public int getSequenceNumber() {
      return sequenceNumber;
    }

    public List<String> getValues() {
      return values;
    }
  }
  // #immutable-message

  public static class DoIt {
    private final ImmutableMessage msg;

    DoIt(ImmutableMessage msg) {
      this.msg = msg;
    }

    public ImmutableMessage getMsg() {
      return msg;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DoIt doIt = (DoIt) o;

      if (!msg.equals(doIt.msg)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return msg.hashCode();
    }

    @Override
    public String toString() {
      return "DoIt{" + "msg=" + msg + '}';
    }
  }

  public static class Message {
    final String str;

    Message(String str) {
      this.str = str;
    }

    public String getStr() {
      return str;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Message message = (Message) o;

      if (!str.equals(message.str)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return str.hashCode();
    }

    @Override
    public String toString() {
      return "Message{" + "str='" + str + '\'' + '}';
    }
  }

  public enum Swap {
    Swap
  }

  public static class Result {
    final String x;
    final String s;

    public Result(String x, String s) {
      this.x = x;
      this.s = s;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((s == null) ? 0 : s.hashCode());
      result = prime * result + ((x == null) ? 0 : x.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Result other = (Result) obj;
      if (s == null) {
        if (other.s != null) return false;
      } else if (!s.equals(other.s)) return false;
      if (x == null) {
        if (other.x != null) return false;
      } else if (!x.equals(other.x)) return false;
      return true;
    }
  }
}
