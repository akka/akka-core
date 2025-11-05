/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import java.io.Serializable;
import java.math.BigInteger;

public class FactorialResult implements Serializable {
  private static final long serialVersionUID = 1L;
  public final int n;
  public final BigInteger factorial;

  FactorialResult(int n, BigInteger factorial) {
    this.n = n;
    this.factorial = factorial;
  }
}
