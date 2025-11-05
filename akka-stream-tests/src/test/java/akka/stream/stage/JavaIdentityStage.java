/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.stage;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;

public class JavaIdentityStage<T> extends GraphStage<FlowShape<T, T>> {
  private Inlet<T> _in = Inlet.create("Identity.in");
  private Outlet<T> _out = Outlet.create("Identity.out");
  private FlowShape<T, T> _shape = FlowShape.of(_in, _out);

  public Inlet<T> in() {
    return _in;
  }

  public Outlet<T> out() {
    return _out;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) {
    return new GraphStageLogic(shape()) {

      {
        setHandler(
            in(),
            new AbstractInHandler() {
              @Override
              public void onPush() {
                push(out(), grab(in()));
              }
            });

        setHandler(
            out(),
            new AbstractOutHandler() {
              @Override
              public void onPull() {
                pull(in());
              }
            });
      }
    };
  }

  @Override
  public FlowShape<T, T> shape() {
    return _shape;
  }
}
