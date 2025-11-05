/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.ddata;

import akka.cluster.ddata.AbstractReplicatedData;
import akka.cluster.ddata.GSet;
import java.util.HashSet;
import java.util.Set;

// #twophaseset
public class TwoPhaseSet extends AbstractReplicatedData<TwoPhaseSet> {

  public final GSet<String> adds;
  public final GSet<String> removals;

  public TwoPhaseSet(GSet<String> adds, GSet<String> removals) {
    this.adds = adds;
    this.removals = removals;
  }

  public static TwoPhaseSet create() {
    return new TwoPhaseSet(GSet.create(), GSet.create());
  }

  public TwoPhaseSet add(String element) {
    return new TwoPhaseSet(adds.add(element), removals);
  }

  public TwoPhaseSet remove(String element) {
    return new TwoPhaseSet(adds, removals.add(element));
  }

  public Set<String> getElements() {
    Set<String> result = new HashSet<>(adds.getElements());
    result.removeAll(removals.getElements());
    return result;
  }

  @Override
  public TwoPhaseSet mergeData(TwoPhaseSet that) {
    return new TwoPhaseSet(this.adds.merge(that.adds), this.removals.merge(that.removals));
  }
}
// #twophaseset
