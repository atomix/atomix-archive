package io.atomix.primitive.partition.impl;

import io.atomix.primitive.event.EventType;
import io.atomix.primitive.partition.PrimaryElectionEvent;

/**
 * Primary elector events.
 */
public final class PrimaryElectorEvents {
  public static final EventType<PrimaryElectionEvent> CHANGE = new EventType<>("change");

  private PrimaryElectorEvents() {
  }
}
