package io.atomix.primitive.partition.impl;

import io.atomix.primitive.event.EventType;

/**
 * Primary elector events.
 */
public enum PrimaryElectorEvents implements EventType {
  CHANGE("change");

  private final String id;

  PrimaryElectorEvents(String id) {
    this.id = id;
  }

  @Override
  public String id() {
    return id;
  }
}
