package io.atomix.server.management;

import io.atomix.api.partition.PrimaryTerm;
import io.atomix.utils.event.AbstractEvent;

/**
 * Primary election event.
 */
public class PrimaryElectionEvent extends AbstractEvent<PrimaryElectionEvent.Type, PrimaryTerm> {

  /**
   * Primary election event type.
   */
  public enum Type {
    TERM_CHANGED
  }

  public PrimaryElectionEvent(PrimaryElectionEvent.Type type, PrimaryTerm subject) {
    super(type, subject);
  }

  /**
   * Returns the primary term.
   *
   * @return the primary term
   */
  public PrimaryTerm term() {
    return subject();
  }
}
