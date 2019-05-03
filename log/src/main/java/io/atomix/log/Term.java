package io.atomix.log;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed log term.
 */
public class Term {
  private final long term;
  private final String leader;
  private final List<String> followers;

  public Term(long term, String leader, List<String> followers) {
    this.term = term;
    this.leader = leader;
    this.followers = followers;
  }

  /**
   * Returns the term number.
   *
   * @return the term number
   */
  public long term() {
    return term;
  }

  /**
   * Returns the leader ID.
   *
   * @return the leader ID
   */
  public String leader() {
    return leader;
  }

  /**
   * Returns a sorted list of follower IDs.
   *
   * @return a sorted list of follower IDs
   */
  public List<String> followers() {
    return followers;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("leader", leader)
        .add("followers", followers)
        .toString();
  }
}
