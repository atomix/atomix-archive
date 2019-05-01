package io.atomix.primitive.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryTerm;

/**
 * Primary elector primitive.
 */
public interface PrimaryElector extends AsyncPrimitive {

  /**
   * Enters the primary election.
   * <p>
   * When entering a primary election, the provided {@link GroupMember} will be added to the election's candidate list.
   * The returned term is representative of the term <em>after</em> the member joins the election. Thus, if the
   * joining member is immediately elected primary, the returned term should reflect that.
   *
   * @param partitionId the partition for which to enter the election
   * @param member      the member to enter the election
   * @return the current term
   */
  CompletableFuture<PrimaryTerm> enter(PartitionId partitionId, GroupMember member);

  /**
   * Returns the current term.
   * <p>
   * The term is representative of the current primary, candidates, and backups in the primary election.
   *
   * @param partitionId the partition for which to get the term
   * @return the current term
   */
  CompletableFuture<PrimaryTerm> getTerm(PartitionId partitionId);

  /**
   * Adds a listener to the elector.
   *
   * @param listener the listener to add
   * @return a future to be completed once the listener has been added
   */
  CompletableFuture<Void> addListener(Consumer<PrimaryElectionEvent> listener);

  /**
   * Removes a listener to the elector.
   *
   * @param listener the listener to remove
   * @return a future to be completed once the listener has been removed
   */
  CompletableFuture<Void> removeListener(Consumer<PrimaryElectionEvent> listener);

}
