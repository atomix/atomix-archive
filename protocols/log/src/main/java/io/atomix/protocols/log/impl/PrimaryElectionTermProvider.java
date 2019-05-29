package io.atomix.protocols.log.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.atomix.api.partition.GroupMember;
import io.atomix.api.partition.PrimaryTerm;
import io.atomix.protocols.log.Term;
import io.atomix.protocols.log.TermProvider;
import io.atomix.server.management.PrimaryElection;
import io.atomix.server.management.PrimaryElectionEvent;

/**
 * Primary election service based term provider.
 */
public class PrimaryElectionTermProvider implements TermProvider {
  private final PrimaryElection election;
  private final GroupMember member;
  private final Map<Consumer<Term>, Consumer<PrimaryElectionEvent>> eventListeners = new ConcurrentHashMap<>();

  public PrimaryElectionTermProvider(PrimaryElection election, GroupMember member) {
    this.election = election;
    this.member = member;
  }

  private Term toTerm(PrimaryTerm term) {
    return new Term(
        term.getTerm(),
        term.getPrimary().getMember(),
        term.getCandidatesList().stream()
            .map(GroupMember::getMember)
            .collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Term> getTerm() {
    return election.getTerm().thenApply(this::toTerm);
  }

  @Override
  public CompletableFuture<Term> join() {
    return election.enter(member).thenApply(this::toTerm);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(Consumer<Term> listener) {
    Consumer<PrimaryElectionEvent> eventListener = event -> listener.accept(toTerm(event.term()));
    eventListeners.put(listener, eventListener);
    return election.addListener(eventListener);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(Consumer<Term> listener) {
    Consumer<PrimaryElectionEvent> eventListener = eventListeners.remove(listener);
    return eventListener != null ? election.removeListener(eventListener) : CompletableFuture.completedFuture(null);
  }
}
