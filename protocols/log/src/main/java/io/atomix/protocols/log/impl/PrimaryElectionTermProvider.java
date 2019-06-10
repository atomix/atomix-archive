/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.log.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import io.atomix.api.controller.PrimaryTerm;
import io.atomix.protocols.log.Term;
import io.atomix.protocols.log.TermProvider;
import io.atomix.server.management.PrimaryElectionEvent;
import io.atomix.server.management.PrimaryElectionService;

/**
 * Primary election service based term provider.
 */
public class PrimaryElectionTermProvider implements TermProvider {
  private final PrimaryElectionService election;
  private final Map<Consumer<Term>, Consumer<PrimaryElectionEvent>> eventListeners = new ConcurrentHashMap<>();

  public PrimaryElectionTermProvider(PrimaryElectionService election) {
    this.election = election;
  }

  private Term toTerm(PrimaryTerm term) {
    return new Term(
        term.getTerm(),
        term.getPrimary(),
        term.getCandidatesList());
  }

  @Override
  public CompletableFuture<Term> getTerm() {
    return election.getTerm().thenApply(this::toTerm);
  }

  @Override
  public CompletableFuture<Term> join() {
    return election.enter().thenApply(this::toTerm);
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
