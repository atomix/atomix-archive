/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.collect.Sets;

/**
 * Test primary election.
 */
public class TestTermProvider implements TermProvider {
  private final String memberId;
  private final Context context;

  TestTermProvider(String memberId, Context context) {
    this.memberId = memberId;
    this.context = context;
  }

  @Override
  public CompletableFuture<Void> join() {
    if (context.term == null) {
      context.term = new Term(++context.counter, memberId, context.followers);
    } else {
      context.followers.add(memberId);
    }
    context.listeners.forEach(l -> l.accept(context.term));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> leave() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Term> getTerm() {
    return CompletableFuture.completedFuture(context.term);
  }

  @Override
  public CompletableFuture<Void> addListener(Consumer<Term> listener) {
    context.listeners.add(listener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(Consumer<Term> listener) {
    context.listeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }

  public static class Context {
    private long counter;
    private Term term;
    private List<String> followers = new ArrayList<>();
    private final Set<Consumer<Term>> listeners = Sets.newConcurrentHashSet();
  }
}
