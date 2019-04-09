/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.set.impl;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Function;

import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.set.DistributedNavigableSetType;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

/**
 * Default distributed tree set service.
 */
public class DefaultDistributedNavigableSetService extends AbstractDistributedSetService<NavigableSet<String>> implements DistributedTreeSetService<String> {
  private final Serializer serializer;

  public DefaultDistributedNavigableSetService() {
    super(DistributedNavigableSetType.instance(), new ConcurrentSkipListSet<>());
    this.serializer = Serializer.using(Namespace.builder()
        .register(DistributedNavigableSetType.instance().namespace())
        .register(SessionId.class)
        .register(IteratorContext.class)
        .register(SubSetIteratorContext.class)
        .register(DescendingIteratorContext.class)
        .register(DescendingSubSetIteratorContext.class)
        .build());
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public String lower(String e) {
    return set().lower(e);
  }

  @Override
  public String floor(String e) {
    return set().floor(e);
  }

  @Override
  public String ceiling(String e) {
    return set().ceiling(e);
  }

  @Override
  public String higher(String e) {
    return set().higher(e);
  }

  @Override
  public String pollFirst() {
    return set().pollFirst();
  }

  @Override
  public String pollLast() {
    return set().pollLast();
  }

  @Override
  public String first() {
    return !set().isEmpty() ? set().first() : null;
  }

  @Override
  public String last() {
    return !set().isEmpty() ? set().last() : null;
  }

  @Override
  public String subSetFirst(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> !set.isEmpty() ? set.first() : null, fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public String subSetLast(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> !set.isEmpty() ? set.last() : null, fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public String subSetLower(String e, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> set.lower(e), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public String subSetFloor(String e, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> set.floor(e), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public String subSetCeiling(String e, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> set.ceiling(e), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public String subSetHigher(String e, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> set.higher(e), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public String subSetPollFirst(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> set.pollFirst(), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public String subSetPollLast(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> set.pollLast(), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public int subSetSize(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return subSetApply(set -> set.size(), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public void subSetClear(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    subSetAccept(set -> set.clear(), fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public IteratorBatch<String> subSetIterate(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return iterate(sessionId -> new SubSetIteratorContext(sessionId, fromElement, fromInclusive, toElement, toInclusive));
  }

  @Override
  public IteratorBatch<String> subSetIterateDescending(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    return iterate(sessionId -> new DescendingSubSetIteratorContext(sessionId, fromElement, fromInclusive, toElement, toInclusive));
  }

  @Override
  public IteratorBatch<String> iterateDescending() {
    return iterate(DescendingIteratorContext::new);
  }

  private void subSetAccept(Consumer<NavigableSet<String>> function, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    if (fromElement != null && toElement != null) {
      function.accept(set().subSet(fromElement, fromInclusive, toElement, toInclusive));
    } else if (fromElement != null) {
      function.accept(set().tailSet(fromElement, fromInclusive));
    } else if (toElement != null) {
      function.accept(set().headSet(toElement, toInclusive));
    } else {
      function.accept(set());
    }
  }

  private <T> T subSetApply(Function<NavigableSet<String>, T> function, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
    if (fromElement != null && toElement != null) {
      return function.apply(set().subSet(fromElement, fromInclusive, toElement, toInclusive));
    } else if (fromElement != null) {
      return function.apply(set().tailSet(fromElement, fromInclusive));
    } else if (toElement != null) {
      return function.apply(set().headSet(toElement, toInclusive));
    } else {
      return function.apply(set());
    }
  }

  protected class DescendingIteratorContext extends AbstractIteratorContext {
    public DescendingIteratorContext(long sessionId) {
      super(sessionId);
    }

    @Override
    protected Iterator<String> create() {
      return collection().descendingIterator();
    }
  }

  protected class SubSetIteratorContext extends AbstractIteratorContext {
    private final String fromElement;
    private final boolean fromInclusive;
    private final String toElement;
    private final boolean toInclusive;

    SubSetIteratorContext(long sessionId, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
      super(sessionId);
      this.fromElement = fromElement;
      this.fromInclusive = fromInclusive;
      this.toElement = toElement;
      this.toInclusive = toInclusive;
    }

    @Override
    protected Iterator<String> create() {
      return subSetApply(set -> set.iterator(), fromElement, fromInclusive, toElement, toInclusive);
    }
  }

  protected class DescendingSubSetIteratorContext extends AbstractIteratorContext {
    private final String fromElement;
    private final boolean fromInclusive;
    private final String toElement;
    private final boolean toInclusive;

    DescendingSubSetIteratorContext(long sessionId, String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
      super(sessionId);
      this.fromElement = fromElement;
      this.fromInclusive = fromInclusive;
      this.toElement = toElement;
      this.toInclusive = toInclusive;
    }

    @Override
    protected Iterator<String> create() {
      return subSetApply(set -> set.descendingIterator(), fromElement, fromInclusive, toElement, toInclusive);
    }
  }
}
