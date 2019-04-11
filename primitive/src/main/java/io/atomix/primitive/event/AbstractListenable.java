/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.primitive.event;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Basis for components which need to export listener mechanism.
 */
public abstract class AbstractListenable<E> implements Listenable<E> {
  private final Set<Consumer<E>> listeners = new CopyOnWriteArraySet<>();

  @Override
  public void addListener(Consumer<E> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(Consumer<E> listener) {
    listeners.remove(listener);
  }

  /**
   * Posts the specified event to the local event dispatcher.
   *
   * @param event event to be posted; may be null
   */
  protected void post(E event) {
    listeners.forEach(listener -> listener.accept(event));
  }
}
