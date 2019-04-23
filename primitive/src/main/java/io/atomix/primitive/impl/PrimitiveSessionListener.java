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
package io.atomix.primitive.impl;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.session.impl.EventContext;
import io.atomix.primitive.session.impl.SessionEventContext;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client session message listener.
 */
final class PrimitiveSessionListener<P extends PrimitiveProxy> {
  private final P proxy;
  private final PrimitiveSessionState state;
  private final PrimitiveSessionSequencer sequencer;

  PrimitiveSessionListener(
      P proxy,
      PrimitiveSessionState state,
      PrimitiveSessionSequencer sequencer) {
    this.proxy = checkNotNull(proxy, "proxy cannot be null");
    this.state = checkNotNull(state, "state cannot be null");
    this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
  }

  public void event(BiConsumer<P, SessionEventContext> consumer) {
    consumer.accept(proxy, SessionEventContext.newBuilder()
        .setSessionId(state.getSessionId().id())
        .build());
  }

  public <T> BiConsumer<EventContext, T> listener(Consumer<T> consumer) {
    return (context, event) -> sequencer.sequenceEvent(context, () -> consumer.accept(event));
  }
}
