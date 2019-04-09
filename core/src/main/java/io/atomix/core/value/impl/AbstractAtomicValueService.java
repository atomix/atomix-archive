/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.value.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.atomix.core.value.AtomicValueType;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

/**
 * Abstract atomic value service.
 */
public abstract class AbstractAtomicValueService extends AbstractPrimitiveService<AtomicValueClient> implements AtomicValueService {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(AtomicValueType.instance().namespace())
      .register(SessionId.class)
      .build());

  private Versioned<byte[]> value = new Versioned<>(null, 0);
  private Set<SessionId> listeners = Sets.newHashSet();

  protected AbstractAtomicValueService(PrimitiveType primitiveType) {
    super(primitiveType, AtomicValueClient.class);
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    AtomicValueSnapshot.Builder builder = AtomicValueSnapshot.newBuilder();
    if (value.value() != null) {
      builder.setValue(ByteString.copyFrom(value.value()));
      builder.setVersion(value.version());
    }
    builder.addAllListeners(listeners.stream().map(SessionId::id).collect(Collectors.toList()));
  }

  @Override
  public void restore(InputStream input) throws IOException {
    AtomicValueSnapshot snapshot = AtomicValueSnapshot.parseFrom(input);
    if (snapshot.getVersion() > 0) {
      value = new Versioned<>(snapshot.getValue().toByteArray(), snapshot.getVersion());
    } else {
      value = new Versioned<>(null, 0);
    }
    listeners = snapshot.getListenersList().stream()
        .map(SessionId::from)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private Versioned<byte[]> updateAndNotify(byte[] value) {
    Versioned<byte[]> oldValue = this.value;
    this.value = new Versioned<>(value, getCurrentIndex());
    listeners.forEach(session -> getSession(session).accept(client -> client.change(this.value, oldValue)));
    return oldValue;
  }

  @Override
  public Versioned<byte[]> set(byte[] value) {
    if (!Arrays.equals(this.value.value(), value)) {
      updateAndNotify(value);
    }
    return this.value;
  }

  @Override
  public Versioned<byte[]> get() {
    return value;
  }

  @Override
  public Versioned<byte[]> compareAndSet(byte[] expect, byte[] update) {
    if (Arrays.equals(value.value(), expect)) {
      updateAndNotify(update);
      return value;
    }
    return null;
  }

  @Override
  public Versioned<byte[]> compareAndSet(long version, byte[] value) {
    if (this.value.version() == version) {
      if (!Arrays.equals(this.value.value(), value)) {
        updateAndNotify(value);
      }
      return this.value;
    }
    return null;
  }

  @Override
  public Versioned<byte[]> getAndSet(byte[] value) {
    if (!Arrays.equals(this.value.value(), value)) {
      return updateAndNotify(value);
    }
    return this.value;
  }

  @Override
  public void addListener() {
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void removeListener() {
    listeners.remove(getCurrentSession().sessionId());
  }

  @Override
  protected void onExpire(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  protected void onClose(Session session) {
    listeners.remove(session.sessionId());
  }
}
