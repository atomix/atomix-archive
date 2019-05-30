/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.atomix.api.headers.Name;
import io.atomix.client.partition.Partitioner;
import io.atomix.client.utils.serializer.Serializer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract builder for distributed primitives.
 *
 * @param <B> builder type
 * @param <P> primitive type
 */
public abstract class PrimitiveBuilder<B extends PrimitiveBuilder<B, P>, P extends SyncPrimitive> {
  protected final Name name;
  protected String group;
  protected Partitioner<String> partitioner = Partitioner.MURMUR3;
  protected boolean readOnly;
  protected Serializer serializer;
  protected final PrimitiveManagementService managementService;

  protected PrimitiveBuilder(Name name, PrimitiveManagementService managementService) {
    this.name = checkNotNull(name, "name cannot be null");
    this.managementService = checkNotNull(managementService, "managementService cannot be null");
  }

  /**
   * Returns the namespaced primitive name.
   *
   * @return the namespaced primitive name
   */
  protected Name getName() {
    return name;
  }

  /**
   * Sets the primitive partition group.
   *
   * @param group the primitive partition group
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withGroup(String group) {
    this.group = checkNotNull(group);
    return (B) this;
  }

  /**
   * Sets the primitive partitioner.
   *
   * @param partitioner the primitive partitioner
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withPartitioner(Partitioner<String> partitioner) {
    this.partitioner = checkNotNull(partitioner);
    return (B) this;
  }

  /**
   * Sets the primitive serializer.
   *
   * @param serializer the primitive serializer
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withSerializer(Serializer serializer) {
    this.serializer = checkNotNull(serializer);
    return (B) this;
  }

  /**
   * Sets the primitive to read-only.
   *
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withReadOnly() {
    return withReadOnly(true);
  }

  /**
   * Sets whether the primitive is read-only.
   *
   * @param readOnly whether the primitive is read-only
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
    return (B) this;
  }

  /**
   * Returns the protocol serializer.
   *
   * @return the protocol serializer
   */
  protected Serializer serializer() {
    return serializer;
  }

  /**
   * Builds a new instance of the primitive.
   * <p>
   * The returned instance will be distinct from all other instances of the same primitive on this node, with a
   * distinct session, ordering guarantees, memory, etc.
   *
   * @return a new instance of the primitive
   */
  public P build() {
    try {
      return buildAsync().join();
    } catch (Exception e) {
      if (e instanceof CompletionException && e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  /**
   * Builds a new instance of the primitive asynchronously.
   * <p>
   * The returned instance will be distinct from all other instances of the same primitive on this node, with a
   * distinct session, ordering guarantees, memory, etc.
   *
   * @return asynchronous distributed primitive
   */
  public abstract CompletableFuture<P> buildAsync();

  /**
   * Gets or builds a singleton instance of the primitive.
   * <p>
   * The returned primitive will be shared by all {@code get()} calls for the named primitive. If no instance has yet
   * been constructed, the instance will be built from this builder's configuration.
   *
   * @return a singleton instance of the primitive
   */
  public P get() {
    try {
      return getAsync().join();
    } catch (Exception e) {
      if (e instanceof CompletionException && e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  /**
   * Gets or builds a singleton instance of the primitive asynchronously.
   * <p>
   * The returned primitive will be shared by all {@code get()} calls for the named primitive. If no instance has yet
   * been constructed, the instance will be built from this builder's configuration.
   *
   * @return a singleton instance of the primitive
   */
  public CompletableFuture<P> getAsync() {
    return managementService.getPrimitiveCache().getPrimitive(name, this::buildAsync);
  }
}
