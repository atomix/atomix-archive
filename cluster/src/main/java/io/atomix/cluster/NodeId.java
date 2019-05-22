/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.cluster;

import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ComparisonChain;
import io.atomix.utils.Identifier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Node identifier.
 */
public class NodeId implements Identifier<String>, Comparable<NodeId> {

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @return node id
   */
  public static NodeId anonymous() {
    return new NodeId(UUID.randomUUID().toString(), DEFAULT_NAMESPACE);
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id string identifier
   * @return node id
   */
  public static NodeId from(String id) {
    return new NodeId(id, DEFAULT_NAMESPACE);
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id        string identifier
   * @param namespace the namespace
   * @return node id
   */
  public static NodeId from(String id, String namespace) {
    return new NodeId(id, namespace);
  }

  protected static final String DEFAULT_NAMESPACE = "default";

  private final String id;
  private final String namespace;

  /**
   * Constructor for serialization.
   */
  private NodeId() {
    this("", "");
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id        string identifier
   * @param namespace the node namespace
   */
  public NodeId(String id, String namespace) {
    this.id = checkNotNull(id);
    this.namespace = namespace != null ? namespace : DEFAULT_NAMESPACE;
  }

  @Override
  public String id() {
    return id;
  }

  /**
   * Returns the node namespace.
   *
   * @return the node namespace
   */
  public String namespace() {
    return namespace;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id(), namespace());
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NodeId) {
      NodeId that = (NodeId) object;
      return this.id().equals(that.id()) && this.namespace().equals(that.namespace());
    }
    return false;
  }

  @Override
  public int compareTo(NodeId that) {
    return ComparisonChain.start()
        .compare(namespace(), that.namespace())
        .compare(id(), that.id())
        .result();
  }

  @Override
  public String toString() {
    return namespace() + "." + id();
  }
}
