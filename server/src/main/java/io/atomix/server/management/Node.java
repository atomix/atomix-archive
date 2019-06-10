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
package io.atomix.server.management;

/**
 * Node info.
 */
public class Node {
  private final String id;
  private final String host;
  private final int port;

  public Node(String id, String host, int port) {
    this.id = id;
    this.host = host;
    this.port = port;
  }

  /**
   * Returns the node ID.
   *
   * @return the node ID
   */
  public String id() {
    return id;
  }

  /**
   * Returns the node host.
   *
   * @return the node host
   */
  public String host() {
    return host;
  }

  /**
   * Returns the node port.
   *
   * @return the node port
   */
  public int port() {
    return port;
  }
}
