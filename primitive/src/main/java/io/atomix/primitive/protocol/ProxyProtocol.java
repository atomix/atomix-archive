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
package io.atomix.primitive.protocol;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.session.SessionClient;

/**
 * State machine replication-based primitive protocol.
 */
public interface ProxyProtocol extends PrimitiveProtocol {

  /**
   * Returns the protocol partition group name.
   *
   * @return the protocol partition group name
   */
  String group();

  /**
   * Returns a new client for the given partition.
   *
   * @param name              the primitive name
   * @param type              the primitive type
   * @param partition         the partition for which to return the session
   * @param managementService the partition management service
   * @return a new client for the given partition
   */
  SessionClient newClient(String name, PrimitiveType type, Partition partition, PrimitiveManagementService managementService);

}
