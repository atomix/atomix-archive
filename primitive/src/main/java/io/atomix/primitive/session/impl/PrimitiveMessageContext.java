/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.session.impl;

import io.atomix.primitive.partition.PartitionId;

/**
 * Primitive message context.
 */
public class PrimitiveMessageContext {
  private final String prefix;

  public PrimitiveMessageContext(PartitionId partitionId) {
    this.prefix = String.format("%s-%d", partitionId.getGroup(), partitionId.getPartition());
  }

  private static String getSubject(String prefix, String type) {
    if (prefix == null) {
      return type;
    } else {
      return String.format("%s-%s", prefix, type);
    }
  }

  String eventSubject(long sessionId) {
    return String.format("%s-event-%d", prefix, sessionId);
  }

  String responseSubject(long sessionId) {
    return String.format("%s-stream-%d", prefix, sessionId);
  }
}
