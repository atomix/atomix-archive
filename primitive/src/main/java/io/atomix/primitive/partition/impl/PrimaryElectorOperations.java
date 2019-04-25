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
package io.atomix.primitive.partition.impl;

import com.google.protobuf.Empty;
import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.QueryId;
import io.atomix.primitive.partition.PrimaryElectionEvent;

/**
 * Primary elector operations.
 */
public final class PrimaryElectorOperations {
  public static final CommandId<EnterRequest, EnterResponse> ENTER = new CommandId<>("enter");
  public static final QueryId<GetTermRequest, GetTermResponse> GET_TERM = new QueryId<>("getTerm");
  public static final CommandId<Empty, PrimaryElectionEvent> STREAM_EVENTS = new CommandId<>("stream");

  private PrimaryElectorOperations() {
  }
}