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
package io.atomix.primitive.session.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.atomix.primitive.service.ServiceClient;
import io.atomix.primitive.service.impl.DefaultServiceClient;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.proto.NextRequest;
import io.atomix.primitive.session.impl.proto.NextResponse;

/**
 * Replicated ID generator service.
 */
public class ReplicatedSessionIdService implements ManagedSessionIdService {
  private static final String PRIMITIVE_NAME = "session-id";

  private final PartitionService partitionService;
  private ServiceClient client;
  private final AtomicBoolean started = new AtomicBoolean();

  public ReplicatedSessionIdService(PartitionService partitionService) {
    this.partitionService = partitionService;
  }

  @Override
  public CompletableFuture<SessionId> nextSessionId() {
    return client.execute(
        SessionIdGeneratorOperations.NEXT,
        RequestContext.newBuilder().build(),
        NextRequest.newBuilder().build(),
        NextRequest::toByteString,
        NextResponse::parseFrom)
        .thenApply(response -> SessionId.from(response.getRight().getSessionId()));
  }

  @Override
  public CompletableFuture<SessionIdService> start() {
    this.client = new DefaultServiceClient(
        ServiceId.newBuilder()
            .setType(SessionIdGeneratorType.instance().name())
            .setName(PRIMITIVE_NAME)
            .build(),
        partitionService.getSystemPartitionGroup().getPartitions().iterator().next().getClient());
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null)
        .thenRun(() -> started.set(false));
  }
}
