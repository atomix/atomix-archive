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

import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.service.ServiceClient;
import io.atomix.primitive.service.impl.DefaultServiceClient;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.proto.NextRequest;
import io.atomix.primitive.session.impl.proto.NextResponse;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Replicated ID generator service.
 */
@Component
public class PrimitiveSessionIdManager implements SessionIdService, Managed {
  private static final String PRIMITIVE_NAME = "session-id";

  @Dependency
  private SystemPartitionService partitionService;
  private ServiceClient client;

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
  public CompletableFuture<Void> start() {
    this.client = new DefaultServiceClient(
        ServiceId.newBuilder()
            .setType(SessionIdGeneratorType.instance().name())
            .setName(PRIMITIVE_NAME)
            .build(),
        partitionService.getSystemPartitionGroup().getPartitions().iterator().next().getClient());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }
}
