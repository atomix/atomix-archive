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
package io.atomix.core.election.impl;

import java.util.concurrent.CompletableFuture;

import com.google.common.io.BaseEncoding;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.LeaderElectionConfig;
import io.atomix.primitive.ManagedAsyncPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.impl.DefaultSessionClient;
import io.atomix.utils.serializer.Serializer;

/**
 * Default implementation of {@code LeaderElectorBuilder}.
 */
public class DefaultLeaderElectionBuilder<T> extends LeaderElectionBuilder<T> {
  public DefaultLeaderElectionBuilder(String name, LeaderElectionConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<LeaderElection<T>> buildAsync() {
    ServiceProtocol protocol = (ServiceProtocol) protocol();
    ServiceId serviceId = ServiceId.newBuilder()
        .setName(name)
        .setType(LeaderElectionService.TYPE.name())
        .build();
    return protocol.createService(name, managementService.getPartitionService())
        .thenApply(client -> new LeaderElectionProxy(new DefaultSessionClient(serviceId, client.getPartition(name))))
        .thenApply(proxy -> new DefaultAsyncLeaderElection(proxy, config.getSessionTimeout(), managementService))
        .thenCompose(ManagedAsyncPrimitive::connect)
        .thenApply(election -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncLeaderElection<T, String>(
              election,
              id -> BaseEncoding.base16().encode(serializer.encode(id)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));
        })
        .thenApply(AsyncLeaderElection::sync);
  }
}