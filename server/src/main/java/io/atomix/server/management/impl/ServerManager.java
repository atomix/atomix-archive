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
package io.atomix.server.management.impl;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.atomix.server.ServerConfig;
import io.atomix.server.management.ProtocolManagementService;
import io.atomix.server.protocol.LogProtocol;
import io.atomix.server.protocol.Protocol;
import io.atomix.server.protocol.ServiceProtocol;
import io.atomix.server.service.counter.CounterServiceImpl;
import io.atomix.server.service.election.LeaderElectionServiceImpl;
import io.atomix.server.service.lock.LockServiceImpl;
import io.atomix.server.service.log.LogServiceImpl;
import io.atomix.server.service.map.MapServiceImpl;
import io.atomix.server.service.set.SetServiceImpl;
import io.atomix.server.service.value.ValueServiceImpl;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Futures;

/**
 * Default primitives service.
 */
@Component(ServerConfig.class)
public class ServerManager implements Managed<ServerConfig> {
  @Dependency
  private ProtocolManagementService managementService;

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start(ServerConfig config) {
    Protocol.Type protocolType = managementService.getProtocolTypeRegistry().getType(config.getProtocol().getType());
    Message protocolConfig;
    try {
      protocolConfig = config.getProtocol().getConfig().unpack(protocolType.getConfigClass());
    } catch (InvalidProtocolBufferException e) {
      return Futures.exceptionalFuture(e);
    }

    Protocol protocol = protocolType.newProtocol(protocolConfig, managementService);
    return protocol.start()
        .thenRun(() -> {
          if (protocol instanceof ServiceProtocol) {
            managementService.getServiceRegistry().register(new CounterServiceImpl((ServiceProtocol) protocol));
            managementService.getServiceRegistry().register(new LeaderElectionServiceImpl((ServiceProtocol) protocol));
            managementService.getServiceRegistry().register(new LockServiceImpl((ServiceProtocol) protocol));
            managementService.getServiceRegistry().register(new MapServiceImpl((ServiceProtocol) protocol));
            managementService.getServiceRegistry().register(new SetServiceImpl((ServiceProtocol) protocol));
            managementService.getServiceRegistry().register(new ValueServiceImpl((ServiceProtocol) protocol));
          }
          if (protocol instanceof LogProtocol) {
            managementService.getServiceRegistry().register(new LogServiceImpl((LogProtocol) protocol));
          }
        });
  }
}
