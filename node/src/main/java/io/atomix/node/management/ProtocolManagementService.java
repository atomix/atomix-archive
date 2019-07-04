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
package io.atomix.node.management;

import io.atomix.node.protocol.Protocol;
import io.atomix.node.service.ServiceTypeRegistry;
import io.atomix.utils.concurrent.ThreadService;

/**
 * Protocol management service.
 */
public interface ProtocolManagementService {

    /**
     * Returns the protocol type.
     *
     * @return the protocol type
     */
    Protocol.Type getProtocolType();

    /**
     * Returns the cluster service.
     *
     * @return the cluster service
     */
    ClusterService getCluster();

    /**
     * Returns the partition service.
     *
     * @return the partition service
     */
    PartitionService getPartitionService();

    /**
     * Returns the service registry.
     *
     * @return the service registry
     */
    ServiceRegistry getServiceRegistry();

    /**
     * Returns the service provider.
     *
     * @return the service provider
     */
    ServiceProvider getServiceProvider();

    /**
     * Returns the thread service.
     *
     * @return the thread service
     */
    ThreadService getThreadService();

    /**
     * Returns the service type registry.
     *
     * @return the service type registry
     */
    ServiceTypeRegistry getServiceTypeRegistry();

    /**
     * Returns the primary election service.
     *
     * @return the primary election service
     */
    PrimaryElectionService getPrimaryElectionService();

}
