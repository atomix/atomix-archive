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
package io.atomix.node.management.impl;

import io.atomix.api.controller.PartitionId;
import io.atomix.node.management.ConfigService;
import io.atomix.node.management.PartitionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;

/**
 * Partition service implementation.
 */
@Component
public class PartitionServiceImpl implements PartitionService {
    @Dependency
    private ConfigService configService;

    @Override
    public PartitionId getPartitionId() {
        return configService.getPartition();
    }
}
