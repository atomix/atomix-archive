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
package ${proxyClass.javaPackage};

/**
 * Atomix generated proxy ${proxyClass.className}.
 */
public class ${proxyClass.className} extends io.atomix.primitive.proxy.SimplePrimitiveProxy {
    public ${proxyClass.className}(
            io.atomix.primitive.service.impl.ServiceId serviceId,
            io.atomix.primitive.partition.PartitionId partitionId,
            io.atomix.primitive.PrimitiveManagementService managementService) {
        super(serviceId, partitionId, managementService);
    }

    <#list operations as operation>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public java.util.concurrent.CompletableFuture<${operation.responseClass.qualifiedName}> ${operation.methodName}(${operation.requestClass.qualifiedName} request) {
        return getClient().execute(
            ${operation.operationsClass.qualifiedName}.${operation.enumValue},
            request,
            ${operation.requestClass.qualifiedName}::toByteString,
            ${operation.responseClass.qualifiedName}::parseFrom);
    }

    </#list>
}