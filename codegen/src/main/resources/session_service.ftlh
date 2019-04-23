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
package ${serviceClass.packageName};

import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.service.SessionManagedPrimitiveService;

<#list operations as operation>
<#if operation.operationsClass.packageName != proxyClass.packageName>
import ${operation.operationsClass.packageName}.${operation.operationsClass.className}
</#if>
<#if operation.requestClass.packageName != proxyClass.packageName>
import ${operation.requestClass.packageName}.${operation.requestClass.className}
</#if>
<#if operation.responseClass.packageName != proxyClass.packageName>
import ${operation.responseClass.packageName}.${operation.responseClass.className}
</#if>
</#list>
<#list events as event>
<#if event.eventsClass.packageName != proxyClass.packageName>
import ${event.eventsClass.packageName}.${operation.eventsClass.className}
</#if>
<#if event.valueClass.packageName != proxyClass.packageName>
import ${event.valueClass.packageName}.${operation.valueClass.className}
</#if>
</#list>
/**
 * Atomix generated service interface ${serviceClass.className}.
 */
public abstract class ${serviceClass.className} extends SessionManagedPrimitiveService {
    protected ${serviceClass.className}(PartitionId partitionId, PartitionManagementService managementService) {
        super(partitionId, managementService);
    }

    @Override
    protected void configure(ServiceExecutor executor) {
        <#list operations as operation>
        executor.register(
            ${operation.operationsClass.className}.${operation.constantName},
            this::${operation.methodName},
            ${operation.requestClass.className}::parseFrom,
            ${operation.responseClass.className}::toByteArray);
         </#list>
    }

    <#list operations as operation>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public abstract ${operation.responseClass.className} ${operation.methodName}(${operation.requestClass.className} request);

    </#list>

    <#list events as event>
    /**
     * Atomix generated event producer method.
     *
     * @param event the event to produce
     */
    protected void ${event.methodName}(SessionId sessionId, ${event.valueClass.className} event) {
        getSession(sessionId).publish(${event.eventsClass.className}.${event.constantName}, event, ${event.valueClass.className}::toByteArray);
    }

    </#list>
}