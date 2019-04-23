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
package ${proxyClass.packageName};

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.SimplePrimitiveProxy;
import io.atomix.utils.concurrent.ThreadContext;

import java.util.concurrent.CompletableFuture;

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
/**
 * Atomix generated proxy ${proxyClass.className}.
 */
public class ${proxyClass.className} extends SimplePrimitiveProxy {
    public ${proxyClass.className}(Context context) {
        super(context);
    }

    <#list operations as operation>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public CompletableFuture<${operation.responseClass.className}> ${operation.methodName}(${operation.requestClass.className} request) {
        return getClient().execute(
            ${operation.operationsClass.className}.${operation.constantName},
            request,
            ${operation.requestClass.className}::toByteString,
            ${operation.responseClass.className}::parseFrom);
    }

    </#list>
}