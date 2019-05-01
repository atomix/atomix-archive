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

import io.atomix.primitive.service.ServiceOperationRegistry;
import io.atomix.primitive.service.SimplePrimitiveService;
<#if hasStreams>
import io.atomix.utils.stream.StreamHandler;
</#if>

<#list operations as operation>
<#if operation.operationsClass.packageName != proxyClass.packageName>
import ${operation.operationsClass.packageName}.${operation.operationsClass.className};
</#if>
<#if operation.requestClass.packageName != proxyClass.packageName>
import ${operation.requestClass.packageName}.${operation.requestClass.className};
</#if>
<#if operation.responseClass.packageName != proxyClass.packageName>
import ${operation.responseClass.packageName}.${operation.responseClass.className};
</#if>

</#list>
<#if hasAsyncs>
import java.util.concurrent.CompletableFuture;

</#if>
/**
 * Atomix generated service interface ${serviceClass.className}.
 */
public abstract class ${serviceClass.className} extends SimplePrimitiveService {

    @Override
    protected void configure(ServiceOperationRegistry registry) {
        <#list operations as operation>
        <#if operation.async>
        registry.register(
            ${operation.operationsClass.className}.${operation.constantName},
            ${operation.operationsClass.className}.${operation.constantName}_STREAM,
            this::${operation.methodName},
            ${operation.requestClass.className}::parseFrom,
            ${operation.responseClass.className}::toByteArray);
        <#elseif operation.stream>
        registry.register(
            ${operation.operationsClass.className}.${operation.constantName},
            ${operation.operationsClass.className}.${operation.constantName}_STREAM,
            this::${operation.methodName},
            ${operation.requestClass.className}::parseFrom,
            ${operation.responseClass.className}::toByteArray);
        <#else>
        registry.register(
            ${operation.operationsClass.className}.${operation.constantName},
            this::${operation.methodName},
            ${operation.requestClass.className}::parseFrom,
            ${operation.responseClass.className}::toByteArray);
        </#if>
        </#list>
    }

    <#list operations as operation>
    <#if operation.async>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public abstract CompletableFuture<${operation.responseClass.className}> ${operation.methodName}(${operation.requestClass.className} request);
    <#elseif operation.stream>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @param handler the response stream
     */
    public abstract void ${operation.methodName}(${operation.requestClass.className} request, StreamHandler<${operation.responseClass.className}> handler);
    <#else>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public abstract ${operation.responseClass.className} ${operation.methodName}(${operation.requestClass.className} request);
    </#if>

    </#list>
}