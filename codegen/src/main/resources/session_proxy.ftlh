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
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.impl.SessionResponseContext;
<#if hasCommands>
import io.atomix.primitive.session.impl.SessionCommandContext;
</#if>
<#if hasQueries>
import io.atomix.primitive.session.impl.SessionQueryContext;
</#if>
<#if hasStreams>
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.utils.stream.StreamHandler;
</#if>
import io.atomix.utils.concurrent.ThreadContext;

import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
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

/**
 * Atomix generated proxy ${proxyClass.className}.
 */
public class ${proxyClass.className} extends SessionEnabledPrimitiveProxy {
    public ${proxyClass.className}(Context context) {
        super(context);
    }

    <#list operations as operation>
    <#if operation.stream>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @param handler the response handler
     * @return the response future
     */
    public CompletableFuture<SessionResponseContext> ${operation.methodName}(
            <#if operation.type == "QUERY">
            SessionQueryContext context,
            <#else>
            SessionCommandContext context,
            </#if>
            ${operation.requestClass.className} request,
            StreamHandler<Pair<SessionStreamContext, ${operation.responseClass.className}>> handler) {
        return getClient().execute(
            ${operation.operationsClass.className}.${operation.constantName},
            ${operation.operationsClass.className}.${operation.constantName}_STREAM,
            context,
            request,
            handler,
            ${operation.requestClass.className}::toByteString,
            ${operation.responseClass.className}::parseFrom);
    }
    <#elseif operation.async>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public CompletableFuture<Pair<SessionResponseContext, ${operation.responseClass.className}>> ${operation.methodName}(
            <#if operation.type == "QUERY">
            SessionQueryContext context,
            <#else>
            SessionCommandContext context,
            </#if>
            ${operation.requestClass.className} request) {
        return getClient().execute(
            ${operation.operationsClass.className}.${operation.constantName},
            ${operation.operationsClass.className}.${operation.constantName}_STREAM,
            context,
            request,
            ${operation.requestClass.className}::toByteString,
            ${operation.responseClass.className}::parseFrom);
    }
    <#else>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public CompletableFuture<Pair<SessionResponseContext, ${operation.responseClass.className}>> ${operation.methodName}(
            <#if operation.type == "QUERY">
            SessionQueryContext context,
            <#else>
            SessionCommandContext context,
            </#if>
            ${operation.requestClass.className} request) {
        return getClient().execute(
            ${operation.operationsClass.className}.${operation.constantName},
            context,
            request,
            ${operation.requestClass.className}::toByteString,
            ${operation.responseClass.className}::parseFrom);
    }
    </#if>

    </#list>
}