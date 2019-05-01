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
package ${operationsClass.packageName};

import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.QueryId;
<#if hasAsyncs || hasStreams>
import io.atomix.primitive.operation.StreamType;
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
/**
 * Atomix generated operations ${operationsClass.className}.
 */
public final class ${operationsClass.className} {

    <#list operations as operation>
    <#if operation.type == "COMMAND">
    public static final CommandId<${operation.requestClass.className}, ${operation.responseClass.className}> ${operation.constantName} = new CommandId<>("${operation.name}");
    </#if>
    </#list>
    <#list operations as operation>
    <#if operation.type == "QUERY">
    public static final QueryId<${operation.requestClass.className}, ${operation.responseClass.className}> ${operation.constantName} = new QueryId<>("${operation.name}");
    </#if>
    </#list>
    <#list operations as operation>
    <#if operation.async || operation.stream>
    public static final StreamType<${operation.responseClass.className}> ${operation.constantName}_STREAM = new StreamType<>("${operation.name}");
    </#if>
    </#list>

    private ${operationsClass.className}() {
    }
}