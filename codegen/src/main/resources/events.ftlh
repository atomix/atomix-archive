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
package ${eventsClass.packageName};

import io.atomix.primitive.event.EventType;
<#list events as event>
<#if event.eventsClass.packageName != proxyClass.packageName>
import ${event.eventsClass.packageName}.${operation.eventsClass.className}
</#if>
<#if event.valueClass.packageName != proxyClass.packageName>
import ${event.valueClass.packageName}.${operation.valueClass.className}
</#if>
</#list>

/**
 * Atomix generated events ${eventsClass.className}.
 */
public final class ${eventsClass.className} {

    <#list events as event>
    public static final EventType<${event.valueClass.className}> ${event.constantName} = new EventType<>("${event.name}");
    </#list>

    private ${eventsClass.className}() {
    }
}