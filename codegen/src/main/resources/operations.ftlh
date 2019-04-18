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
package ${operationsClass.javaPackage};

/**
 * Atomix generated operations ${operationsClass.className}.
 */
public enum ${operationsClass.className} implements io.atomix.primitive.operation.OperationId {

    <#list operations as operation>
    <#if operation?is_last>
    ${operation.enumValue}("${operation.name}", io.atomix.primitive.operation.OperationType.${operation.type});
    <#else>
    ${operation.enumValue}("${operation.name}", io.atomix.primitive.operation.OperationType.${operation.type}),
    </#if>

    </#list>
    private final String id;
    private final io.atomix.primitive.operation.OperationType type;

    ${operationsClass.className}(String id, io.atomix.primitive.operation.OperationType type) {
        this.id = id;
        this.type = type;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public io.atomix.primitive.operation.OperationType type() {
        return type;
    }
}