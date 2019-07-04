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

import java.util.concurrent.CompletableFuture;

import io.atomix.api.controller.PrimaryTerm;
import io.atomix.utils.event.AsyncListenable;

/**
 * Primary election service.
 */
public interface PrimaryElectionService extends AsyncListenable<PrimaryElectionEvent> {

    /**
     * Enters the primary election.
     * <p>
     * When entering a primary election, the provided member will be added to the election's candidate list.
     * The returned term is representative of the term <em>after</em> the member joins the election. Thus, if the
     * joining member is immediately elected primary, the returned term should reflect that.
     *
     * @return the current term
     */
    CompletableFuture<PrimaryTerm> enter();

    /**
     * Returns the current term.
     * <p>
     * The term is representative of the current primary, candidates, and backups in the primary election.
     *
     * @return the current term
     */
    CompletableFuture<PrimaryTerm> getTerm();

}