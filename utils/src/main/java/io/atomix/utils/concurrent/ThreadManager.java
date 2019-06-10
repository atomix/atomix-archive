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
package io.atomix.utils.concurrent;

import java.util.concurrent.CompletableFuture;

import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread manager.
 */
@Component
public class ThreadManager implements ThreadService, Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadManager.class);

  private ThreadContextFactory threadContextFactory;

  @Override
  public ThreadContextFactory getFactory() {
    return threadContextFactory;
  }

  @Override
  public ThreadContext createContext() {
    return threadContextFactory.createContext();
  }

  @Override
  public CompletableFuture<Void> start() {
    threadContextFactory = new ThreadPoolContextFactory(
        "atomix-management-%d",
        Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 32), 8),
        LOGGER);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    threadContextFactory.close();
    return CompletableFuture.completedFuture(null);
  }
}
