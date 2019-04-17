/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.raft.storage.system;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.atomix.raft.storage.RaftStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Manages persistence of server configurations.
 * <p>
 * The server metastore is responsible for persisting server configurations according to the configured
 * {@link RaftStorage#storageLevel() storage level}. Each server persists their current {@link #loadTerm() term}
 * and last {@link #loadVote() vote} as is dictated by the Raft consensus algorithm. Additionally, the
 * metastore is responsible for storing the last know server {@link RaftConfiguration}, including cluster
 * membership.
 */
public class MetaStore implements AutoCloseable {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final File metaFile;
  private final File configFile;

  public MetaStore(RaftStorage storage) {
    if (!(storage.directory().isDirectory() || storage.directory().mkdirs())) {
      throw new IllegalArgumentException(String.format("Can't create storage directory [%s].", storage.directory()));
    }
    metaFile = new File(storage.directory(), String.format("%s.meta", storage.prefix()));
    configFile = new File(storage.directory(), String.format("%s.conf", storage.prefix()));
  }

  /**
   * Stores the current server term.
   *
   * @param term The current server term.
   */
  public synchronized void storeTerm(long term) throws IOException {
    log.trace("Store term {}", term);
    RaftMetadata metadata;
    try (InputStream input = new FileInputStream(metaFile)) {
      metadata = RaftMetadata.parseFrom(input);
    } catch (IOException e) {
      metadata = RaftMetadata.newBuilder().build();
    }

    metadata = RaftMetadata.newBuilder(metadata)
        .setTerm(term)
        .build();

    try (OutputStream output = new FileOutputStream(metaFile)) {
      metadata.writeTo(output);
    }
  }

  /**
   * Loads the stored server term.
   *
   * @return The stored server term.
   */
  public synchronized long loadTerm() {
    try (InputStream input = new FileInputStream(metaFile)) {
      return RaftMetadata.parseFrom(input).getTerm();
    } catch (IOException e) {
      return 0;
    }
  }

  /**
   * Stores the last voted server.
   *
   * @param vote The server vote.
   */
  public synchronized void storeVote(String vote) throws IOException {
    log.trace("Store vote {}", vote);
    RaftMetadata metadata;
    try (InputStream input = new FileInputStream(metaFile)) {
      metadata = RaftMetadata.parseFrom(input);
    } catch (IOException e) {
      metadata = RaftMetadata.newBuilder().build();
    }

    metadata = RaftMetadata.newBuilder(metadata)
        .setVote(vote != null ? vote : "")
        .build();

    try (OutputStream output = new FileOutputStream(metaFile)) {
      metadata.writeTo(output);
    }
  }

  /**
   * Loads the last vote for the server.
   *
   * @return The last vote for the server.
   */
  public synchronized String loadVote() {
    try (InputStream input = new FileInputStream(metaFile)) {
      String vote = RaftMetadata.parseFrom(input).getVote();
      return vote != null && !vote.equals("") ? vote : null;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Stores the current cluster configuration.
   *
   * @param configuration The current cluster configuration.
   */
  public synchronized void storeConfiguration(RaftConfiguration configuration) throws IOException {
    log.trace("Store configuration {}", configuration);
    try (OutputStream output = new FileOutputStream(configFile)) {
      configuration.writeTo(output);
    }
  }

  /**
   * Loads the current cluster configuration.
   *
   * @return The current cluster configuration.
   */
  public synchronized RaftConfiguration loadConfiguration() {
    try (InputStream input = new FileInputStream(configFile)) {
      return RaftConfiguration.parseFrom(input);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }

}
