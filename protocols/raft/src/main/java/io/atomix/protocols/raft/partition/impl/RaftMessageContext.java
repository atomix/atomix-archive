/*
 * Copyright 2017-present Open Networking Foundation
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
 * limitations under the License.
 */
package io.atomix.protocols.raft.partition.impl;

/**
 * Protocol message context.
 */
class RaftMessageContext {
  final String querySubject;
  final String queryStreamSubject;
  final String commandSubject;
  final String commandStreamSubject;
  final String joinSubject;
  final String leaveSubject;
  final String configureSubject;
  final String reconfigureSubject;
  final String installSubject;
  final String transferSubject;
  final String pollSubject;
  final String voteSubject;
  final String appendSubject;

  RaftMessageContext(String prefix) {
    this.querySubject = getSubject(prefix, "query");
    this.queryStreamSubject = getSubject(prefix, "query-stream");
    this.commandSubject = getSubject(prefix, "command");
    this.commandStreamSubject = getSubject(prefix, "command-stream");
    this.joinSubject = getSubject(prefix, "join");
    this.leaveSubject = getSubject(prefix, "leave");
    this.configureSubject = getSubject(prefix, "configure");
    this.reconfigureSubject = getSubject(prefix, "reconfigure");
    this.installSubject = getSubject(prefix, "install");
    this.transferSubject = getSubject(prefix, "transfer");
    this.pollSubject = getSubject(prefix, "poll");
    this.voteSubject = getSubject(prefix, "vote");
    this.appendSubject = getSubject(prefix, "append");
  }

  private static String getSubject(String prefix, String type) {
    if (prefix == null) {
      return type;
    } else {
      return String.format("%s-%s", prefix, type);
    }
  }
}
