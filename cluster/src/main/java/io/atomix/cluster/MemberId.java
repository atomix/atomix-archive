/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.cluster;

import java.util.UUID;

import com.google.common.base.Strings;

/**
 * Controller cluster identity.
 */
public class MemberId extends NodeId {

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @return node id
   */
  public static MemberId anonymous() {
    return new MemberId(UUID.randomUUID().toString(), DEFAULT_NAMESPACE);
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id string identifier
   * @return node id
   */
  public static MemberId from(String id) {
    int index = id.indexOf('.');
    return index != -1
        ? new MemberId(id.substring(index + 1), id.substring(0, index))
        : new MemberId(id, DEFAULT_NAMESPACE);
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id        string identifier
   * @param namespace member namespace
   * @return node id
   */
  public static MemberId from(String id, String namespace) {
    return new MemberId(id, !Strings.isNullOrEmpty(namespace) ? namespace : DEFAULT_NAMESPACE);
  }

  /**
   * Returns the member ID for the given member.
   *
   * @param member the member for which to return the ID
   * @return the member ID for the given member
   */
  public static MemberId from(Member member) {
    return new MemberId(member.getId(), member.getNamespace());
  }

  public MemberId(String id, String namespace) {
    super(id, namespace);
  }
}
