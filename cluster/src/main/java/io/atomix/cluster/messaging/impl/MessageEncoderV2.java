/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

import io.atomix.utils.net.Address;
import io.netty.buffer.ByteBuf;

/**
 * V2 message encoder.
 */
class MessageEncoderV2 extends AbstractMessageEncoder {
  MessageEncoderV2(Address address) {
    super(address);
  }

  @Override
  protected void encodeAddress(ProtocolMessage message, ByteBuf buffer) {
    writeString(buffer, address.host());
    buffer.writeInt(address.port());
  }

  @Override
  protected void encodeMessage(ProtocolMessage message, ByteBuf buffer) {
    buffer.writeByte(message.type().id());
    writeLong(buffer, message.id());
  }

  @Override
  protected void encodeRequest(ProtocolRequest message, ByteBuf buffer) {
    writeString(buffer, message.subject());
    final byte[] payload = message.payload();
    writeInt(buffer, payload.length);
    buffer.writeBytes(payload);
  }

  @Override
  protected void encodeReply(ProtocolReply message, ByteBuf buffer) {
    buffer.writeByte(message.status().id());
    final byte[] payload = message.payload();
    writeInt(buffer, payload.length);
    buffer.writeBytes(payload);
  }

  @Override
  protected void encodeStreamRequest(ProtocolStreamRequest message, ByteBuf buffer) {
    writeString(buffer, message.subject());
  }

  @Override
  protected void encodeStreamReply(ProtocolStreamReply message, ByteBuf out) {
  }

  @Override
  protected void encodeStream(ProtocolStream message, ByteBuf buffer) {
    final byte[] payload = message.payload();
    writeInt(buffer, payload.length);
    buffer.writeBytes(payload);
  }

  @Override
  protected void encodeStreamEnd(ProtocolStreamEnd message, ByteBuf buffer) {
    buffer.writeByte(message.status().id());
  }
}
