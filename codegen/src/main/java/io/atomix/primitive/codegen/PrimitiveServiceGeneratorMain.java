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
package io.atomix.primitive.codegen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.ByteStreams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.compiler.PluginProtos;
import io.atomix.service.ServiceDescriptorProto;

/**
 * Primitive service generator main.
 */
public class PrimitiveServiceGeneratorMain {
  public static void main(String[] args) throws IOException, Descriptors.DescriptorValidationException {
    InputStream is = System.in;

    if (args.length > 0) {
      File replayFile = new File(args[0]);
      FileOutputStream fos = new FileOutputStream(replayFile);

      ByteStreams.copy(System.in, fos);
      fos.close();

      is = new FileInputStream(replayFile);
    }

    ExtensionRegistryLite registryLite = ExtensionRegistryLite.newInstance();
    ServiceDescriptorProto.registerAllExtensions(registryLite);

    PrimitiveServiceGenerator compiler = new PrimitiveServiceGenerator();
    PluginProtos.CodeGeneratorRequest request = PluginProtos.CodeGeneratorRequest.parseFrom(is, registryLite);
    PluginProtos.CodeGeneratorResponse response = compiler.generate(request);

    response.writeTo(System.out);
  }
}