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
import io.atomix.primitive.service.impl.PrimitiveServiceProto;

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
    PrimitiveServiceProto.registerAllExtensions(registryLite);

    PrimitiveServiceGenerator compiler = new PrimitiveServiceGenerator();
    PluginProtos.CodeGeneratorRequest request = PluginProtos.CodeGeneratorRequest.parseFrom(is, registryLite);
    PluginProtos.CodeGeneratorResponse response = compiler.generate(request);

    response.writeTo(System.out);
  }
}