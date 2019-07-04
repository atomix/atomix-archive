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
package io.atomix.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.atomix.api.controller.PartitionConfig;
import io.atomix.node.protocol.Protocol;
import io.atomix.utils.component.Component;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Atomix server runner.
 */
public class AtomixNodeRunner {

  /**
   * Runs a standalone Atomix server from the given command line arguments.
   *
   * @param args the program arguments
   * @throws Exception if the supplied arguments are invalid
   */
  public static void main(String[] args) throws Exception {
    // Parse the command line arguments.
    final Namespace namespace = parseArgs(args);
    final Logger logger = createLogger();

    logger.info("Node ID: {}", namespace.getString("node"));
    logger.info("Partition Config: {}", namespace.<File>get("config"));
    logger.info("Protocol Config: {}", namespace.<File>get("protocol"));

    logger.info("Starting server");
    final AtomixNode atomix = buildServer(namespace);
    atomix.start().join();

    String readyFile = System.getProperty("atomix.readyFile", "/tmp/atomix-ready");
    if (!new File(readyFile).createNewFile()) {
      logger.warn("Failed to create ready file {}", readyFile);
    }

    synchronized (AtomixNode.class) {
      while (atomix.isRunning()) {
        AtomixNode.class.wait();
      }
    }
  }

  /**
   * Parses the command line arguments, returning an argparse4j namespace.
   *
   * @param args the arguments to parse
   * @return the namespace
   */
  static Namespace parseArgs(String[] args) {
    ArgumentParser parser = createParser();
    Namespace namespace = null;
    try {
      namespace = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }
    return namespace;
  }

  /**
   * Creates an agent argument parser.
   */
  private static ArgumentParser createParser() {
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Runs the Atomix server with the given arguments.");
    parser.addArgument("node")
        .metavar("ID")
        .type(String.class)
        .required(true)
        .help("The local node ID.");
    parser.addArgument("config")
        .metavar("FILE")
        .type(File.class)
        .required(true)
        .help("The node configuration.");
    parser.addArgument("protocol")
        .metavar("FILE")
        .type(File.class)
        .required(true)
        .help("The protocol configuration.");
    return parser;
  }

  /**
   * Configures and creates a new logger for the given namespace.
   *
   * @return a new agent logger
   */
  static Logger createLogger() {
    if (System.getProperty("atomix.log.directory") == null) {
      System.setProperty("atomix.log.directory", "log");
    }
    if (System.getProperty("atomix.log.level") == null) {
      System.setProperty("atomix.log.level", "INFO");
    }
    if (System.getProperty("atomix.log.file.level") == null) {
      System.setProperty("atomix.log.file.level", "INFO");
    }
    if (System.getProperty("atomix.log.console.level") == null) {
      System.setProperty("atomix.log.console.level", "INFO");
    }
    return LoggerFactory.getLogger(AtomixNodeRunner.class);
  }

  /**
   * Finds the protocol type from the classpath.
   *
   * @return the protocol type
   */
  private static Protocol.Type findProtocolType() throws Exception {
    final ClassGraph classGraph = new ClassGraph()
        .enableClassInfo()
        .enableAnnotationInfo();

    try (ScanResult result = classGraph.scan()) {
      for (ClassInfo classInfo : result.getClassesWithAnnotation(Component.class.getName())) {
        Class<?> componentClass = classInfo.loadClass();
        if (Protocol.Type.class.isAssignableFrom(componentClass)) {
          return (Protocol.Type) componentClass.newInstance();
        }
      }
    }
    throw new IllegalStateException("no protocol implementation found");
  }

  private static PartitionConfig parsePartitionConfig(File configFile) throws IOException {
    try (FileInputStream is = new FileInputStream(configFile)) {
      PartitionConfig.Builder builder = PartitionConfig.newBuilder();
      JsonFormat.parser().ignoringUnknownFields().merge(new InputStreamReader(is), builder);
      return builder.build();
    }
  }

  private static Message parseProtocolConfig(Protocol.Type protocolType, File configFile) throws IOException {
    try (FileInputStream is = new FileInputStream(configFile)) {
      return protocolType.parseConfig(is);
    }
  }

  /**
   * Builds a new Atomix instance from the given namespace.
   *
   * @param namespace the namespace from which to build the instance
   * @return the Atomix instance
   */
  private static AtomixNode buildServer(Namespace namespace) throws Exception {
    String nodeId = namespace.getString("node");
    PartitionConfig partitionConfig = parsePartitionConfig(namespace.get("config"));
    Protocol.Type protocolType = findProtocolType();
    Message protocolConfig = parseProtocolConfig(protocolType, namespace.get("protocol"));
    return new AtomixNode(nodeId, partitionConfig, protocolType, protocolConfig);
  }
}
