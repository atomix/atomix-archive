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
package io.atomix.server;

import java.io.File;

import io.atomix.server.management.impl.ConfigServiceImpl;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Atomix server runner.
 */
public class AtomixServerRunner {

  /**
   * Runs a standalone Atomix server from the given command line arguments.
   *
   * @param args the program arguments
   * @throws Exception if the supplied arguments are invalid
   */
  public static void main(String[] args) throws Exception {
    // Parse the command line arguments.
    final Namespace namespace = parseArgs(args);
    final Logger logger = createLogger(namespace);

    logger.info("Starting server");
    final AtomixServer atomix = buildServer(namespace);
    atomix.start().join();

    synchronized (AtomixServer.class) {
      while (atomix.isRunning()) {
        AtomixServer.class.wait();
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
   * @param namespace the namespace from which to create the logger configuration
   * @return a new agent logger
   */
  static Logger createLogger(Namespace namespace) {
    String logConfig = namespace.getString("log_config");
    if (logConfig != null) {
      System.setProperty("logback.configurationFile", logConfig);
    }
    if (System.getProperty("atomix.log.directory") == null) {
      System.setProperty("atomix.log.directory", "log");
    }
    if (System.getProperty("atomix.log.file.level") == null) {
      System.setProperty("atomix.log.file.level", "INFO");
    }
    if (System.getProperty("atomix.log.console.level") == null) {
      System.setProperty("atomix.log.console.level", "INFO");
    }
    return LoggerFactory.getLogger(AtomixServerRunner.class);
  }

  /**
   * Builds a new Atomix instance from the given namespace.
   *
   * @param namespace the namespace from which to build the instance
   * @return the Atomix instance
   */
  private static AtomixServer buildServer(Namespace namespace) throws Exception {
    ConfigServiceImpl.load(namespace.get("config"));
    return new AtomixServer(namespace.get("protocol"));
  }
}
