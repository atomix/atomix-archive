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
package io.atomix.server.runner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import io.atomix.server.AtomixServer;
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
    final List<String> unknown = new ArrayList<>();
    final Namespace namespace = parseArgs(args, unknown);
    final Namespace extraArgs = parseUnknown(unknown);
    extraArgs.getAttrs().forEach((key, value) -> System.setProperty(key, value.toString()));

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
  static Namespace parseArgs(String[] args, List<String> unknown) {
    ArgumentParser parser = createParser();
    Namespace namespace = null;
    try {
      namespace = parser.parseKnownArgs(args, unknown);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }
    return namespace;
  }

  /**
   * Parses unknown arguments, returning an argparse4j namespace.
   *
   * @param unknown the unknown arguments to parse
   * @return the namespace
   * --foo.bar baz --bar.baz foo bar --foo.bar.baz bang
   */
  static Namespace parseUnknown(List<String> unknown) {
    Map<String, Object> attrs = new HashMap<>();
    String attr = null;
    for (String arg : unknown) {
      if (arg.startsWith("--")) {
        int splitIndex = arg.indexOf('=');
        if (splitIndex == -1) {
          attr = arg.substring(2);
        } else {
          attrs.put(arg.substring(2, splitIndex), arg.substring(splitIndex + 1));
          attr = null;
        }
      } else if (attr != null) {
        attrs.put(attr, arg);
        attr = null;
      }
    }
    return new Namespace(attrs);
  }

  /**
   * Creates an agent argument parser.
   */
  private static ArgumentParser createParser() {
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Runs the Atomix server with the given arguments.");
    parser.addArgument("--config", "-c")
        .metavar("YAML")
        .type(File.class)
        .nargs("*")
        .required(false)
        .setDefault(System.getProperty("atomix.config.files") != null
            ? Lists.newArrayList(System.getProperty("atomix.config.files").split(","))
            : Lists.newArrayList())
        .help("Path to Atomix configuration files. Multiple files are recursively merged in the specified order.");
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
    System.setProperty("atomix.log.directory", namespace.getString("log_dir"));
    System.setProperty("atomix.log.level", namespace.getString("log_level"));
    System.setProperty("atomix.log.console.level", namespace.getString("console_log_level"));
    System.setProperty("atomix.log.file.level", namespace.getString("file_log_level"));
    return LoggerFactory.getLogger(AtomixServerRunner.class);
  }

  /**
   * Builds a new Atomix instance from the given namespace.
   *
   * @param namespace the namespace from which to build the instance
   * @return the Atomix instance
   */
  private static AtomixServer buildServer(Namespace namespace) {
    final List<File> configFiles = namespace.getList("config");
    return new AtomixServer(Thread.currentThread().getContextClassLoader(), configFiles);
  }
}
