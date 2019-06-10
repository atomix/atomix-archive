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
package io.atomix.utils.config;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import com.google.common.base.CaseFormat;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;

/**
 * Protobuf utilities.
 */
public final class Protos {

  /**
   * Resolves a proto message name into a Java class.
   *
   * <p>
   * The full algorithm for this requires all of proto file options, proto package name, proto file
   * name, and fully qualified message name.
   *
   * @throws IllegalArgumentException if resolution fails.
   */
  @SuppressWarnings("unchecked")
  public static Class<? extends Message> getMessageClass(Descriptors.Descriptor descriptor) {
    return getMessageClass(
        descriptor.getFile().getOptions(),
        descriptor.getFile().getPackage(),
        descriptor.getFile().getName(),
        descriptor.getName());
  }

  /**
   * Resolves a proto message name into a Java class.
   *
   * <p>
   * The full algorithm for this requires all of proto file options, proto package name, proto file
   * name, and fully qualified message name.
   *
   * @throws ConfigurationException if resolution fails.
   */
  @SuppressWarnings("unchecked")
  public static Class<? extends Message> getMessageClass(
      DescriptorProtos.FileOptions fileOptions, String packageName, String fileName, String messageName) {

    messageName = fixTypeName(packageName, messageName);
    String javaName = getJavaName(fileOptions, packageName, fileName, messageName);

    // Resolve the class.
    try {
      Class<?> type = Class.forName(javaName);
      if (!Message.class.isAssignableFrom(type)) {
        throw new ConfigurationException(String.format("The type '%s' is not a message", type.getName()));
      }
      return (Class<? extends Message>) Class.forName(javaName);
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException(
          String.format("Cannot resolve type '%s' as mapped to Java type '%s'",
              packageName + "." + messageName.replace('$', '.'),
              javaName));
    }
  }

  /**
   * Resolves a proto enum name into a Java class.
   *
   * <p>
   * The full algorithm for this requires all of proto file options, proto package name, proto file
   * name, and fully qualified enum name.
   *
   * @throws ConfigurationException if resolution fails.
   */
  @SuppressWarnings("unchecked")
  public static Class<? extends ProtocolMessageEnum> getEnumClass(
      DescriptorProtos.FileOptions fileOptions, String packageName, String fileName, String enumName) {

    enumName = fixTypeName(packageName, enumName);
    String javaName = getJavaName(fileOptions, packageName, fileName, enumName);

    // Resolve the class.
    try {
      Class<?> type = Class.forName(javaName);
      //type.
      if (!ProtocolMessageEnum.class.isAssignableFrom(type)) {
        throw new ConfigurationException(String.format("The type '%s' is not a ProtocolMessageEnum", type.getName()));
      }
      return (Class<? extends ProtocolMessageEnum>) Class.forName(javaName);
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException(
          String.format("Cannot resolve type '%s' as mapped to Java type '%s'",
              packageName + "." + enumName.replace('$', '.'),
              javaName));
    }
  }

  /**
   * Returns a default instance for the given message descriptor. First tries to get it from a
   * concrete class representing the message. If that fails, falls back to use
   * {@link DynamicMessage}, which works, but is significant slower on parsing and such.
   */
  public static Message getDefaultInstance(Descriptors.Descriptor message) {
    Class<? extends Message> type =
        getMessageClass(
            message.getFile().getOptions(),
            message.getFile().getPackage(),
            message.getFile().getName(),
            message.getFullName());
    if (type != null) {
      return getDefaultInstance(type);
    }
    return DynamicMessage.getDefaultInstance(message);
  }

  /**
   * Returns a Enum for the given enum value descriptor. It tries to get it from a concrete class
   * representing the Enum. Returns null if cannot find the Enum.
   */
  @SuppressWarnings("unchecked")
  public static ProtocolMessageEnum getEnum(Descriptors.EnumValueDescriptor enumValueDescriptor) {
    Descriptors.EnumDescriptor enumDescriptor = enumValueDescriptor.getType();
    Class<? extends ProtocolMessageEnum> type =
        getEnumClass(
            enumDescriptor.getFile().getOptions(),
            enumDescriptor.getFile().getPackage(),
            enumDescriptor.getFile().getName(),
            enumDescriptor.getFullName());
    if (type != null) {
      try {
        return invoke(
            ProtocolMessageEnum.class,
            type.getMethod("valueOf", int.class),
            null,
            enumValueDescriptor.getNumber());
      } catch (NoSuchMethodException e) {
        return null;
      } catch (SecurityException e) {
        throw Throwables.propagate(e);
      }
    }
    return null;
  }

  /**
   * Returns the default instance for the message type.
   */
  public static Message getDefaultInstance(Class<? extends Message> type) {
    return invoke(Message.class, type, "getDefaultInstance", null);
  }

  /**
   * Invokes a method with receiver and arguments, and casts the result to a specified type.
   */
  public static <T> T invoke(Class<T> returnType, Method method, Object receiver, Object... args) {
    try {
      return returnType.cast(method.invoke(receiver, args));
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e.getCause());
    } catch (IllegalAccessException | IllegalArgumentException | SecurityException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Invokes a method with receiver and arguments, and casts the result to a list of messages.
   */
  @SuppressWarnings("unchecked")
  public static List<Message> invoke(Method method, Object receiver, Object... args) {
    try {
      Object result = method.invoke(receiver, args);
      if (result instanceof List<?>) {
        return (List<Message>) result;
      } else {
        throw new ConfigurationException(String.format("Method '%s' failed to return list", method.getName()));
      }
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e.getCause());
    } catch (IllegalAccessException | IllegalArgumentException | SecurityException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Invokes a named method with receiver and arguments, and casts the result to a specified type.
   */
  public static <T> T invoke(
      Class<T> returnType, Class<?> type, String name, Object receiver, Object... args) {
    try {
      return invoke(returnType, type.getMethod(name, getTypes(args)), receiver, args);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /**
   * Returns a value of a field. If the field is a map, goes the extra mile to get the real map via
   * reflection.
   */
  public static Object getField(GeneratedMessage target, Descriptors.FieldDescriptor field) {
    if (!field.isMapField()) {
      return target.getField(field);
    }
    return invoke(
        Object.class,
        target.getClass(),
        "get" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, field.getName()),
        target);
  }

  private static Class<?>[] getTypes(Object[] args) {
    Class<?>[] result = new Class<?>[args.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = args[i].getClass();
    }
    return result;
  }

  /**
   * Removes the package name from the typeName and also replaces 'dot's with '$' signs.
   */
  private static String fixTypeName(String packageName, String typeName) {
    // Remove the package name prefix from the messageName
    if (typeName.startsWith(packageName + ".")) {
      typeName = typeName.substring(packageName.length() + 1);
    }

    // Replace any nesting from messages by '$', Java's convention for inner classes.
    typeName = typeName.replace('.', '$');
    return typeName;
  }

  /**
   * Resolves a proto element name into a Java class name.
   *
   * <p>
   * The full algorithm for this requires all of proto file options, proto package name, proto file
   * name, and fully qualified proto element name.
   */
  private static String getJavaName(
      DescriptorProtos.FileOptions fileOptions, String packageName, String fileName, String typeName) {
    // Compute the Java class name.
    String javaName = fileOptions.getJavaPackage();
    if (javaName.isEmpty()) {
      javaName = "com.google.protos." + packageName;
    }
    if (!fileOptions.getJavaMultipleFiles()) {
      if (fileOptions.hasJavaOuterClassname()) {
        javaName = javaName + "." + fileOptions.getJavaOuterClassname();
      } else {
        fileName = Files.getNameWithoutExtension(new File(fileName).getName());
        javaName =
            javaName + "." + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fileName);
      }
      javaName = javaName + "$" + typeName;
    } else {
      javaName = javaName + "." + typeName;
    }
    return javaName;
  }
}
