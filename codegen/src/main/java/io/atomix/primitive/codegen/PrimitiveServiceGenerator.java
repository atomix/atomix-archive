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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.compiler.PluginProtos;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.impl.PrimitiveServiceProto;
import io.atomix.primitive.service.impl.ServiceTypeInfo;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Primitive service compiler.
 */
public class PrimitiveServiceGenerator {

  private static final String OPERATIONS_TEMPLATE = "operations.ftlh";
  private static final String SIMPLE_SERVICE_TEMPLATE = "simple_service.ftlh";
  private static final String SESSION_SERVICE_TEMPLATE = "session_service.ftlh";
  private static final String SIMPLE_PROXY_TEMPLATE = "simple_proxy.ftlh";
  private static final String SESSION_PROXY_TEMPLATE = "session_proxy.ftlh";

  private static final String OPERATIONS_SUFFIX = "Operations";
  private static final String SERVICE_SUFFIX = "Service";
  private static final String PROXY_SUFFIX = "Proxy";
  private static final String ABSTRACT_PREFIX = "Abstract";

  /**
   * Generates the given code generator request.
   *
   * @param request the code generator request
   * @return the code generator response
   */
  public PluginProtos.CodeGeneratorResponse generate(PluginProtos.CodeGeneratorRequest request) throws IOException {
    MessageTable messages = new MessageTable();
    PluginProtos.CodeGeneratorResponse.Builder response = PluginProtos.CodeGeneratorResponse.newBuilder();
    for (DescriptorProtos.FileDescriptorProto fileDescriptor : request.getProtoFileList()) {
      for (DescriptorProtos.DescriptorProto descriptor : fileDescriptor.getMessageTypeList()) {
        messages.add(fileDescriptor, descriptor);
      }
      generate(fileDescriptor, messages, response);
    }
    return response.build();
  }

  /**
   * Generates the given file.
   *
   * @param fileDescriptor the file descriptor
   * @param messages       the message lookup table
   * @param response       the codegen response
   */
  private void generate(
      DescriptorProtos.FileDescriptorProto fileDescriptor,
      MessageTable messages,
      PluginProtos.CodeGeneratorResponse.Builder response) throws IOException {
    for (DescriptorProtos.ServiceDescriptorProto serviceDescriptor : fileDescriptor.getServiceList()) {
      ServiceDescriptor serviceContext = buildServiceContext(serviceDescriptor, fileDescriptor, messages);

      // Generate the primitive operations.
      if (!serviceContext.getOperations().isEmpty()) {
        generate(
            OPERATIONS_TEMPLATE,
            serviceContext.getOperationsClass(),
            serviceContext,
            response);
      }

      if (!serviceContext.getOperations().isEmpty()) {
        if (serviceContext.isSession()) {
          // Generate the primitive service base class.
          generate(
              SESSION_SERVICE_TEMPLATE,
              serviceContext.getServiceClass(),
              serviceContext,
              response);

          // Generate the proxy class.
          generate(
              SESSION_PROXY_TEMPLATE,
              serviceContext.getProxyClass(),
              serviceContext,
              response);
        } else {
          // Generate the primitive service base class.
          generate(
              SIMPLE_SERVICE_TEMPLATE,
              serviceContext.getServiceClass(),
              serviceContext,
              response);

          // Generate the proxy class.
          generate(
              SIMPLE_PROXY_TEMPLATE,
              serviceContext.getProxyClass(),
              serviceContext,
              response);
        }
      }
    }
  }

  /**
   * Compiles the given service class to the given file.
   *
   * @param templateFileName the template from which to generate the file
   * @param classDescriptor  the class descriptor
   * @param serviceContext   the service descriptor
   * @param response         the code generator response
   */
  private void generate(
      String templateFileName,
      ClassDescriptor classDescriptor,
      ServiceDescriptor serviceContext,
      PluginProtos.CodeGeneratorResponse.Builder response) throws IOException {
    Configuration config = new Configuration();
    config.setClassForTemplateLoading(getClass(), "/");

    Template template = config.getTemplate(templateFileName);
    Writer writer = new StringWriter();
    try {
      template.process(serviceContext, writer);
    } catch (TemplateException e) {
      throw new IOException(e);
    }

    response.addFile(PluginProtos.CodeGeneratorResponse.File.newBuilder()
        .setContent(writer.toString())
        .setName(classDescriptor.getFileName())
        .build());

    System.err.println(writer.toString());
  }

  /**
   * Builds a service context for code generation.
   *
   * @param serviceDescriptor the service descriptor
   * @param fileDescriptor    the descriptor for the file to which the service belongs
   * @param messages          the message lookup table
   * @return the generated service context
   */
  private ServiceDescriptor buildServiceContext(
      DescriptorProtos.ServiceDescriptorProto serviceDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor,
      MessageTable messages) {
    ServiceDescriptor context = new ServiceDescriptor();
    context.setServiceName(getServiceName(serviceDescriptor));
    context.setServiceClass(buildServiceClassDescriptor(serviceDescriptor, fileDescriptor));
    context.setProxyClass(buildProxyClassDescriptor(serviceDescriptor, fileDescriptor));
    context.setOperationsClass(buildOperationsClassDescriptor(serviceDescriptor, fileDescriptor));
    context.setSession(isSessionService(serviceDescriptor));
    for (DescriptorProtos.MethodDescriptorProto methodDescriptor : serviceDescriptor.getMethodList()) {
      if (isOperationMethod(methodDescriptor)) {
        context.addOperation(buildOperationDescriptor(methodDescriptor, serviceDescriptor, fileDescriptor, messages));
      }
    }
    return context;
  }

  /**
   * Builds an class descriptor for the given service.
   *
   * @param serviceDescriptor the service descriptor
   * @param fileDescriptor    the descriptor for the file to which the service belongs
   * @return the generated service class
   */
  private ClassDescriptor buildServiceClassDescriptor(
      DescriptorProtos.ServiceDescriptorProto serviceDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor) {
    ClassDescriptor serviceClass = new ClassDescriptor();
    serviceClass.setPackageName(getPackageName(fileDescriptor));
    serviceClass.setFileName(getAbstractFilePath(serviceDescriptor, fileDescriptor));
    serviceClass.setClassName(getPrefixedClassName(ABSTRACT_PREFIX, serviceDescriptor));
    return serviceClass;
  }

  /**
   * Builds an class descriptor for the given proxy.
   *
   * @param serviceDescriptor the service descriptor
   * @param fileDescriptor    the descriptor for the file to which the service belongs
   * @return the generated service class
   */
  private ClassDescriptor buildProxyClassDescriptor(
      DescriptorProtos.ServiceDescriptorProto serviceDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor) {
    ClassDescriptor proxyClass = new ClassDescriptor();
    proxyClass.setPackageName(getPackageName(fileDescriptor));
    proxyClass.setFileName(getBaseFilePath(PROXY_SUFFIX, serviceDescriptor, fileDescriptor));
    proxyClass.setClassName(getSuffixedClassName(PROXY_SUFFIX, serviceDescriptor));
    return proxyClass;
  }

  /**
   * Builds an class descriptor for the given service operations.
   *
   * @param serviceDescriptor the service descriptor
   * @param fileDescriptor    the descriptor for the file to which the service belongs
   * @return the generated service class
   */
  private ClassDescriptor buildOperationsClassDescriptor(
      DescriptorProtos.ServiceDescriptorProto serviceDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor) {
    ClassDescriptor operationsClass = new ClassDescriptor();
    operationsClass.setPackageName(getPackageName(fileDescriptor));
    operationsClass.setFileName(getBaseFilePath(OPERATIONS_SUFFIX, serviceDescriptor, fileDescriptor));
    operationsClass.setClassName(getSuffixedClassName(OPERATIONS_SUFFIX, serviceDescriptor));
    return operationsClass;
  }

  /**
   * Builds an class descriptor for the given service message.
   *
   * @param messageDescriptor the message descriptor
   * @param fileDescriptor    the descriptor for the file to which the service belongs
   * @return the generated service class
   */
  private ClassDescriptor buildMessageClassDescriptor(
      DescriptorProtos.DescriptorProto messageDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor) {
    ClassDescriptor messageClass = new ClassDescriptor();
    messageClass.setPackageName(getPackageName(fileDescriptor));
    messageClass.setFileName(getFilePath(messageDescriptor, fileDescriptor));
    messageClass.setClassName(getClassName(messageDescriptor));
    return messageClass;
  }

  /**
   * Builds a descriptor for the given service operation.
   *
   * @param methodDescriptor the method descriptor
   * @param messages         the message lookup table
   * @return the generated method
   */
  private OperationDescriptor buildOperationDescriptor(
      DescriptorProtos.MethodDescriptorProto methodDescriptor,
      DescriptorProtos.ServiceDescriptorProto serviceDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor,
      MessageTable messages) {
    Pair<DescriptorProtos.FileDescriptorProto, DescriptorProtos.DescriptorProto> inputType = messages.get(methodDescriptor.getInputType());
    Pair<DescriptorProtos.FileDescriptorProto, DescriptorProtos.DescriptorProto> outputType = messages.get(methodDescriptor.getOutputType());
    OperationDescriptor operation = new OperationDescriptor();
    operation.setMethodName(getMethodName(methodDescriptor));
    operation.setRequestClass(buildMessageClassDescriptor(inputType.getRight(), inputType.getLeft()));
    operation.setResponseClass(buildMessageClassDescriptor(outputType.getRight(), outputType.getLeft()));
    operation.setOperationsClass(buildOperationsClassDescriptor(serviceDescriptor, fileDescriptor));
    operation.setAsync(isAsyncMethod(methodDescriptor));
    operation.setStream(isStreamMethod(methodDescriptor));
    operation.setType(getOperationType(methodDescriptor));
    operation.setName(getOperationName(methodDescriptor));
    operation.setConstantName(getOperationConstant(methodDescriptor));
    return operation;
  }

  /**
   * Returns a boolean indicating whether the given service is a session enabled service.
   *
   * @param serviceDescriptor the service descriptor
   * @return indicates whether the given service is a session enabled service
   */
  private boolean isSessionService(DescriptorProtos.ServiceDescriptorProto serviceDescriptor) {
    return serviceDescriptor.getOptions().hasExtension(PrimitiveServiceProto.type)
        && serviceDescriptor.getOptions().getExtension(PrimitiveServiceProto.type) == ServiceTypeInfo.SESSION;
  }

  /**
   * Returns a boolean indicating whether the given descriptor is for a primitive operation.
   *
   * @param methodDescriptor the method descriptor
   * @return indicates whether the given descriptor is a primitive operation
   */
  private boolean isOperationMethod(DescriptorProtos.MethodDescriptorProto methodDescriptor) {
    return methodDescriptor.getOptions().hasExtension(PrimitiveServiceProto.operation);
  }

  /**
   * Returns a boolean indicating whether the given method is an asynchronous method.
   *
   * @param methodDescriptor tne method descriptor
   * @return indicates whether the given method is an asynchronous method
   */
  private boolean isAsyncMethod(DescriptorProtos.MethodDescriptorProto methodDescriptor) {
    return methodDescriptor.getOptions().getExtension(PrimitiveServiceProto.operation).getAsync();
  }

  /**
   * Returns a boolean indicating whether the given method is a streaming method.
   *
   * @param methodDescriptor tne method descriptor
   * @return indicates whether the given method is a streaming method
   */
  private boolean isStreamMethod(DescriptorProtos.MethodDescriptorProto methodDescriptor) {
    return methodDescriptor.getServerStreaming();
  }

  /**
   * Returns the Java package name for the given file.
   *
   * @param fileDescriptor the file descriptor
   * @return the Java package for the given file
   */
  private static String getPackageName(DescriptorProtos.FileDescriptorProto fileDescriptor) {
    return fileDescriptor.getOptions().getJavaPackage();
  }

  /**
   * Returns the Java class name for the given message.
   *
   * @param messageDescriptor the service descriptor
   * @return the Java class name
   */
  private static String getClassName(DescriptorProtos.DescriptorProto messageDescriptor) {
    return messageDescriptor.getName();
  }

  /**
   * Returns the Java class name for the given service.
   *
   * @param serviceDescriptor the service descriptor
   * @return the Java class name
   */
  private static String getClassName(DescriptorProtos.ServiceDescriptorProto serviceDescriptor) {
    return serviceDescriptor.getName();
  }

  /**
   * Returns the Java package path for the given file.
   *
   * @param fileDescriptor the file descriptor
   * @return the Java package path
   */
  private static String getPackagePath(DescriptorProtos.FileDescriptorProto fileDescriptor) {
    return getPackageName(fileDescriptor).replace('.', '/');
  }

  /**
   * Returns the service name for the given service.
   *
   * @param serviceDescriptor the service descriptor
   * @return the service name
   */
  private static String getServiceName(DescriptorProtos.ServiceDescriptorProto serviceDescriptor) {
    if (serviceDescriptor.getOptions().hasExtension(PrimitiveServiceProto.name)) {
      return serviceDescriptor.getOptions().getExtension(PrimitiveServiceProto.name);
    }
    return getBaseName(serviceDescriptor);
  }

  /**
   * Returns the base name for the given service.
   *
   * @param serviceDescriptor the service descriptor
   * @return the service base name
   */
  private static String getBaseName(DescriptorProtos.ServiceDescriptorProto serviceDescriptor) {
    String className = getClassName(serviceDescriptor);
    if (className.endsWith(SERVICE_SUFFIX)) {
      return className.substring(0, className.lastIndexOf(SERVICE_SUFFIX));
    }
    return className;
  }

  /**
   * Returns the full file path for the given message type.
   *
   * @param messageDescriptor the service descriptor
   * @param fileDescriptor    the file descriptor
   * @return the full file path for the Java class
   */
  private static String getFilePath(
      DescriptorProtos.DescriptorProto messageDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor) {
    return getPackagePath(fileDescriptor) + "/" + messageDescriptor.getName() + ".java";
  }

  /**
   * Returns the full file path for the given service with the given suffix.
   *
   * @param suffix            the filename suffix
   * @param serviceDescriptor the service descriptor
   * @param fileDescriptor    the file descriptor
   * @return the full file path for the Java class
   */
  private static String getBaseFilePath(
      String suffix,
      DescriptorProtos.ServiceDescriptorProto serviceDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor) {
    return getPackagePath(fileDescriptor) + "/" + getBaseName(serviceDescriptor) + suffix + ".java";
  }

  /**
   * Returns the full file path to the abstract Java class for the given service.
   *
   * @param serviceDescriptor the service descriptor
   * @param fileDescriptor    the file descriptor
   * @return the full file path for the abstract Java class
   */
  private static String getAbstractFilePath(
      DescriptorProtos.ServiceDescriptorProto serviceDescriptor,
      DescriptorProtos.FileDescriptorProto fileDescriptor) {
    return getPackagePath(fileDescriptor) + "/" + ABSTRACT_PREFIX + getClassName(serviceDescriptor) + ".java";
  }

  /**
   * Returns the class name for the given service.
   *
   * @param prefix            the class name prefix
   * @param serviceDescriptor the service descriptor
   * @return the class name
   */
  private static String getPrefixedClassName(String prefix, DescriptorProtos.ServiceDescriptorProto serviceDescriptor) {
    return prefix + getClassName(serviceDescriptor);
  }

  /**
   * Returns the class name for the given service.
   *
   * @param suffix            the class name suffix
   * @param serviceDescriptor the service descriptor
   * @return the class name
   */
  private static String getSuffixedClassName(String suffix, DescriptorProtos.ServiceDescriptorProto serviceDescriptor) {
    return getBaseName(serviceDescriptor) + suffix;
  }

  /**
   * Returns the Java method name for the given method.
   *
   * @param methodDescriptor the method descriptor
   * @return the Java method name
   */
  private static String getMethodName(DescriptorProtos.MethodDescriptorProto methodDescriptor) {
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, methodDescriptor.getName());
  }

  /**
   * Returns the Java message type name for the given Protobuf message type.
   *
   * @param typeName the Protobuf type name
   * @param messages the message lookup table
   * @return the Java message type name
   */
  private static String getTypeName(String typeName, MessageTable messages) {
    return messages.getClassName(typeName);
  }

  /**
   * Returns the name of the given operation.
   *
   * @param methodDescriptor the method descriptor
   * @return the operation name
   */
  private static String getOperationName(DescriptorProtos.MethodDescriptorProto methodDescriptor) {
    String name = methodDescriptor.getOptions().getExtension(PrimitiveServiceProto.operation).getName();
    return Strings.isNullOrEmpty(name)
        ? getMethodName(methodDescriptor)
        : name;
  }

  /**
   * Returns the operation constant name.
   *
   * @param methodDescriptor the method descriptor
   * @return the operation constant name
   */
  private static String getOperationConstant(DescriptorProtos.MethodDescriptorProto methodDescriptor) {
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, methodDescriptor.getName());
  }

  /**
   * Returns the operation type name.
   *
   * @param methodDescriptor the method descriptor
   * @return the operation type name
   */
  private static String getOperationType(DescriptorProtos.MethodDescriptorProto methodDescriptor) {
    return methodDescriptor.getOptions().getExtension(PrimitiveServiceProto.operation).getType().name();
  }

  /**
   * A table mapping message types to descriptors.
   */
  private class MessageTable {
    private final Map<String, Pair<DescriptorProtos.FileDescriptorProto, DescriptorProtos.DescriptorProto>> messages = new HashMap<>();
    private final List<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();

    /**
     * Adds a message type to the table.
     *
     * @param fileDescriptor    the descriptor for the file to which the message type belongs
     * @param messageDescriptor the message descriptor
     */
    void add(DescriptorProtos.FileDescriptorProto fileDescriptor, DescriptorProtos.DescriptorProto messageDescriptor) {
      String prefix = "." + fileDescriptor.getPackage() + "." + messageDescriptor.getName();
      try {
        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(
            fileDescriptor, fileDescriptors.toArray(new Descriptors.FileDescriptor[0])
        );
        fileDescriptors.add(fd);
        messages.put(prefix, Pair.of(fileDescriptor, fd.findMessageTypeByName(messageDescriptor.getName()).toProto()));
      } catch (Descriptors.DescriptorValidationException e) {
      }
    }

    /**
     * Returns the file and message descriptor for the given message type.
     *
     * @param messageType the message type
     * @return the file and message descriptor for the given message type
     */
    Pair<DescriptorProtos.FileDescriptorProto, DescriptorProtos.DescriptorProto> get(String messageType) {
      return messages.get(messageType);
    }

    /**
     * Returns the fully qualified class name for the given protobuf message type relative to the current file.
     *
     * @param typeName the protobuf message type
     * @return the fully qualified Java class name
     */
    String getClassName(String typeName) {
      Pair<DescriptorProtos.FileDescriptorProto, DescriptorProtos.DescriptorProto> typeInfo = messages.get(typeName);
      if (typeInfo == null) {
        return null;
      }
      return typeInfo.getLeft().getOptions().getJavaPackage() + "." + typeInfo.getRight().getName();
    }
  }

  public static class ServiceDescriptor {
    private String serviceName;
    private ClassDescriptor serviceClass;
    private ClassDescriptor proxyClass;
    private ClassDescriptor operationsClass;
    private ClassDescriptor eventsClass;
    private List<OperationDescriptor> operations = new ArrayList<>();
    private boolean session;

    public String getServiceName() {
      return serviceName;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }

    public ClassDescriptor getServiceClass() {
      return serviceClass;
    }

    public void setServiceClass(ClassDescriptor serviceClass) {
      this.serviceClass = serviceClass;
    }

    public ClassDescriptor getProxyClass() {
      return proxyClass;
    }

    public void setProxyClass(ClassDescriptor proxyClass) {
      this.proxyClass = proxyClass;
    }

    public ClassDescriptor getOperationsClass() {
      return operationsClass;
    }

    public void setOperationsClass(ClassDescriptor operationsClass) {
      this.operationsClass = operationsClass;
    }

    public ClassDescriptor getEventsClass() {
      return eventsClass;
    }

    public void setEventsClass(ClassDescriptor eventsClass) {
      this.eventsClass = eventsClass;
    }

    public List<OperationDescriptor> getOperations() {
      return operations;
    }

    public void setOperations(List<OperationDescriptor> operations) {
      this.operations = operations;
    }

    public void addOperation(OperationDescriptor operation) {
      operations.add(operation);
    }

    public boolean isHasCommands() {
      return operations.stream().anyMatch(operation -> operation.getType().equals(OperationType.COMMAND.name()));
    }

    public boolean isHasQueries() {
      return operations.stream().anyMatch(operation -> operation.getType().equals(OperationType.QUERY.name()));
    }

    public boolean isHasStreams() {
      return operations.stream().anyMatch(OperationDescriptor::isStream);
    }

    public boolean isHasAsyncs() {
      return operations.stream().anyMatch(OperationDescriptor::isAsync);
    }

    public boolean isSession() {
      return session || isHasStreams() || isHasAsyncs();
    }

    public void setSession(boolean session) {
      this.session = session;
    }
  }

  public static class ClassDescriptor {
    private String fileName;
    private String packageName;
    private String className;

    ClassDescriptor() {
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public String getPackageName() {
      return packageName;
    }

    public void setPackageName(String packageName) {
      this.packageName = packageName;
    }

    public String getClassName() {
      return className;
    }

    public void setClassName(String className) {
      this.className = className;
    }

    public String getQualifiedName() {
      return getPackageName() + "." + getClassName();
    }
  }

  public static class OperationDescriptor {
    private String methodName;
    private ClassDescriptor requestClass;
    private ClassDescriptor responseClass;
    private ClassDescriptor operationsClass;
    private boolean async;
    private boolean stream;
    private String type;
    private String name;
    private String constantName;

    public String getMethodName() {
      return methodName;
    }

    public void setMethodName(String methodName) {
      this.methodName = methodName;
    }

    public ClassDescriptor getRequestClass() {
      return requestClass;
    }

    public void setRequestClass(ClassDescriptor requestClass) {
      this.requestClass = requestClass;
    }

    public ClassDescriptor getResponseClass() {
      return responseClass;
    }

    public void setResponseClass(ClassDescriptor responseClass) {
      this.responseClass = responseClass;
    }

    public ClassDescriptor getOperationsClass() {
      return operationsClass;
    }

    public void setOperationsClass(ClassDescriptor operationsClass) {
      this.operationsClass = operationsClass;
    }

    public boolean isAsync() {
      return async;
    }

    public void setAsync(boolean async) {
      this.async = async;
    }

    public boolean isStream() {
      return stream;
    }

    public void setStream(boolean stream) {
      this.stream = stream;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getConstantName() {
      return constantName;
    }

    public void setConstantName(String constantName) {
      this.constantName = constantName;
    }
  }
}