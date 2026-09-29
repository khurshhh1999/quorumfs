package io.quorumfs.protocol;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Conservative additive-only gate covering field, enum and RPC signatures, including nested types.
 */
class CompatibilityTest {
  @Test
  void currentDescriptorsPreserveV1() throws Exception {
    var baseline =
        FileDescriptorSet.parseFrom(
            Files.readAllBytes(Path.of(System.getProperty("quorumfs.baseline"))));
    var current =
        FileDescriptorSet.parseFrom(
            Files.readAllBytes(Path.of(System.getProperty("quorumfs.descriptor"))));
    for (var old : baseline.getFileList()) {
      if (!old.getName().startsWith("quorumfs/")) continue;
      var now =
          current.getFileList().stream()
              .filter(f -> f.getName().equals(old.getName()))
              .findFirst()
              .orElseThrow();
      assertEquals(old.getPackage(), now.getPackage());
      assertEquals(old.getSyntax(), now.getSyntax());
      assertEquals(old.getOptions(), now.getOptions());
      messages(old.getMessageTypeList(), now.getMessageTypeList());
      enums(old.getEnumTypeList(), now.getEnumTypeList());
      for (var service : old.getServiceList()) {
        var next =
            now.getServiceList().stream()
                .filter(s -> s.getName().equals(service.getName()))
                .findFirst()
                .orElseThrow();
        for (var method : service.getMethodList())
          assertTrue(next.getMethodList().contains(method), "RPC changed: " + method.getName());
      }
    }
  }

  private static void messages(List<DescriptorProto> before, List<DescriptorProto> after) {
    for (var old : before) {
      var now =
          after.stream().filter(m -> m.getName().equals(old.getName())).findFirst().orElseThrow();
      assertEquals(old.getOptions(), now.getOptions());
      assertEquals(
          old.getOneofDeclList(), now.getOneofDeclList().subList(0, old.getOneofDeclCount()));
      assertTrue(now.getReservedRangeList().containsAll(old.getReservedRangeList()));
      assertTrue(now.getReservedNameList().containsAll(old.getReservedNameList()));
      for (var field : old.getFieldList())
        assertTrue(
            now.getFieldList().contains(field),
            "Field changed: " + old.getName() + "." + field.getName());
      messages(old.getNestedTypeList(), now.getNestedTypeList());
      enums(old.getEnumTypeList(), now.getEnumTypeList());
    }
  }

  private static void enums(List<EnumDescriptorProto> before, List<EnumDescriptorProto> after) {
    for (var old : before) {
      var now =
          after.stream().filter(e -> e.getName().equals(old.getName())).findFirst().orElseThrow();
      assertTrue(now.getValueList().containsAll(old.getValueList()), "Enum values changed");
      assertTrue(now.getReservedRangeList().containsAll(old.getReservedRangeList()));
      assertTrue(now.getReservedNameList().containsAll(old.getReservedNameList()));
    }
  }
}
