package org.apache.avro;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class SchemaCompatibilityBaduaTest {

  private Schema schemaWrite;
  private Schema schemaRead;
  private int resultSize;

  public enum SchemaWriterType {
    INT, STRING, UNION, RECORD, CLASS
  }

  public enum SchemaReaderType {
    INT, STRING, UNION, RECORD,
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { SchemaWriterType.INT, SchemaReaderType.STRING }, // 0
        { SchemaWriterType.INT, SchemaReaderType.RECORD }, // 1
        { SchemaWriterType.INT, SchemaReaderType.UNION }, // 2

        { SchemaWriterType.STRING, SchemaReaderType.INT }, // 3
        { SchemaWriterType.STRING, SchemaReaderType.RECORD }, // 4
        { SchemaWriterType.STRING, SchemaReaderType.UNION }, // 5

        { SchemaWriterType.RECORD, SchemaReaderType.STRING }, // 6
        { SchemaWriterType.RECORD, SchemaReaderType.INT }, // 7
        { SchemaWriterType.RECORD, SchemaReaderType.UNION }, // 8

        { SchemaWriterType.UNION, SchemaReaderType.STRING }, // 9
        { SchemaWriterType.UNION, SchemaReaderType.RECORD }, // 10
        { SchemaWriterType.UNION, SchemaReaderType.INT },// 11

    });
  }

  public void configure(SchemaWriterType typeWriter, SchemaReaderType typeReader) {
    this.resultSize = 1;
    switch (typeReader) {
    case STRING:
      schemaRead = Schema.create(Schema.Type.STRING);
      switch (typeWriter) {
      case INT:
        schemaWrite = Schema.create(Schema.Type.INT);
        break;
      case UNION:
        Schema union1 = Schema.create(Schema.Type.BOOLEAN);
        Schema union2 = Schema.create(Schema.Type.BYTES);
        schemaWrite = Schema.createUnion(union1, union2);
        break;
      case RECORD:
        List<Schema.Field> fields = new ArrayList<>();
        Schema.Field field1 = new Schema.Field("x", Schema.create(Schema.Type.DOUBLE));
        Schema.Field field2 = new Schema.Field("y", Schema.create(Schema.Type.NULL));
        fields.add(field1);
        fields.add(field2);
        schemaWrite = Schema.createRecord("name", null, "", false, fields);
        break;
      }
      break;
    case INT:
      schemaRead = Schema.create(Schema.Type.INT);
      switch (typeWriter) {
      case STRING:
        schemaWrite = Schema.create(Schema.Type.STRING);
        break;
      case UNION:
        Schema union1 = Schema.create(Schema.Type.BOOLEAN);
        Schema union2 = Schema.create(Schema.Type.BYTES);
        schemaWrite = Schema.createUnion(union1, union2);
        this.resultSize = 2;
        break;
      case RECORD:
        List<Schema.Field> fields = new ArrayList<>();
        Schema.Field field1 = new Schema.Field("x", Schema.create(Schema.Type.DOUBLE));
        Schema.Field field2 = new Schema.Field("y", Schema.create(Schema.Type.NULL));
        fields.add(field1);
        fields.add(field2);
        schemaWrite = Schema.createRecord("name", null, "", false, fields);
        break;
      }
      break;
    case RECORD:
      List<Schema.Field> fields = new ArrayList<>();
      Schema.Field field1 = new Schema.Field("x", Schema.create(Schema.Type.DOUBLE));
      Schema.Field field2 = new Schema.Field("y", Schema.create(Schema.Type.NULL));
      fields.add(field1);
      fields.add(field2);
      schemaRead = Schema.createRecord("name", null, "", false, fields);
      this.resultSize = 2;
      switch (typeWriter) {
      case STRING:
        schemaWrite = Schema.create(Schema.Type.STRING);
        this.resultSize = 1;
        break;
      case UNION:
        Schema union1 = Schema.create(Schema.Type.BOOLEAN);
        Schema union2 = Schema.create(Schema.Type.BYTES);
        schemaWrite = Schema.createUnion(union1, union2);
        break;
      case INT:
        schemaWrite = Schema.create(Schema.Type.INT);
        this.resultSize = 1;
        break;
      }
      break;

    case UNION:
      Schema union1 = Schema.create(Schema.Type.BOOLEAN);
      schemaRead = Schema.createUnion(union1);
      this.resultSize = 1;
      switch (typeWriter) {
      case STRING:
        schemaWrite = Schema.create(Schema.Type.STRING);
        break;
      case INT:
        schemaWrite = Schema.create(Schema.Type.INT);
        break;
      case RECORD:
        List<Schema.Field> fields3 = new ArrayList<>();
        Schema.Field field7 = new Schema.Field("x", Schema.create(Schema.Type.DOUBLE));
        Schema.Field field8 = new Schema.Field("y", Schema.create(Schema.Type.NULL));
        fields3.add(field7);
        fields3.add(field8);
        schemaWrite = Schema.createRecord("name", null, "", false, fields3);
        break;
      }
      break;

    }
  }

  public SchemaCompatibilityBaduaTest(SchemaWriterType typeWriter, SchemaReaderType typeReader) {
    configure(typeWriter, typeReader);
  }

  @Test
  public void CompatibilityTest() {
    SchemaCompatibility.SchemaPairCompatibility pair = SchemaCompatibility.checkReaderWriterCompatibility(schemaRead,
        schemaWrite);
    Assert.assertEquals(pair.getType(), SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
    SchemaCompatibility.SchemaCompatibilityResult schemaResult = pair.getResult();
    Assert.assertEquals(schemaResult.getCompatibility(), SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
    List<SchemaCompatibility.Incompatibility> listIncompatibility = schemaResult.getIncompatibilities();
    Assert.assertEquals(listIncompatibility.size(), this.resultSize);
  }

}
