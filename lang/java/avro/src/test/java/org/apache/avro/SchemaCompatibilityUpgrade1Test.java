package org.apache.avro;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class SchemaCompatibilityUpgrade1Test {
  private Schema schema;
  private Schema.Field field;
  private Schema.Field expectedOutput;
  private boolean isExceptionExpected;

  private boolean test1;

  public enum ParamListField {
    NULL, VOID, VALID, DUPLICATEDALIAS
  }

  public enum ParamField {
    NULL, PRESENT, NOTPRESENT
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { true, Schema.Type.INT, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.RECORD, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.RECORD, ParamListField.VALID, ParamField.PRESENT },
        { false, Schema.Type.RECORD, ParamListField.DUPLICATEDALIAS, ParamField.PRESENT },
        { false, Schema.Type.RECORD, ParamListField.VOID, ParamField.PRESENT },
        { false, Schema.Type.RECORD, ParamListField.NULL, ParamField.PRESENT },
        { false, Schema.Type.RECORD, ParamListField.VALID, ParamField.NOTPRESENT },
        { false, Schema.Type.BOOLEAN, ParamListField.VOID, ParamField.NULL },
        { false, Schema.Type.DOUBLE, ParamListField.NULL, ParamField.NULL },
        { false, Schema.Type.STRING, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.LONG, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.BYTES, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.FLOAT, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.MAP, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.ARRAY, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.UNION, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.ENUM, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.NULL, ParamListField.VALID, ParamField.NULL },
        { false, Schema.Type.INT, ParamListField.NULL, ParamField.NULL }, });
  }

  public void configure(boolean nullable, Schema.Type type, ParamListField listField, ParamField field) {
    if (nullable) {
      this.schema = null;
    } else {
      Schema valuetype = mock(Schema.class);
      Mockito.when(valuetype.getFullName()).thenReturn("test");
      switch (type) {
      case MAP:
        this.schema = Schema.createMap(valuetype);
        break;
      case FIXED:
        this.schema = Schema.createFixed("name", "doc", "space", 12);
        break;
      case UNION:
        this.schema = Schema.createUnion(valuetype);
        break;
      case ARRAY:
        this.schema = Schema.createArray(valuetype);
        break;
      case ENUM:
        List<String> stringList = new ArrayList<>();
        this.schema = Schema.createEnum("int", "doc", "int", stringList);
        break;
      case RECORD:
        this.schema = Schema.createRecord("INT", "doc", "int", false);
        switch (listField) {
        case NULL:
          break;
        case VOID:
          this.schema.setFields(new ArrayList<Schema.Field>());
          break;
        case VALID:
          Schema.Field recordField = new Schema.Field("Valore", this.schema, null, null);
          List<Schema.Field> recordFields = new ArrayList<>();
          recordFields.add(recordField);
          this.schema.setFields(recordFields);
          break;
        case DUPLICATEDALIAS:
          Schema.Field recordField1 = new Schema.Field("Valore", this.schema, null, null);
          Schema.Field recordField2 = new Schema.Field("Value", this.schema, null, null);
          Schema.Field recordField3 = new Schema.Field("Val", this.schema, null, null);
          List<Schema.Field> recordFields1 = new ArrayList<>();
          recordFields1.add(recordField1);
          recordFields1.add(recordField2);
          recordFields1.add(recordField3);
          this.schema.setFields(recordFields1);
          break;

        }
        break;
      default:
        this.schema = Schema.create(type);
        break;
      }
      Schema schemaAppoggio = Schema.createRecord("INT", "doc", "int", false);
      List<Schema.Field> list = new ArrayList<>();
      switch (field) {
      case PRESENT:
        this.field = new Schema.Field("Valore", schemaAppoggio, null, null);
        this.field.addAlias("Value");
        list.add(this.field);
        schemaAppoggio.setFields(list);
        expectedOutput = this.field;
        break;
      case NOTPRESENT:
        this.field = new Schema.Field("ValoreNoNValido", schemaAppoggio, null, null);
        list.add(this.field);
        schemaAppoggio.setFields(list);
        expectedOutput = null;
        break;
      case NULL:
        this.field = null;
        expectedOutput = null;
        break;
      }
    }
    if (nullable || type != Schema.Type.RECORD || listField == ParamListField.NULL
        || listField == ParamListField.DUPLICATEDALIAS || listField == ParamListField.VOID || field == ParamField.NULL)
      this.isExceptionExpected = true;
    else
      this.isExceptionExpected = false;
  }

  public SchemaCompatibilityUpgrade1Test(boolean nullable, Schema.Type type, ParamListField listField,
      ParamField field) {
    this.test1 = true;
    configure(nullable, type, listField, field);
  }

  @Test
  public void testLookUpWriterFields() {
    try {
      Schema.Field actual = SchemaCompatibility.lookupWriterField(this.schema, this.field);
      Assert.assertEquals(this.expectedOutput, actual);
    } catch (AssertionError e) {
      Assert.assertEquals(true, this.isExceptionExpected);
    } catch (AvroRuntimeException e) {
      Assert.assertEquals(true, this.isExceptionExpected);
    } catch (NullPointerException e) {
      Assert.assertEquals(true, this.isExceptionExpected);
    }

  }

}
