package org.apache.avro;

import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Union;
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
public class SchemaCompatibility2Test {

  private boolean isExceptionExpected;

  private Schema schemaWriter;
  private Schema schemaReader;
  private SchemaCompatibility.SchemaCompatibilityType typeResult;

  public enum SchemaWriterType {
    NULL, INT, STRING,
  }

  public enum SchemaReaderType {
    NULL, SAME, NOT_SAME
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {

        { SchemaWriterType.NULL, SchemaReaderType.NULL }, { SchemaWriterType.INT, SchemaReaderType.SAME },
        { SchemaWriterType.STRING, SchemaReaderType.SAME }, { SchemaWriterType.INT, SchemaReaderType.NOT_SAME },
        { SchemaWriterType.STRING, SchemaReaderType.NOT_SAME },

    });
  }

  public void configure2(SchemaWriterType typeWriter, SchemaReaderType typeReader) {
    this.isExceptionExpected = false;
    if (typeReader == SchemaReaderType.NULL) {
      this.schemaReader = null;
      this.isExceptionExpected = true;
    }
    switch (typeWriter) {
    case NULL:
      this.schemaWriter = null;
      this.isExceptionExpected = true;
      this.schemaReader = null;
      break;
    case INT:
      this.schemaWriter = Schema.create(Schema.Type.INT);
      if (typeReader == SchemaReaderType.SAME) {
        this.schemaReader = Schema.create(Schema.Type.INT);
        this.typeResult = SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
      }
      if (typeReader == SchemaReaderType.NOT_SAME) {
        this.schemaReader = Schema.create(Schema.Type.STRING);
        this.typeResult = SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE;
      }
      break;
    case STRING:
      this.schemaWriter = Schema.create(Schema.Type.STRING);
      if (typeReader == SchemaReaderType.SAME) {
        this.schemaReader = Schema.create(Schema.Type.STRING);
        this.typeResult = SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
      }
      if (typeReader == SchemaReaderType.NOT_SAME) {
        this.schemaReader = Schema.create(Schema.Type.INT);
        this.typeResult = SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE;
      }
      break;
    }

  }

  public SchemaCompatibility2Test(SchemaWriterType typeWritre, SchemaReaderType typeReader) {
    configure2(typeWritre, typeReader);
  }

  @Test
  public void testcheckReaderWriterCompatibility() {
    try {
      SchemaCompatibility.SchemaPairCompatibility p = SchemaCompatibility
          .checkReaderWriterCompatibility(this.schemaReader, this.schemaWriter);
      Assert.assertEquals(p.getType(), this.typeResult);
      Assert.assertFalse(this.isExceptionExpected);
    } catch (Exception e) {
      Assert.assertTrue(this.isExceptionExpected);
    } catch (AssertionError e) {
      Assert.assertTrue(this.isExceptionExpected);
    }
  }
}
