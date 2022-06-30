package org.apache.avro;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SchemaCompatibilityPitTest {

  private Schema reader;
  private Schema writer;
  private boolean result;
  private boolean exception;

  public enum ParamSchemaReader {
    NULL, STRING, ENUM, FIXED, RECORD
  }

  public enum ParamSchemaWriter {
    NULL, SAME_NAME_SAME_SCHEMA, SAME_NAME_DIFFERENT_SCHEMA, NOT_NAME_SAME_SCHEMA, NOT_NAME_NOT_SCHEMA
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { ParamSchemaReader.NULL, ParamSchemaWriter.NULL }, // 0
        { ParamSchemaReader.NULL, ParamSchemaWriter.SAME_NAME_SAME_SCHEMA }, // 1
        { ParamSchemaReader.NULL, ParamSchemaWriter.SAME_NAME_DIFFERENT_SCHEMA }, // 2
        { ParamSchemaReader.NULL, ParamSchemaWriter.NOT_NAME_NOT_SCHEMA }, // 3
        { ParamSchemaReader.NULL, ParamSchemaWriter.NOT_NAME_SAME_SCHEMA }, // 4

        { ParamSchemaReader.STRING, ParamSchemaWriter.NULL }, // 5
        { ParamSchemaReader.STRING, ParamSchemaWriter.SAME_NAME_SAME_SCHEMA }, // 6
        { ParamSchemaReader.STRING, ParamSchemaWriter.SAME_NAME_DIFFERENT_SCHEMA }, // 7
        { ParamSchemaReader.STRING, ParamSchemaWriter.NOT_NAME_NOT_SCHEMA }, // 8
        { ParamSchemaReader.STRING, ParamSchemaWriter.NOT_NAME_SAME_SCHEMA }, // 9

        { ParamSchemaReader.ENUM, ParamSchemaWriter.NULL }, // 10
        { ParamSchemaReader.ENUM, ParamSchemaWriter.SAME_NAME_SAME_SCHEMA }, // 11
        { ParamSchemaReader.ENUM, ParamSchemaWriter.SAME_NAME_DIFFERENT_SCHEMA }, // 12
        { ParamSchemaReader.ENUM, ParamSchemaWriter.NOT_NAME_NOT_SCHEMA }, // 13
        { ParamSchemaReader.ENUM, ParamSchemaWriter.NOT_NAME_SAME_SCHEMA }, // 14

        { ParamSchemaReader.FIXED, ParamSchemaWriter.NULL }, // 15
        { ParamSchemaReader.FIXED, ParamSchemaWriter.SAME_NAME_SAME_SCHEMA }, // 16
        { ParamSchemaReader.FIXED, ParamSchemaWriter.SAME_NAME_DIFFERENT_SCHEMA }, // 17
        { ParamSchemaReader.FIXED, ParamSchemaWriter.NOT_NAME_NOT_SCHEMA }, // 18
        { ParamSchemaReader.FIXED, ParamSchemaWriter.NOT_NAME_SAME_SCHEMA }, // 19

        { ParamSchemaReader.RECORD, ParamSchemaWriter.NULL }, // 20
        { ParamSchemaReader.RECORD, ParamSchemaWriter.SAME_NAME_SAME_SCHEMA }, // 21
        { ParamSchemaReader.RECORD, ParamSchemaWriter.SAME_NAME_DIFFERENT_SCHEMA }, // 22
        { ParamSchemaReader.RECORD, ParamSchemaWriter.NOT_NAME_NOT_SCHEMA }, // 23
        { ParamSchemaReader.RECORD, ParamSchemaWriter.NOT_NAME_SAME_SCHEMA },// 24

    });
  }

  public SchemaCompatibilityPitTest(ParamSchemaReader typeReader, ParamSchemaWriter typeWriter) {
    configure(typeReader, typeWriter);
  }

  public void configure(ParamSchemaReader typeReader, ParamSchemaWriter typeWriter) {
    this.exception = false;
    switch (typeReader) {
    case NULL:
      this.reader = null;
      this.exception = true;
      break;
    case STRING:
      this.reader = Schema.create(Schema.Type.STRING);
      this.exception = true;
      break;
    case RECORD:
      this.reader = Schema.createRecord("name", null, "", false);
      break;
    case FIXED:
      this.reader = Schema.createFixed("name", null, "", 10);
      break;
    case ENUM:
      this.reader = Schema.createEnum("name", null, "", new ArrayList<>());
      break;
    }
    switch (typeWriter) {
    case NULL:
      this.writer = null;
      this.exception = true;
      break;
    case SAME_NAME_SAME_SCHEMA:
      this.writer = null;
      this.result = true;
      if (typeReader == ParamSchemaReader.RECORD) {
        this.writer = Schema.createRecord("name", null, "", false);
      }
      if (typeReader == ParamSchemaReader.FIXED) {
        this.writer = Schema.createFixed("name", null, "", 10);
      }
      if (typeReader == ParamSchemaReader.ENUM) {
        this.writer = Schema.createEnum("name", null, "", new ArrayList<>());
      }
      break;
    case SAME_NAME_DIFFERENT_SCHEMA:
      this.writer = null;
      this.result = true;
      if (typeReader == ParamSchemaReader.RECORD) {
        this.writer = Schema.createEnum("name", null, "", new ArrayList<>());
      }
      if (typeReader == ParamSchemaReader.FIXED) {
        this.writer = Schema.createRecord("name", null, "", false);
      }
      if (typeReader == ParamSchemaReader.ENUM) {
        this.writer = Schema.createFixed("name", null, "", 10);
      }
      break;
    case NOT_NAME_SAME_SCHEMA:
      this.writer = null;
      this.result = false;
      if (typeReader == ParamSchemaReader.RECORD) {
        this.writer = Schema.createRecord("name1", null, "", false);
      }
      if (typeReader == ParamSchemaReader.FIXED) {
        this.writer = Schema.createFixed("name1", null, "", 10);
      }
      if (typeReader == ParamSchemaReader.ENUM) {
        this.writer = Schema.createEnum("name1", null, "", new ArrayList<>());
      }
      break;
    case NOT_NAME_NOT_SCHEMA:
      this.writer = null;
      this.result = false;
      if (typeReader == ParamSchemaReader.RECORD) {
        this.writer = Schema.createEnum("name1", null, "", new ArrayList<>());
      }
      if (typeReader == ParamSchemaReader.FIXED) {
        this.writer = Schema.createRecord("name1", null, "", false);
      }
      if (typeReader == ParamSchemaReader.ENUM) {
        this.writer = Schema.createFixed("name1", null, "", 10);
      }
      break;
    }
  }

  @Test
  public void testEqualsName() {
    try {
      boolean actual = SchemaCompatibility.schemaNameEquals(reader, writer);
      Assert.assertEquals(this.result, actual);
      Assert.assertFalse(this.exception);
    } catch (Exception e) {
      Assert.assertTrue(this.exception);
    }
  }
}
