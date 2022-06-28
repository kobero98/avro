package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.FixedSize;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class ReflectionDataBaduaTest {
  public Type type;
  public Map<String, Schema> names;
  public Schema resultExpected;
  public int sizeExpected;
  public boolean exceptionExpeted;

  @Union({ Integer.class, String.class })
  public class UnionTestClass {
  }

  @FixedSize(5)
  public abstract class FixedTestClass implements GenericFixed {
  }

  public enum ParamType {
    UnionAnnotation, Fixed,
  }

  public enum NamesType {
    NULL, EMPTY, VAL_VAL, NULL_VAL, PRESENT_NULL, NOT_PRESENT_NULL, PRESENT_VAL, NOT_PRESENT_VAL
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { ParamType.UnionAnnotation, NamesType.EMPTY }, // 0
        { ParamType.UnionAnnotation, NamesType.NULL }, // 1
        { ParamType.UnionAnnotation, NamesType.VAL_VAL }, // 2
        { ParamType.UnionAnnotation, NamesType.PRESENT_NULL }, // 3
        { ParamType.UnionAnnotation, NamesType.PRESENT_VAL }, // 4
        { ParamType.UnionAnnotation, NamesType.NOT_PRESENT_NULL }, // 5
        { ParamType.UnionAnnotation, NamesType.NOT_PRESENT_VAL }, // 6
        { ParamType.UnionAnnotation, NamesType.NULL_VAL }, // 7

        { ParamType.Fixed, NamesType.NULL }, // 8
        { ParamType.Fixed, NamesType.VAL_VAL }, // 9
        { ParamType.Fixed, NamesType.PRESENT_NULL }, // 10
        { ParamType.Fixed, NamesType.PRESENT_VAL }, // 11
        { ParamType.Fixed, NamesType.NOT_PRESENT_NULL }, // 12
        { ParamType.Fixed, NamesType.NOT_PRESENT_VAL }, // 13
        { ParamType.Fixed, NamesType.NULL_VAL }, // 14
    });

  }

  public ReflectionDataBaduaTest(ParamType type, NamesType names) {
    configure(type, names);
  }

  public void configure(ParamType type, NamesType names) {
    this.exceptionExpeted = false;
    this.sizeExpected = 0;
    switch (type) {
    case UnionAnnotation:
      this.type = UnionTestClass.class;
      Schema string = Schema.create(Schema.Type.STRING);
      Schema inter = Schema.create(Schema.Type.INT);
      this.resultExpected = Schema.createUnion(inter, string);
      switch (names) {
      case NULL:
        this.names = null;
        this.sizeExpected = -1;
        this.exceptionExpeted = true;
        return;
      case EMPTY:
        this.names = new HashMap<>();
        break;
      case VAL_VAL:
        this.names = new HashMap<>();
        Schema schema1 = Schema.create(Schema.Type.STRING);
        Schema schema2 = Schema.create(Schema.Type.NULL);
        this.names.put("persona", schema1);
        this.names.put("Schema2", schema2);
        this.sizeExpected = this.sizeExpected + 2;
        break;
      case NULL_VAL:
        this.names = new HashMap<>();
        this.names.put(null, Schema.create(Schema.Type.INT));
        this.sizeExpected++;
        break;
      case PRESENT_VAL:
        String key = this.type.getTypeName();
        this.names = new HashMap<>();
        this.names.put(key, this.resultExpected);
        this.sizeExpected++;
        break;
      case PRESENT_NULL:
        String key1 = this.type.getTypeName();
        this.names = new HashMap<>();
        this.names.put(key1, null);
        this.sizeExpected++;
        break;
      case NOT_PRESENT_VAL:
        String key2 = this.resultExpected.getFullName() + "1";
        this.names = new HashMap<>();
        this.names.put(key2, Schema.create(Schema.Type.INT));
        this.sizeExpected++;
        break;
      case NOT_PRESENT_NULL:
        String key3 = this.resultExpected.getFullName() + "1";
        this.names = new HashMap<>();
        this.names.put(key3, null);
        this.sizeExpected++;
        break;
      }
      break;
    case Fixed:
      this.type = FixedTestClass.class;
      this.resultExpected = Schema.createFixed("FixedTestClass", null,
          "org.apache.avro.reflect.ReflectionDataBaduaTest", 5);
      this.sizeExpected = 1;
      this.exceptionExpeted = false;
      switch (names) {
      case NULL:
        this.names = null;
        this.sizeExpected = -1;
        this.exceptionExpeted = true;
        return;
      case EMPTY:
        this.names = new HashMap<>();
        break;
      case VAL_VAL:
        this.names = new HashMap<>();
        Schema schema1 = Schema.create(Schema.Type.STRING);
        Schema schema2 = Schema.create(Schema.Type.NULL);
        this.names.put("persona", schema1);
        this.names.put("Schema2", schema2);
        this.sizeExpected = this.sizeExpected + 2;
        break;
      case NULL_VAL:
        this.names = new HashMap<>();
        this.names.put(null, Schema.create(Schema.Type.INT));
        this.sizeExpected = 2;
        break;
      case PRESENT_VAL:
        String key = this.type.getTypeName();
        this.names = new HashMap<>();
        this.names.put(key, this.resultExpected);
        this.sizeExpected = 1;
        break;
      case PRESENT_NULL:
        String key1 = this.type.getTypeName();
        this.names = new HashMap<>();
        this.names.put(key1, null);
        this.sizeExpected = 1;
        break;
      case NOT_PRESENT_VAL:
        String key2 = this.resultExpected.getFullName() + "1";
        this.names = new HashMap<>();
        this.names.put(key2, Schema.create(Schema.Type.INT));
        this.sizeExpected = 2;
        break;
      case NOT_PRESENT_NULL:
        String key3 = this.resultExpected.getFullName() + "1";
        this.names = new HashMap<>();
        this.names.put(key3, null);
        this.sizeExpected = 2;
        break;
      }
      break;
    }
  }

  @Test
  public void testCreate() {
    try {
      ReflectData reflectiondata = new ReflectData();
      Schema actual = reflectiondata.createSchema(this.type, this.names);
      if (this.sizeExpected != -1) {
        Assert.assertEquals(this.type.toString(), this.sizeExpected, this.names.size());
      }
      Assert.assertEquals(this.type.toString(), this.resultExpected, actual);
      Assert.assertFalse(this.exceptionExpeted);

    } catch (Exception e) {
      Assert.assertTrue(e.toString(), this.exceptionExpeted);
    }
  }
}
