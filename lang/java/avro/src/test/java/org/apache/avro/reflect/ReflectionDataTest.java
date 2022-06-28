package org.apache.avro.reflect;

import org.apache.avro.Schema;
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
public class ReflectionDataTest {

  private Type type;
  private Map<String, Schema> names;
  private Schema resultExpceted;
  private int sizeExcpected;
  private boolean eceptionExpeted;

  public enum ParamType {
    NULL, VOID, BOOLEAN, INT, BYTE, SHORT, CHAR, LONG, STRING
  }

  public enum NamesType {
    NULL, EMPTY, VAL_VAL,
  }

  public ReflectionDataTest(ParamType type, NamesType names) {
    configure(type, names);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { ParamType.NULL, NamesType.NULL }, { ParamType.NULL, NamesType.EMPTY },
        { ParamType.NULL, NamesType.VAL_VAL },

        { ParamType.VOID, NamesType.NULL }, { ParamType.VOID, NamesType.EMPTY }, { ParamType.VOID, NamesType.VAL_VAL },

        { ParamType.INT, NamesType.NULL }, { ParamType.INT, NamesType.EMPTY }, { ParamType.INT, NamesType.VAL_VAL },

        { ParamType.SHORT, NamesType.NULL }, { ParamType.SHORT, NamesType.EMPTY },
        { ParamType.SHORT, NamesType.VAL_VAL },

        { ParamType.LONG, NamesType.NULL }, { ParamType.LONG, NamesType.EMPTY }, { ParamType.LONG, NamesType.VAL_VAL },

        { ParamType.BOOLEAN, NamesType.NULL }, { ParamType.BOOLEAN, NamesType.EMPTY },
        { ParamType.BOOLEAN, NamesType.VAL_VAL },

        { ParamType.BYTE, NamesType.NULL }, { ParamType.BYTE, NamesType.EMPTY }, { ParamType.BYTE, NamesType.VAL_VAL },

        { ParamType.CHAR, NamesType.NULL }, { ParamType.CHAR, NamesType.EMPTY }, { ParamType.CHAR, NamesType.VAL_VAL },

        { ParamType.STRING, NamesType.NULL }, { ParamType.STRING, NamesType.EMPTY },
        { ParamType.STRING, NamesType.VAL_VAL }, });

  }

  public void configure(ParamType type, NamesType names) {
    this.eceptionExpeted = false;
    switch (names) {
    case NULL:
      this.names = null;
      this.sizeExcpected = -1;
      break;
    case EMPTY:
      this.names = new HashMap<>();
      this.sizeExcpected = 0;
      break;
    case VAL_VAL:
      this.names = new HashMap<>();
      Schema schema1 = Schema.create(Schema.Type.STRING);
      Schema schema2 = Schema.create(Schema.Type.NULL);
      this.names.put("persona", schema1);
      this.names.put("Schema2", schema2);
      this.sizeExcpected = 2;
      break;
    }
    switch (type) {
    case NULL:
      this.type = null;
      this.eceptionExpeted = true;
      break;
    case INT:
      this.type = Integer.TYPE;
      this.resultExpceted = Schema.create(Schema.Type.INT);
      break;
    case BYTE:
      this.type = Byte.TYPE;
      this.resultExpceted = Schema.create(Schema.Type.INT);
      this.resultExpceted.addProp("java-class", "java.lang.Byte");
      break;
    case BOOLEAN:
      this.type = Boolean.TYPE;
      this.resultExpceted = Schema.create(Schema.Type.BOOLEAN);
      break;
    case CHAR:
      this.type = Character.TYPE;
      this.resultExpceted = Schema.create(Schema.Type.INT);
      this.resultExpceted.addProp("java-class", "java.lang.Character");
      break;
    case VOID:
      this.type = Void.TYPE;
      this.resultExpceted = Schema.create(Schema.Type.NULL);
      break;
    case SHORT:
      this.type = Short.TYPE;
      this.resultExpceted = Schema.create(Schema.Type.INT);
      this.resultExpceted.addProp("java-class", "java.lang.Short");
      break;
    case LONG:
      this.type = Long.TYPE;
      this.resultExpceted = Schema.create(Schema.Type.LONG);
      break;
    case STRING:
      this.type = String.class;
      this.resultExpceted = Schema.create(Schema.Type.STRING);
      break;

    }

  }

  @Test
  public void testCreate() {
    try {
      ReflectData reflectiondata = new ReflectData();
      Schema actual = reflectiondata.createSchema(this.type, this.names);
      if (this.sizeExcpected != -1) {
        Assert.assertEquals(this.type.toString(), this.sizeExcpected, this.names.size());
      }
      Assert.assertEquals(this.type.toString(), this.resultExpceted, actual);
      Assert.assertFalse(this.eceptionExpeted);

    } catch (Exception e) {
      Assert.assertTrue(e.toString(), this.eceptionExpeted);
    }
  }
}
