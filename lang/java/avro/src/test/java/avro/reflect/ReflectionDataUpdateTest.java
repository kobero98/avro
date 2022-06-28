package avro.reflect;

import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroDoc;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Union;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.*;

@RunWith(Parameterized.class)
public class ReflectionDataUpdateTest {

  private Type type;
  private Map<String, Schema> names;
  private Schema resultExpceted;
  private boolean eceptionExpeted;
  private int sizeExcpected;

  public enum ParamType {
    NULL, VOID, BOOLEAN, INT, BYTE, SHORT, CHAR, LONG, STRING, INT_ARR, CLASSTEST, GENERICARRAYTYPE_BYTE,
    PARAMETRIZEDCLASS, UNION
  }

  public enum NamesType {
    NULL, EMPTY, VAL_VAL, NULL_VAL, PRESENT_NULL, NOT_PRESENT_NULL, PRESENT_VAL, NOT_PRESENT_VAL
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { ParamType.INT_ARR, NamesType.NULL }, // 0
        { ParamType.INT_ARR, NamesType.EMPTY }, // 1
        { ParamType.INT_ARR, NamesType.VAL_VAL }, // 2
        { ParamType.INT_ARR, NamesType.NULL_VAL }, // 3
        { ParamType.INT_ARR, NamesType.NOT_PRESENT_NULL }, // 4
        { ParamType.INT_ARR, NamesType.NOT_PRESENT_VAL }, // 5
        { ParamType.INT_ARR, NamesType.PRESENT_NULL }, // 6
        { ParamType.INT_ARR, NamesType.PRESENT_VAL }, // 7

        { ParamType.UNION, NamesType.NULL }, // 8
        { ParamType.UNION, NamesType.EMPTY }, // 9
        { ParamType.UNION, NamesType.VAL_VAL }, // 10
        { ParamType.UNION, NamesType.NULL_VAL }, // 11
        { ParamType.UNION, NamesType.NOT_PRESENT_NULL }, // 12
        { ParamType.UNION, NamesType.NOT_PRESENT_VAL }, // 13
        { ParamType.UNION, NamesType.PRESENT_NULL }, // 14
        { ParamType.UNION, NamesType.PRESENT_VAL }, // 15

        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.NULL }, // 16
        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.EMPTY }, // 17
        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.VAL_VAL }, // 18
        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.NULL_VAL }, // 19
        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.NOT_PRESENT_NULL }, // 20
        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.NOT_PRESENT_VAL }, // 21
        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.PRESENT_NULL }, // 22
        { ParamType.GENERICARRAYTYPE_BYTE, NamesType.PRESENT_VAL }, // 23

        { ParamType.CLASSTEST, NamesType.NULL }, // 24
        { ParamType.CLASSTEST, NamesType.EMPTY }, // 25
        { ParamType.CLASSTEST, NamesType.VAL_VAL }, // 26
        { ParamType.CLASSTEST, NamesType.NULL_VAL }, // 27
        { ParamType.CLASSTEST, NamesType.NOT_PRESENT_NULL }, // 28
        { ParamType.CLASSTEST, NamesType.NOT_PRESENT_VAL }, // 29
        { ParamType.CLASSTEST, NamesType.PRESENT_NULL }, // 30
        { ParamType.CLASSTEST, NamesType.PRESENT_VAL }, // 31

        { ParamType.PARAMETRIZEDCLASS, NamesType.NULL }, // 32
        { ParamType.PARAMETRIZEDCLASS, NamesType.EMPTY }, // 33
        { ParamType.PARAMETRIZEDCLASS, NamesType.VAL_VAL }, // 34
        { ParamType.PARAMETRIZEDCLASS, NamesType.NULL_VAL }, // 35
        { ParamType.PARAMETRIZEDCLASS, NamesType.NOT_PRESENT_NULL }, // 36
        { ParamType.PARAMETRIZEDCLASS, NamesType.NOT_PRESENT_VAL }, // 37
        { ParamType.PARAMETRIZEDCLASS, NamesType.PRESENT_NULL }, // 38
        { ParamType.PARAMETRIZEDCLASS, NamesType.PRESENT_VAL },// 39

    });

  }

  public static class testPersona {
    public int x;
    public Byte[] array;
    public Collection<String>[] stringa;

  }

  @AvroDoc("ciao")
  public static class persona {
    public int x;
    public int y;
  }

  public void configure(ParamType type, NamesType names) {
    this.eceptionExpeted = false;
    this.sizeExcpected = 1;
    switch (type) {
    case UNION:
      this.type = Union.class;
      this.resultExpceted = Schema.createRecord("Union", null, "org.apache.avro.reflect", false, new ArrayList<>());
      break;
    case INT_ARR:
      int v[] = new int[5];
      this.type = v.getClass();
      Schema element = Schema.create(Schema.Type.INT);
      this.resultExpceted = Schema.createArray(element);
      this.resultExpceted.addProp("java-class", "[I");
      break;
    case CLASSTEST:
      this.type = persona.class;
      Schema app = Schema.create(Schema.Type.INT);
      Schema.Field field = new Schema.Field("x", app);
      Schema.Field field1 = new Schema.Field("y", app);
      List<Schema.Field> fields = new ArrayList<>();
      fields.add(field);
      fields.add(field1);
      this.resultExpceted = Schema.createRecord("persona", "ciao", "org.apache.avro.reflect.ReflectionDataUpdateTest",
          false, fields);
      this.eceptionExpeted = false;
      break;
    case GENERICARRAYTYPE_BYTE:
      class ByteGenericArray implements GenericArrayType {

        @Override
        public Type getGenericComponentType() {
          return Byte.TYPE;
        }
      }
      this.type = new ByteGenericArray();
      this.resultExpceted = Schema.create(Schema.Type.BYTES);
      this.eceptionExpeted = false;
      break;

    case PARAMETRIZEDCLASS:
      try {
        this.type = testPersona.class.getField("stringa").getGenericType();
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e + " test");
      }
      Schema arrayapp = Schema.createArray(Schema.create(Schema.Type.STRING));
      arrayapp.addProp("java-class", "java.util.Collection");
      this.resultExpceted = Schema.createArray(arrayapp);
      this.eceptionExpeted = false;
      break;
    }
    if (this.resultExpceted == null)
      return;
    switch (names) {
    case NULL:
      this.names = null;
      this.sizeExcpected = -1;
      if (type != ParamType.INT_ARR && type != ParamType.GENERICARRAYTYPE_BYTE && type != ParamType.PARAMETRIZEDCLASS)
        this.eceptionExpeted = true;
      break;
    case EMPTY:
      this.names = new HashMap<>();
      if (type != ParamType.INT_ARR && type != ParamType.GENERICARRAYTYPE_BYTE && type != ParamType.PARAMETRIZEDCLASS)
        this.sizeExcpected = 1;
      else
        this.sizeExcpected = 0;

      break;
    case VAL_VAL:
      this.names = new HashMap<>();
      Schema schema1 = Schema.create(Schema.Type.STRING);
      Schema schema2 = Schema.create(Schema.Type.NULL);
      this.names.put("persona", schema1);
      this.names.put("Schema2", schema2);
      if (type != ParamType.INT_ARR && type != ParamType.GENERICARRAYTYPE_BYTE && type != ParamType.PARAMETRIZEDCLASS)
        this.sizeExcpected = 3;
      else
        this.sizeExcpected = 2;
      break;
    case NULL_VAL:
      this.names = new HashMap<>();
      this.names.put(null, Schema.create(Schema.Type.INT));
      if (type != ParamType.INT_ARR && type != ParamType.GENERICARRAYTYPE_BYTE && type != ParamType.PARAMETRIZEDCLASS)
        this.sizeExcpected++;
      break;
    case PRESENT_VAL:
      String key = this.type.getTypeName();
      this.names = new HashMap<>();
      this.names.put(key, this.resultExpceted);
      break;
    case PRESENT_NULL:
      String key1 = this.type.getTypeName();
      this.names = new HashMap<>();
      this.names.put(key1, null);
      break;
    case NOT_PRESENT_VAL:
      String key2 = this.resultExpceted.getFullName() + "1";
      this.names = new HashMap<>();
      this.names.put(key2, Schema.create(Schema.Type.INT));
      if (type != ParamType.INT_ARR && type != ParamType.GENERICARRAYTYPE_BYTE && type != ParamType.PARAMETRIZEDCLASS)
        this.sizeExcpected++;
      break;
    case NOT_PRESENT_NULL:
      String key3 = this.resultExpceted.getFullName() + "1";
      this.names = new HashMap<>();
      this.names.put(key3, null);
      if (type != ParamType.INT_ARR && type != ParamType.GENERICARRAYTYPE_BYTE && type != ParamType.PARAMETRIZEDCLASS)
        this.sizeExcpected++;
      break;
    }
  }

  public ReflectionDataUpdateTest(ParamType type, NamesType name) {
    configure(type, name);
  }

  @Test
  public void testCreate() {
    try {
      ReflectData reflectiondata = new ReflectData();
      Schema actual = reflectiondata.createSchema(this.type, this.names);
      if (this.sizeExcpected != -1)
        Assert.assertEquals(this.names.toString(), this.sizeExcpected, this.names.size());
      Assert.assertEquals(this.resultExpceted, actual);
      Assert.assertFalse("mi aspettavo un eccezzione", this.eceptionExpeted);

    } catch (Exception e) {
      Assert.assertTrue(e.toString(), this.eceptionExpeted);
    }
  }
}
