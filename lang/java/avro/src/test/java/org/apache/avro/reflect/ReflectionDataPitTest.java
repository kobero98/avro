package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class ReflectionDataPitTest {
  class IntGenericArray implements GenericArrayType {

    @Override
    public Type getGenericComponentType() {
      return Integer.TYPE;
    }
  }

  @Test
  public void test() {

    Type type = new IntGenericArray();
    Schema item = Schema.create(Schema.Type.INT);
    Schema resultExpceted = Schema.createArray(item);
    Map<String, Schema> names = new HashMap<>();
    Schema schema1 = Schema.create(Schema.Type.STRING);
    Schema schema2 = Schema.create(Schema.Type.NULL);
    names.put("persona", schema1);
    names.put("Schema2", schema2);
    ReflectData reflectiondata = new ReflectData();
    Schema actual = reflectiondata.createSchema(type, names);
    Assert.assertEquals(resultExpceted, actual);

  }

}
