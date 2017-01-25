package org.apache.hadoop.dynamodb.util;

import com.amazonaws.util.Base64;

import java.io.*;

/**
 * Created by huy on 2017. 1. 17..
 */
public class SerializeUtil {
  /** Read the object from Base64 string. */
  public static Object fromString(String s ) throws IOException,
      ClassNotFoundException {
    byte [] data = Base64.decode( s );
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    Object o  = ois.readObject();
    ois.close();
    return o;
  }

  /** Write the object to a Base64 string. */
  public static String toString(Serializable o ) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream( baos );
    oos.writeObject( o );
    oos.close();
    return Base64.encodeAsString(baos.toByteArray());
  }
}
