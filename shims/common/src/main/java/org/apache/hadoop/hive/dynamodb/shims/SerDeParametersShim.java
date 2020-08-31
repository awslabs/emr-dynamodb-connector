package org.apache.hadoop.hive.dynamodb.shims;

import java.util.List;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public interface SerDeParametersShim {

  List<String> getColumnNames();

  List<TypeInfo> getColumnTypes();

  byte[] getSeparators();

}
