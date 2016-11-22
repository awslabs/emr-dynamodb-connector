package org.apache.hadoop.hive.dynamodb.shims;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public interface SerDeParametersShim {

  List<String> getColumnNames();

  List<TypeInfo> getColumnTypes();

  byte[] getSeparators();

}
