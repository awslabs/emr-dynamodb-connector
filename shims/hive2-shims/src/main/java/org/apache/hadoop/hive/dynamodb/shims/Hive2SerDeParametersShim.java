package org.apache.hadoop.hive.dynamodb.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Properties;

class Hive2SerDeParametersShim implements SerDeParametersShim {

  private final LazySerDeParameters realSerDeParameters;

  Hive2SerDeParametersShim(Configuration configuration, Properties properties, String serDeName)
      throws SerDeException {
    this.realSerDeParameters = new LazySerDeParameters(configuration, properties, serDeName);
  }

  @Override
  public List<String> getColumnNames() {
    return this.realSerDeParameters.getColumnNames();
  }

  @Override
  public List<TypeInfo> getColumnTypes() {
    return this.realSerDeParameters.getColumnTypes();
  }

  @Override
  public byte[] getSeparators() {
    return this.realSerDeParameters.getSeparators();
  }
}
