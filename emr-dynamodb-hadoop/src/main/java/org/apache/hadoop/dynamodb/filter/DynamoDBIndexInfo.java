package org.apache.hadoop.dynamodb.filter;

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.Projection;
import java.util.List;

public class DynamoDBIndexInfo {

  private String indexName;

  private List<KeySchemaElement> indexSchema;
  private Projection indexProjection;

  public DynamoDBIndexInfo(String indexName,
      List<KeySchemaElement> indexSchema,
      Projection indexProjection) {
    this.indexName = indexName;
    this.indexSchema = indexSchema;
    this.indexProjection = indexProjection;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public List<KeySchemaElement> getIndexSchema() {
    return indexSchema;
  }

  public void setIndexSchema(
      List<KeySchemaElement> indexSchema) {
    this.indexSchema = indexSchema;
  }

  public Projection getIndexProjection() {
    return indexProjection;
  }

  public void setIndexProjection(Projection indexProjection) {
    this.indexProjection = indexProjection;
  }
}
