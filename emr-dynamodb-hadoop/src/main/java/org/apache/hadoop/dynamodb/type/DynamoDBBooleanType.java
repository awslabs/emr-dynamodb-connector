package org.apache.hadoop.dynamodb.type;

import org.apache.hadoop.dynamodb.key.DynamoDBBooleanKey;
import org.apache.hadoop.dynamodb.key.DynamoDBKey;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;


/**
 * Created by srramas on 7/26/17.
 */
public class DynamoDBBooleanType implements DynamoDBType {

    @Override
    public AttributeValue getAttributeValue(String... values) {
        return new AttributeValue().withBOOL(new Boolean(values[0]));
    }

    @Override
    public String getDynamoDBType() {
        return "BOOL";
    }

    @Override
    public DynamoDBKey getKey(String key) {
        return new DynamoDBBooleanKey(key);
    }
}
