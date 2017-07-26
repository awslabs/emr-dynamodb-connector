package org.apache.hadoop.dynamodb.key;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Created by srramas on 7/26/17.
 */
public class DynamoDBBooleanKey  extends AbstractDynamoDBKey {

    public DynamoDBBooleanKey(String key) {
        super(key);
    }
    @Override
    public int compareValue(AttributeValue attribute) {
        return new Boolean(key).compareTo(attribute.getBOOL());
    }
}
