package org.apache.hadoop.hive.dynamodb.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.dynamodb.key.DynamoDBKey;
import org.apache.hadoop.dynamodb.type.DynamoDBBooleanType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Created by srramas on 7/26/17.
 */
public class HiveDynamoDBBooleanType extends DynamoDBBooleanType implements HiveDynamoDBType {

    private final DynamoDBDataParser parser = new DynamoDBDataParser();

    @Override
    public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
        Boolean value = parser.getBoolean(data, objectInspector);
        return new AttributeValue().withBOOL(value);
    }

    @Override
    public Object getHiveData(AttributeValue data, String hiveType) {
        if (data == null) {
            return null;
        }
        return data.getBOOL();
    }
}
