package org.apache.hadoop.dynamodb.provider;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBClientTest;
import org.apache.hadoop.dynamodb.constants.DynamoDBConstants;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.dynamodb.constants.DynamoDBConstants.*;

public class AWSSTSAssumeRoleSessionCredentialsProviderTest {

    @Test
    public void testCustomCredentialsProvider() {
        final String ROLE = "test_role";
        final String SESSION_NAME = "test_session";
        final String DEFAULT_SECRET_KEY = "test_secret";
        final String DEFAULT_ACCESS_KEY = "test_access";
        final String DYNAMO_ACCESS_KEY = "dynamo_test_access";
        final String DYNAMO_SECRET_KEY = "dynamo_test_secret";
        final String CREDENTIAL_TYPE = "test_credential";

        Configuration conf = new Configuration();

        conf.set("aws.assumed.role.arn",ROLE);
        conf.set("aws.assumed.role.session.name",SESSION_NAME);
        conf.set("dynamodb.awsAccessKeyId",DYNAMO_ACCESS_KEY);
        conf.set("dynamodb.awsSecretAccessKey",DYNAMO_SECRET_KEY);
        conf.set("fs.s3.awsSecretAccessKey",DEFAULT_SECRET_KEY);
        conf.set("fs.s3.awsAccessKeyId",DEFAULT_ACCESS_KEY);
        conf.set("dynamodb.credential.type",CREDENTIAL_TYPE);

        AWSSTSAssumeRoleSessionCredentialsProvider awsstsAssumeRoleSessionCredentialsProvider = new AWSSTSAssumeRoleSessionCredentialsProvider(conf);
        Assert.assertEquals(ROLE, awsstsAssumeRoleSessionCredentialsProvider.getConf().get(ROLE_ARN_CONF));
        Assert.assertEquals(SESSION_NAME,  awsstsAssumeRoleSessionCredentialsProvider.getConf().get(SESSION_NAME_CONF));
        Assert.assertEquals(DYNAMO_ACCESS_KEY,  awsstsAssumeRoleSessionCredentialsProvider.getConf().get(DYNAMODB_ACCESS_KEY_CONF));
        Assert.assertEquals(DYNAMO_SECRET_KEY,  awsstsAssumeRoleSessionCredentialsProvider.getConf().get(DYNAMODB_SECRET_KEY_CONF));
        Assert.assertEquals(DEFAULT_SECRET_KEY,  awsstsAssumeRoleSessionCredentialsProvider.getConf().get(DEFAULT_SECRET_KEY_CONF));
        Assert.assertEquals(DEFAULT_ACCESS_KEY,  awsstsAssumeRoleSessionCredentialsProvider.getConf().get(DEFAULT_ACCESS_KEY_CONF));
        Assert.assertEquals(CREDENTIAL_TYPE,  awsstsAssumeRoleSessionCredentialsProvider.getConf().get(DYNAMODB_CREDENTIAL_TYPE));
    }

}
