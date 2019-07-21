package org.apache.hadoop.dynamodb.provider;

import com.amazonaws.auth.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.constants.DynamoDBCredentialType;

import static org.apache.hadoop.dynamodb.constants.DynamoDBConstants.*;

public class AWSSTSAssumeRoleSessionCredentialsProvider
        implements AWSSessionCredentialsProvider, Configurable {
    private Configuration configuration;
    private STSAssumeRoleSessionCredentialsProvider delegate;

    public AWSSTSAssumeRoleSessionCredentialsProvider(Configuration configuration) {
        setConf(configuration);
    }

    public AWSSessionCredentials getCredentials() {
        return delegate.getCredentials();
    }

    public void refresh() {
        delegate.refresh();
    }


    public STSAssumeRoleSessionCredentialsProvider getDelegate() {
        return delegate;
    }

    @Override
    public void setConf(Configuration configuration) {
        AWSSecurityTokenService sts = null;
        this.configuration = configuration;
        String roleArn = configuration.get(ROLE_ARN_CONF);
        String sessionName = configuration.get(SESSION_NAME_CONF);
        String accessKey = configuration.get(DEFAULT_ACCESS_KEY_CONF);
        String secretKey = configuration.get(DEFAULT_SECRET_KEY_CONF);
        String dynamoDBaccessKey = configuration.get(DEFAULT_ACCESS_KEY_CONF);
        String dynamoDBSecretKey = configuration.get(DEFAULT_SECRET_KEY_CONF);
        String credentialType = configuration.get(DYNAMODB_CREDENTIAL_TYPE);

        if (roleArn == null || roleArn.isEmpty() || sessionName == null || sessionName.isEmpty() || credentialType == null || credentialType.isEmpty()) {
            throw new IllegalStateException("Please set " + ROLE_ARN_CONF + " and "
                    + SESSION_NAME_CONF + " and " + DYNAMODB_CREDENTIAL_TYPE + " before use.");
        }


        switch (credentialType) {
            case DynamoDBCredentialType.DEFAULT:
                sts = getStsTypeforDefault(accessKey, secretKey);
                break;

            case DynamoDBCredentialType.DYNAMODB_CREDENTIALS:
                sts = getStsTypeforDynamoDB(dynamoDBaccessKey, dynamoDBSecretKey);
                break;

            case DynamoDBCredentialType.INSTANCE_PROFILE:
                sts = AWSSecurityTokenServiceClientBuilder.standard()
                        .withCredentials(new InstanceProfileCredentialsProvider(true)).build();
                break;

        }
        this.delegate = new STSAssumeRoleSessionCredentialsProvider.Builder(
                roleArn, sessionName).withStsClient(sts).build();
    }

    private AWSSecurityTokenService getStsTypeforDefault(String accessKey, String secretKey) {
        if (Strings.isNullOrEmpty(accessKey) || Strings.isNullOrEmpty(secretKey)) {
            throw new IllegalStateException("Please set " + DEFAULT_ACCESS_KEY_CONF + " and "
                    + DEFAULT_SECRET_KEY_CONF + " before use.");
        }

        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider credentialsProvider = new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return credentials;
            }

            @Override
            public void refresh() {
            }
        };
        return AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(credentialsProvider).build();
    }

    private AWSSecurityTokenService getStsTypeforDynamoDB(String dynamoDBAccessKey, String dynamoDBSecretKey) {
        if (Strings.isNullOrEmpty(dynamoDBAccessKey) || Strings.isNullOrEmpty(dynamoDBSecretKey)) {
            throw new IllegalStateException("Please set " + DEFAULT_ACCESS_KEY_CONF + " and "
                    + DEFAULT_SECRET_KEY_CONF + " before use.");
        }

        final AWSCredentials credentials = new BasicAWSCredentials(dynamoDBAccessKey, dynamoDBSecretKey);
        AWSCredentialsProvider credentialsProvider = new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return credentials;
            }

            @Override
            public void refresh() {
            }
        };
        return AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(credentialsProvider).build();
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }


}


