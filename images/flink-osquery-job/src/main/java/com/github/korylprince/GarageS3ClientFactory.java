package com.github.korylprince;

import java.util.Map;

import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Custom AWS client factory that disables S3 chunked encoding.
 * Garage does not support STREAMING-AWS4-HMAC-SHA256-PAYLOAD signing.
 */
public class GarageS3ClientFactory implements AwsClientFactory {

    private AwsClientProperties awsClientProperties;
    private S3FileIOProperties s3FileIOProperties;
    private HttpClientProperties httpClientProperties;

    @Override
    public void initialize(Map<String, String> properties) {
        this.awsClientProperties = new AwsClientProperties(properties);
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
    }

    @Override
    public S3Client s3() {
        return S3Client.builder()
            .applyMutation(awsClientProperties::applyClientRegionConfiguration)
            .applyMutation(httpClientProperties::applyHttpClientConfigurations)
            .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
            .applyMutation(s3FileIOProperties::applyServiceConfigurations)
            .applyMutation(
                b -> s3FileIOProperties.applyCredentialConfigurations(awsClientProperties, b))
            .applyMutation(s3FileIOProperties::applySignerConfiguration)
            .applyMutation(s3FileIOProperties::applyUserAgentConfigurations)
            .applyMutation(s3FileIOProperties::applyRetryConfigurations)
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .chunkedEncodingEnabled(false)
                    .build())
            .build();
    }

    @Override
    public S3AsyncClient s3Async() {
        return S3AsyncClient.builder()
            .applyMutation(awsClientProperties::applyClientRegionConfiguration)
            .applyMutation(
                b -> s3FileIOProperties.applyCredentialConfigurations(awsClientProperties, b))
            .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
            .build();
    }

    @Override
    public GlueClient glue() {
        throw new UnsupportedOperationException("Glue not needed");
    }

    @Override
    public KmsClient kms() {
        throw new UnsupportedOperationException("KMS not needed");
    }

    @Override
    public DynamoDbClient dynamo() {
        throw new UnsupportedOperationException("DynamoDB not needed");
    }
}
