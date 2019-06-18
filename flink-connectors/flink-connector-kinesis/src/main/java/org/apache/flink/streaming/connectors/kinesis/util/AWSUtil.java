/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.deser.DeserializerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Some utilities specific to Amazon Web Service.
 */
@Internal
public class AWSUtil {
	/** Used for formatting Flink-specific user agent string when creating Kinesis client. */
	private static final String USER_AGENT_FORMAT = "Apache Flink %s (%s) Kinesis Connector";

	/**
	 * Creates an AmazonKinesis client.
	 * @param configProps configuration properties containing the access key, secret key, and region
	 * @return a new AmazonKinesis client
	 */
	public static KinesisClient createKinesisClient(Properties configProps) {
		return createKinesisClient(configProps, ClientOverrideConfiguration.builder(), ApacheHttpClient.builder().build());
	}

	/**
	 * Creates an Amazon Kinesis Client.
	 * @param configProps configuration properties containing the access key, secret key, and region
	 * @param awsClientConfig preconfigured AWS SDK client configuration
	 * @return a new Amazon Kinesis Client
	 */
	public static KinesisClient createKinesisClient(Properties configProps, ClientOverrideConfiguration.Builder awsClientConfig, SdkHttpClient httpClient) {
		// set a Flink-specific user agent
		awsClientConfig.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, String.format(USER_AGENT_FORMAT,
			EnvironmentInformation.getVersion(),
			EnvironmentInformation.getRevisionInformation().commitId));

		//return builder.build();

		KinesisClientBuilder clientBuilder = KinesisClient.builder()
			.credentialsProvider(AWSUtil.getAwsCredentialsProvider(configProps))
			.overrideConfiguration(awsClientConfig.build())
			.httpClient(httpClient);

		if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
			clientBuilder.endpointOverride(URI.create(configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT)));
		} else {
			clientBuilder.region(Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION)));
		}

		return clientBuilder.build();
	}

	/**
	 * Return a {@link AwsCredentialsProvider} instance corresponding to the configuration properties.
	 *
	 * @param configProps the configuration properties
	 * @return The corresponding AWS Credentials Provider SDK v2.x instance
	 */
	public static AwsCredentialsProvider getAwsCredentialsProvider(final Properties configProps) {
		return getAwsCredentialsProvider(configProps, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
	}

	/**
	 * If the provider is ASSUME_ROLE, then the credentials for assuming this role are determined
	 * recursively.
	 *
	 * @param configProps the configuration properties
	 * @param configPrefix the prefix of the config properties for this credentials provider,
	 *                     e.g. aws.credentials.provider for the base credentials provider,
	 *                     aws.credentials.provider.role.provider for the credentials provider
	 *                     for assuming a role, and so on.
	 */
	private static AwsCredentialsProvider getAwsCredentialsProvider(final Properties configProps, final String configPrefix) {
		CredentialProvider credentialProviderType;
		if (!configProps.containsKey(configPrefix)) {
			if (configProps.containsKey(AWSConfigConstants.accessKeyId(configPrefix))
				&& configProps.containsKey(AWSConfigConstants.secretKey(configPrefix))) {
				// if the credential provider type is not specified, but the Access Key ID and Secret Key are given, it will default to BASIC
				credentialProviderType = CredentialProvider.BASIC;
			} else {
				// if the credential provider type is not specified, it will default to AUTO
				credentialProviderType = CredentialProvider.AUTO;
			}
		} else {
			credentialProviderType = CredentialProvider.valueOf(configProps.getProperty(configPrefix));
		}

		switch (credentialProviderType) {
			case ENV_VAR:
				return EnvironmentVariableCredentialsProvider.create();

			case SYS_PROP:
				return SystemPropertyCredentialsProvider.create();

			case PROFILE:
				String profileName = configProps.getProperty(
						AWSConfigConstants.profileName(configPrefix), null);
				String profileConfigPath = configProps.getProperty(
						AWSConfigConstants.profilePath(configPrefix), null);
				return (profileConfigPath == null)
					? ProfileCredentialsProvider.builder().profileName(profileName).build()
					: ProfileCredentialsProvider.builder().profileName(profileName).profileFile(ProfileFile.builder().content(Paths.get(profileConfigPath)).build()).build();

			case BASIC:
				return new AwsCredentialsProvider() {
					@Override
					public AwsCredentials resolveCredentials() {
						return AwsBasicCredentials.create(
							configProps.getProperty(AWSConfigConstants.accessKeyId(configPrefix)),
							configProps.getProperty(AWSConfigConstants.secretKey(configPrefix)));
					}
				};

			case ASSUME_ROLE:
				final StsClient stsClient = StsClient.builder()
					.credentialsProvider(getAwsCredentialsProvider(configProps, AWSConfigConstants.roleCredentialsProvider(configPrefix)))
					.region(Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION)))
					.build();

				final AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
					.roleArn(AWSConfigConstants.roleArn(configPrefix))
					.roleSessionName(AWSConfigConstants.roleSessionName(configPrefix))
					.externalId(configProps.getProperty(AWSConfigConstants.externalId(configPrefix)))
					.build();

				return StsAssumeRoleCredentialsProvider.builder()
					.asyncCredentialUpdateEnabled(true) //TODO: note that this is the previous default
					.stsClient(stsClient)
					.refreshRequest(assumeRoleRequest).build();

			default:
			case AUTO:
				return DefaultCredentialsProvider.create();
		}
	}

	/**
	 * Return a {@link AWSCredentialsProvider} instance corresponding to the configuration properties.
	 *
	 * @param configProps the configuration properties
	 * @return The corresponding AWS Credentials Provider instance
	 */
	public static AWSCredentialsProvider getCredentialsProvider(final Properties configProps) {
		return getCredentialsProvider(configProps, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
	}

	/**
	 * If the provider is ASSUME_ROLE, then the credentials for assuming this role are determined
	 * recursively.
	 *
	 * @param configProps the configuration properties
	 * @param configPrefix the prefix of the config properties for this credentials provider,
	 *                     e.g. aws.credentials.provider for the base credentials provider,
	 *                     aws.credentials.provider.role.provider for the credentials provider
	 *                     for assuming a role, and so on.
	 */
	private static AWSCredentialsProvider getCredentialsProvider(final Properties configProps, final String configPrefix) {
		CredentialProvider credentialProviderType;
		if (!configProps.containsKey(configPrefix)) {
			if (configProps.containsKey(AWSConfigConstants.accessKeyId(configPrefix))
				&& configProps.containsKey(AWSConfigConstants.secretKey(configPrefix))) {
				// if the credential provider type is not specified, but the Access Key ID and Secret Key are given, it will default to BASIC
				credentialProviderType = CredentialProvider.BASIC;
			} else {
				// if the credential provider type is not specified, it will default to AUTO
				credentialProviderType = CredentialProvider.AUTO;
			}
		} else {
			credentialProviderType = CredentialProvider.valueOf(configProps.getProperty(configPrefix));
		}

		switch (credentialProviderType) {
			case ENV_VAR:
				return new com.amazonaws.auth.EnvironmentVariableCredentialsProvider();

			case SYS_PROP:
				return new SystemPropertiesCredentialsProvider();

			case PROFILE:
				String profileName = configProps.getProperty(
					AWSConfigConstants.profileName(configPrefix), null);
				String profileConfigPath = configProps.getProperty(
					AWSConfigConstants.profilePath(configPrefix), null);
				return (profileConfigPath == null)
					? new com.amazonaws.auth.profile.ProfileCredentialsProvider(profileName)
					: new com.amazonaws.auth.profile.ProfileCredentialsProvider(profileConfigPath, profileName);

			case BASIC:
				return new AWSCredentialsProvider() {
					@Override
					public AWSCredentials getCredentials() {
						return new BasicAWSCredentials(
							configProps.getProperty(AWSConfigConstants.accessKeyId(configPrefix)),
							configProps.getProperty(AWSConfigConstants.secretKey(configPrefix)));
					}

					@Override
					public void refresh() {
						// do nothing
					}
				};

			case ASSUME_ROLE:
				final AWSSecurityTokenService baseCredentials = AWSSecurityTokenServiceClientBuilder.standard()
					.withCredentials(getCredentialsProvider(configProps, AWSConfigConstants.roleCredentialsProvider(configPrefix)))
					.withRegion(configProps.getProperty(AWSConfigConstants.AWS_REGION))
					.build();
				return new STSAssumeRoleSessionCredentialsProvider.Builder(
					configProps.getProperty(AWSConfigConstants.roleArn(configPrefix)),
					configProps.getProperty(AWSConfigConstants.roleSessionName(configPrefix)))
					.withExternalId(configProps.getProperty(AWSConfigConstants.externalId(configPrefix)))
					.withStsClient(baseCredentials)
					.build();

			default:
			case AUTO:
				return new DefaultAWSCredentialsProviderChain();
		}
	}

	/**
	 * Checks whether or not a region ID is valid.
	 *
	 *
	 * @param region The AWS region ID to check
	 * @return true if the supplied region ID is valid, false otherwise
	 */
	public static boolean isValidRegion(String region) {
		// TODO: SDK 2.x allows for specifying regions that did not exist at the time the SDK was released. This particular check is preventing
		// new regions from being set.
		try {
			if (!Region.regions().stream().anyMatch(r -> r.id().equals(region.toLowerCase()))) {
				throw new IllegalArgumentException("Supplied AWS region is not defined: " + region.toLowerCase());
			}
		} catch (IllegalArgumentException e) {
			return false;
		}
		return true;
	}

	/**
	 * The prefix used for properties that should be applied to {@link ClientOverrideConfiguration}.
	 */
	public static final String AWS_CLIENT_CONFIG_PREFIX = "aws.clientconfig.";

	/**
	 * Set all prefixed properties on {@link ClientOverrideConfiguration}.
	 * @param config
	 * @param configProps
	 */
	public static void setAwsClientOverrideConfigurationProperties(ClientOverrideConfiguration.Builder config, Properties configProps) {
		Map<String, Object> awsConfigProperties = new HashMap<>();
		for (Map.Entry<Object, Object> entry : configProps.entrySet()) {
			String key = (String) entry.getKey();
			if (key.startsWith(AWS_CLIENT_CONFIG_PREFIX)) {
				awsConfigProperties.put(key.substring(AWS_CLIENT_CONFIG_PREFIX.length()), entry.getValue());
			}
		}
		// TODO: figure out a good way to populate the builder.
	}

	/**
	 * Set all prefixed properties on {@link SdkHttpClient}.
	 * @param sdkHttpClient
	 * @param configProps
	 */
	public static void setSdkHttpClientConfigurationProperties(SdkHttpClient.Builder sdkHttpClient, Properties configProps) {
		Map<String, Object> awsConfigProperties = new HashMap<>();
		for (Map.Entry<Object, Object> entry : configProps.entrySet()) {
			String key = (String) entry.getKey();
			if (key.startsWith(AWS_CLIENT_CONFIG_PREFIX)) {
				awsConfigProperties.put(key.substring(AWS_CLIENT_CONFIG_PREFIX.length()), entry.getValue());
			}
		}
		// TODO: figure out a good way to populate the builder.
	}


	/**
	 * Set all prefixed properties on {@link ClientConfiguration}.
	 * @param config
	 * @param configProps
	 */
	public static void setAwsClientConfigProperties(ClientConfiguration config, Properties configProps) {
		Map<String, Object> awsConfigProperties = new HashMap<>();
		for (Map.Entry<Object, Object> entry : configProps.entrySet()) {
			String key = (String) entry.getKey();
			if (key.startsWith(AWS_CLIENT_CONFIG_PREFIX)) {
				awsConfigProperties.put(key.substring(AWS_CLIENT_CONFIG_PREFIX.length()), entry.getValue());
			}
		}
		// Jackson does not like the following properties
		String[] ignorableProperties = {"secureRandom"};
		BeanDeserializerModifier modifier = new BeanDeserializerModifierForIgnorables(
			ClientConfiguration.class, ignorableProperties);
		DeserializerFactory factory = BeanDeserializerFactory.instance.withDeserializerModifier(
			modifier);
		ObjectMapper mapper = new ObjectMapper(null, null,
			new DefaultDeserializationContext.Impl(factory));

		JsonNode propTree = mapper.convertValue(awsConfigProperties, JsonNode.class);
		try {
			mapper.readerForUpdating(config).readValue(propTree);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
}
