package com.dataeconomy.migration.app.service.aws;

import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Service;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.DmuConnectionDTO;
import com.dataeconomy.migration.app.util.DmuConstants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DmuAwsAssumeRoleWithSAMLCredentialsService {

	public Optional<BasicSessionCredentials> getAwsAssumeRoleRequestWithSAMLCredentials(DmuConnectionDTO connectionDto)
			throws DataMigrationException {
		log.info(
				"called => DMUAwsAssumeRoleWithSAMLCredentialsService :: getAwsAssumeRoleRequestWithSAMLCredentials  ");
		try {
			AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withRoleArn(connectionDto.getRoleArn())
					.withDurationSeconds(connectionDto.getDuration())
					.withRoleSessionName(connectionDto.getRoleSesnName())
					.withPolicy(connectionDto.getInlineSesnPolicy());
			BasicAWSCredentials awsCredentials = new BasicAWSCredentials(connectionDto.getAwsAccessIdSc(),
					connectionDto.getAwsSecretKeySc());
			AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
					.withRegion(DmuConstants.CLIENT_REGION).build();
			final Credentials credentials = stsClient.assumeRole(assumeRoleRequest).getCredentials();
			BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(credentials.getAccessKeyId(),
					credentials.getSecretAccessKey(), credentials.getSessionToken());
			return Optional.ofNullable(basicSessionCredentials);
		} catch (SdkClientException e) {
			log.error(
					"exception => Exception occured at DMUAwsAssumeRoleWithSAMLCredentialsService :: getAwsAssumeRoleRequestWithSAMLCredentials {} ",
					ExceptionUtils.getStackTrace(e));
			throw new DataMigrationException("Invalid Connection Details for AWS SAML Validation");
		}
	}
}
