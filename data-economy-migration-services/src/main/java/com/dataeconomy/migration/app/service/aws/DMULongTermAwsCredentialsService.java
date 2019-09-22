package com.dataeconomy.migration.app.service.aws;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMULongTermAwsCredentialsService {

	public boolean validateLongTermAWSCredentials(ConnectionDto connectionDto) throws DataMigrationException {
		log.info(" DMULongTermCredentialsService :: validateLongTermAWSCredentials  ");
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(connectionDto.getAwsAccessIdLc(),
					connectionDto.getAwsSecretKeyLc());
			AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.withRegion(Constants.CLIENT_REGION).build();
			return true;
		} catch (Exception exception) {
			log.info(" Exception occured at DMULongTermCredentialsService :: validateLongTermAWSCredentials :: {}  ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Invalid Connection Details for Longterm AWS Validation");
		}
	}
}
