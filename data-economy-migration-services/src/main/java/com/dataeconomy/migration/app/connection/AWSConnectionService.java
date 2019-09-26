package com.dataeconomy.migration.app.connection;

import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.BasicSessionCredentials;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.service.aws.AwsFederatedTempCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMUAwsAssumeRoleCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMUAwsAssumeRoleWithSAMLCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMULongTermAwsCredentialsService;
import com.dataeconomy.migration.app.util.Constants;
import com.dataeconomy.migration.app.util.DMUHelperService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AWSConnectionService {

	@Autowired
	private AwsFederatedTempCredentialsService awsFederatedTempCredentialsService;

	@Autowired
	private DMULongTermAwsCredentialsService awsLongTermAwsCredentialsService;

	@Autowired
	private DMUAwsAssumeRoleCredentialsService awsAssumeRoleCredentialsService;

	@Autowired
	private DMUAwsAssumeRoleWithSAMLCredentialsService awsAssumeRoleWithSAMLCredentialsService;

	private BasicSessionCredentials basicSessionCredentials;

	@Autowired
	private DMUHelperService dmuHelpserService;

	@PostConstruct
	public void populateAWSCredentials() {
		try {
			log.info(
					" called => AWSConnectionService ::  populateAWSCredentials creating AWS credentials and connectio details while server running");
			ConnectionDto connectionDto = ConnectionDto.builder().build();
			dmuHelpserService.populateDMUS3Properties(connectionDto);
			Optional<BasicSessionCredentials> basicSessionCredentialsOpt = retrieveAWSCredentials(connectionDto);
			if (basicSessionCredentialsOpt.isPresent()) {
				basicSessionCredentials = basicSessionCredentialsOpt.get();
			} else {
				log.info(" AWSConnectionService :: populateAWSCredentials :: Unable to retrieve AWS crdentials");
			}
		} catch (Exception exception) {
			log.error(" Exception occured at AWSConnectionService :: populateAWSCredentials ::  {} ",
					ExceptionUtils.getStackTrace(exception));
		}
	}

	private Optional<BasicSessionCredentials> retrieveAWSCredentials(ConnectionDto connectionDto)
			throws DataMigrationException {
		try {
			if (StringUtils.equalsIgnoreCase(Constants.DIRECT_LC, connectionDto.getConnectionType())) {
				return awsLongTermAwsCredentialsService.validateLongTermAWSCredentials(connectionDto);
			} else if (StringUtils.equalsIgnoreCase(Constants.DIRECT_SC, connectionDto.getConnectionType())) {
				if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME)) {
					return awsAssumeRoleCredentialsService.getAwsAssumeRoleRequestCredentials(connectionDto);
				} else if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME_SAML)) {
					return awsAssumeRoleWithSAMLCredentialsService
							.getAwsAssumeRoleRequestWithSAMLCredentials(connectionDto);
				} else if (StringUtils.equalsIgnoreCase(Constants.AWS_FEDERATED_USER,
						connectionDto.getScCrdntlAccessType())) {
					return awsFederatedTempCredentialsService.getFederatedCredentials(connectionDto);
				} else {
					throw new DataMigrationException("Invalid Connection Details for AWS Shortterm Validation ");
				}
			}
			return Optional.empty();
		} catch (DataMigrationException e) {
			throw new DataMigrationException("Invalid Connection Details for AWS Shortterm Validation ");
		}
	}

	public BasicSessionCredentials getBasicSessionCredentials() {
		return basicSessionCredentials;
	}

	public void setBasicSessionCredentials(BasicSessionCredentials basicSessionCredentials) {
		this.basicSessionCredentials = basicSessionCredentials;
	}

}
