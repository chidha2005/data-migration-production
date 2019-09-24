package com.dataeconomy.migration.app.connection;

import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.BasicSessionCredentials;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.mysql.entity.DMUS3;
import com.dataeconomy.migration.app.mysql.repository.DMUS3Repository;
import com.dataeconomy.migration.app.service.ConnectionService;
import com.dataeconomy.migration.app.service.aws.AwsFederatedTempCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMUAwsAssumeRoleCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMUAwsAssumeRoleWithSAMLCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMULongTermAwsCredentialsService;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AWSConnectionService {

	@Autowired
	private DMUS3Repository dmuS3Repository;

	@Autowired
	private ConnectionService connectionService;

	@Autowired
	private AwsFederatedTempCredentialsService awsFederatedTempCredentialsService;

	@Autowired
	private DMULongTermAwsCredentialsService awsLongTermAwsCredentialsService;

	@Autowired
	private DMUAwsAssumeRoleCredentialsService awsAssumeRoleCredentialsService;

	@Autowired
	private DMUAwsAssumeRoleWithSAMLCredentialsService awsAssumeRoleWithSAMLCredentialsService;

	private BasicSessionCredentials basicSessionCredentials;

	@PostConstruct
	public void populateAWSCredentials() {
		try {
			log.info(
					" called => AWSConnectionService ::  populateAWSCredentials creating AWS credentials and connectio details while server running");
			Optional<DMUS3> dmuS3 = dmuS3Repository.findById(1L);
			if (dmuS3.isPresent()) {
				ConnectionDto connectionDto = ConnectionDto.builder().build();
				populateDMUS3Properties(connectionDto, dmuS3);
				connectionService.validateConnection(connectionDto);
				Optional<BasicSessionCredentials> basicSessionCredentialsOpt = retrieveAWSCredentials(connectionDto);
				if (basicSessionCredentialsOpt.isPresent()) {
					basicSessionCredentials = basicSessionCredentialsOpt.get();
				} else {
					log.info(" AWSConnectionService :: populateAWSCredentials :: Unable to retrieve AWS crdentials");
				}
			}
		} catch (Exception exception) {

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

	private void populateDMUS3Properties(ConnectionDto connectionDto, Optional<DMUS3> dmuS3) {
		if (dmuS3.isPresent()) {
			DMUS3 dmuS3Obj = dmuS3.get();
			connectionDto.setCredentialStrgType(dmuS3Obj.getCredentialStrgType());

			connectionDto.setAwsAccessIdLc(dmuS3Obj.getAwsAccessIdLc());
			connectionDto.setAwsSecretKeyLc(dmuS3Obj.getAwsSecretKeyLc());
			connectionDto.setAwsAccessIdSc(dmuS3Obj.getAwsAccessIdSc());
			connectionDto.setScCrdntlAccessType(dmuS3Obj.getScCrdntlAccessType());
			connectionDto.setAwsSecretKeySc(dmuS3Obj.getAwsSecretKeySc());
			connectionDto.setAwsAccessIdSc(dmuS3Obj.getAwsAccessIdLc());
			connectionDto.setRoleArn(dmuS3Obj.getRoleArn());
			connectionDto.setPrincipalArn(dmuS3Obj.getPrincipalArn());
			connectionDto.setSamlProviderArn(dmuS3Obj.getSamlProviderArn());
			connectionDto.setRoleSesnName(dmuS3Obj.getRoleSesnName());
			connectionDto.setPolicyArnMembers(dmuS3Obj.getPolicyArnMembers());
			connectionDto.setExternalId(dmuS3Obj.getExternalId());
			connectionDto.setDuration(dmuS3Obj.getDuration() != null ? Math.toIntExact(dmuS3Obj.getDuration()) : 0);
			connectionDto.setLdapUserName(dmuS3Obj.getLdapUserName());
			connectionDto.setLdapUserPassw(dmuS3Obj.getLdapUserPassw());
			connectionDto.setScCrdntlAccessType(dmuS3Obj.getScCrdntlAccessType());
		}
	}

	public BasicSessionCredentials getBasicSessionCredentials() {
		return basicSessionCredentials;
	}

	public void setBasicSessionCredentials(BasicSessionCredentials basicSessionCredentials) {
		this.basicSessionCredentials = basicSessionCredentials;
	}

}
