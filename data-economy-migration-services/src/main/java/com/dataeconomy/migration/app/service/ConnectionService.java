package com.dataeconomy.migration.app.service;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dataeconomy.migration.app.connection.AWSConnectionService;
import com.dataeconomy.migration.app.connection.DMUConnectionValidationService;
import com.dataeconomy.migration.app.connection.HDFSConnectionService;
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
public class ConnectionService {

	@Value("${hs2.datasource.driver-class-name: com.cloudera.hive.jdbc41.HS2Driver}")
	public String hs2Driver;

	@Value("${hs2.validate.url}")
	public String hs2DriverUrl;

	@Autowired
	private AwsFederatedTempCredentialsService awsFederatedTempCredentialsService;

	@Autowired
	private DMULongTermAwsCredentialsService awsLongTermAwsCredentialsService;

	@Autowired
	private DMUAwsAssumeRoleCredentialsService awsAssumeRoleCredentialsService;

	@Autowired
	private DMUAwsAssumeRoleWithSAMLCredentialsService awsAssumeRoleWithSAMLCredentialsService;

	@Autowired
	private DMUConnectionValidationService dmuConnectionValidationService;

	@Autowired
	private AWSConnectionService awsConnectionService;

	@Autowired
	HDFSConnectionService hdfsConnectionService;

	@Autowired
	TGTOtherPropService tgtOtherPropService;

	@Autowired
	DMUHelperService dmuHelperService;

	@Transactional
	public boolean validateConnection(ConnectionDto connectionDto) throws DataMigrationException {
		try {
			log.info(" ConnectionService :: validateConnection :: connectionDto {}",
					ObjectUtils.toString(connectionDto));
			if (Constants.AWS_TO_S3.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				if (StringUtils.equalsIgnoreCase(Constants.DIRECT_LC, connectionDto.getConnectionType())) {
					awsLongTermAwsCredentialsService.validateLongTermAWSCredentials(connectionDto);
					return true;
				} else if (StringUtils.equalsIgnoreCase(Constants.DIRECT_SC, connectionDto.getConnectionType())) {
					if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME)) {
						awsAssumeRoleCredentialsService.getAwsAssumeRoleRequestCredentials(connectionDto);
						return true;
					} else if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(),
							Constants.ASSUME_SAML)) {
						awsAssumeRoleWithSAMLCredentialsService
								.getAwsAssumeRoleRequestWithSAMLCredentials(connectionDto);
						return true;
					} else if (StringUtils.equalsIgnoreCase(Constants.AWS_FEDERATED_USER,
							connectionDto.getScCrdntlAccessType())) {
						awsFederatedTempCredentialsService.getFederatedCredentials(connectionDto);
						return true;
					} else {
						throw new DataMigrationException("Invalid Connection Details for AWS Shortterm Validation ");
					}
				}
			} else if (Constants.HDFS.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				return dmuConnectionValidationService.validateConnectionDetails(connectionDto);
			} else {
				throw new DataMigrationException("Invalid Connection Details for AWS/HDFS save details ");
			}
			return false;
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionObject :: validateConnection {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException(exception.getMessage());
		}

	}

	@CacheEvict(value = "dataEconomyCache", allEntries = true)
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public boolean saveConnectionDetails(ConnectionDto connectionDto) throws DataMigrationException {
		try {
			if (Constants.AWS_TO_S3.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				dmuHelperService.saveDMUS3Properties(connectionDto);
				awsConnectionService.populateAWSCredentials();
			}
			if (Constants.HDFS.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				dmuHelperService.saveDMUHdfsEntityProperties(connectionDto);
				hdfsConnectionService.initDataSourceConfig();
			}
			if (Constants.TARGET_FILE_PROPS.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				dmuHelperService.saveTGTFormatProperties(connectionDto);
			}
			if (Constants.OTHER_PROPS.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				dmuHelperService.saveTGTOtherProperties(connectionDto);
			}

		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: saveConnectionDetails ::   {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Exception while saving connection properties ");
		}
		return true;
	}

	@Cacheable("dataEconomyCache")
	public ConnectionDto getConnectionDetails() throws DataMigrationException {
		ConnectionDto connectionDto = ConnectionDto.builder().build();
		try {
			dmuHelperService.populateDMUHdfsProperties(connectionDto);
			dmuHelperService.populateDMUS3Properties(connectionDto);
			dmuHelperService.populateDMUAuthenticationProperties(connectionDto);
			dmuHelperService.populateTGTFormatProperties(connectionDto);
			dmuHelperService.populateTGTOtherProperties(connectionDto);
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Exception occured while retrieving connection details ");
		}
		return connectionDto;

	}

}