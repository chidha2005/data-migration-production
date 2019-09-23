package com.dataeconomy.migration.app.service;

import java.util.Optional;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.connection.DMUConnectionValidationService;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.model.TGTOtherPropDto;
import com.dataeconomy.migration.app.mysql.entity.DMUAuthentication;
import com.dataeconomy.migration.app.mysql.entity.DMUHdfs;
import com.dataeconomy.migration.app.mysql.entity.DMUS3;
import com.dataeconomy.migration.app.mysql.entity.TGTFormatProp;
import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.AuthenticationRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUS3Repository;
import com.dataeconomy.migration.app.mysql.repository.HDFSRepository;
import com.dataeconomy.migration.app.mysql.repository.TGTFormatPropRepository;
import com.dataeconomy.migration.app.mysql.repository.TGTOtherPropRepository;
import com.dataeconomy.migration.app.service.aws.AwsFederatedTempCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMUAwsAssumeRoleCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMUAwsAssumeRoleWithSAMLCredentialsService;
import com.dataeconomy.migration.app.service.aws.DMULongTermAwsCredentialsService;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ConnectionService {

	@Value("${hs2.datasource.driver-class-name: com.cloudera.hive.jdbc41.HS2Driver}")
	public String hs2Driver;

	@Value("${hs2.validate.url}")
	public String hs2DriverUrl;

	@Autowired
	private HDFSRepository hdfsRepository;

	@Autowired
	private DMUS3Repository dmuS3Repository;

	@Autowired
	private AuthenticationRepository authenticationRepository;

	@Autowired
	private TGTFormatPropRepository tgtFormatPropRepository;

	@Autowired
	private TGTOtherPropRepository tgtOtherPropRepository;

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

	public boolean validateConnection(ConnectionDto connectionDto) throws DataMigrationException {
		try {
			log.info(" ConnectionService :: validateConnection :: connectionDto {}",
					ObjectUtils.toString(connectionDto));

			if (StringUtils.equalsIgnoreCase(Constants.DIRECT_LC, connectionDto.getConnectionType())) {
				awsLongTermAwsCredentialsService.validateLongTermAWSCredentials(connectionDto);
				return true;
			} else if (StringUtils.equalsIgnoreCase(Constants.DIRECT_SC, connectionDto.getConnectionType())) {
				if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME)) {
					awsAssumeRoleCredentialsService.getAwsAssumeRoleRequestCredentials(connectionDto);
					return true;
				} else if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME_SAML)) {
					awsAssumeRoleWithSAMLCredentialsService.getAwsAssumeRoleRequestWithSAMLCredentials(connectionDto);
					return true;
				} else if (StringUtils.equalsIgnoreCase(Constants.AWS_FEDERATED_USER,
						connectionDto.getScCrdntlAccessType())) {
					awsFederatedTempCredentialsService.getFederatedCredentials(connectionDto);
					return true;
				} else {
					throw new DataMigrationException("Invalid Connection Details for AWS Shortterm Validation ");
				}
			} else {
				return dmuConnectionValidationService.validateConnectionDetails(connectionDto);
			}
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionObject :: validateConnection {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException(exception.getMessage());
		}
	}

	public boolean saveConnectionDetails(ConnectionDto connectionDto) {

		return true;
	}

	public ConnectionDto getConnectionDetails() {
		ConnectionDto connectionDto = ConnectionDto.builder().build();
		try {
			Optional<DMUHdfs> dmuHdfs = hdfsRepository.findById(1L);
			Optional<DMUS3> dmuS3 = dmuS3Repository.findById(1L);
			Optional<DMUAuthentication> dmuAuthentication = authenticationRepository.findById(1L);
			Optional<TGTFormatProp> tgtFormatProp = tgtFormatPropRepository.findById(1L);
			Optional<TGTOtherProp> tgtOtherProp = tgtOtherPropRepository.findById(1L);

			if (dmuHdfs.isPresent()) {
				DMUHdfs dmuHdfsObj = dmuHdfs.get();
				connectionDto.setHiveCnctnFlag(dmuHdfsObj.getHiveCnctnFlag());
				connectionDto.setImpalaCnctnFlag(dmuHdfsObj.getImpalaCnctnFlag());
				connectionDto.setSparkCnctnFlag(String.valueOf(dmuHdfsObj.getSparkCnctnFlag()));
			}

			if (dmuS3.isPresent()) {
				DMUS3 dmuS3Obj = dmuS3.get();
				connectionDto.setCredentialStrgType(dmuS3Obj.getCredentialStrgType());
				connectionDto.setAwsAccessIdLc(dmuS3Obj.getAwsAccessIdLc());
				connectionDto.setAwsSecretKeyLc(dmuS3Obj.getAwsSecretKeyLc());
				connectionDto.setScCrdntlAccessType(dmuS3Obj.getScCrdntlAccessType());
			}

			if (dmuAuthentication.isPresent()) {
				DMUAuthentication dmuAuthenticationObj = dmuAuthentication.get();
				connectionDto.setAuthenticationType(dmuAuthenticationObj.getAuthenticationType());
				connectionDto.setLdapCnctnFlag(dmuAuthenticationObj.getLdapCnctnFlag());
				connectionDto.setKerberosCnctnFlag(dmuAuthenticationObj.getKerberosCnctnFlag());
			}

			if (tgtFormatProp.isPresent()) {
				TGTFormatProp tgtFormatPropObj = tgtFormatProp.get();
				connectionDto.setTextFormatFlag(tgtFormatPropObj.getTextFormatFlag());
			}

			if (tgtOtherProp.isPresent()) {
				TGTOtherProp tgtOtherPropObj = tgtOtherProp.get();
				connectionDto
						.setTgtOtherPropDto(TGTOtherPropDto.builder().parallelJobs(tgtOtherPropObj.getParallelJobs())
								.parallelUsrRqst(tgtOtherPropObj.getParallelUsrRqst()).build());
			}

		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
		}
		return connectionDto;
	}

	private void persistConnectionDetailsForAws(ConnectionDto connectionDto) {
		dmuS3Repository.saveAndFlush(DMUS3.builder().awsAccessIdLc(connectionDto.getAwsAccessIdLc())
				.awsSecretKeyLc(connectionDto.getAwsSecretKeyLc()).srNo(1L).build());
	}

	private void persistConnectionDetailsForImpala(ConnectionDto connectionDto) {
		hdfsRepository.saveAndFlush(DMUHdfs.builder().hiveCnctnFlag("Y").hiveHostName(connectionDto.getHiveHostName())
				.hivePortNmbr(Long.valueOf(connectionDto.getHivePortNmbr())).srNo(1L).build());
	}

	private void persistConnectionDetailsForHive(ConnectionDto connectionDto) {
		hdfsRepository
				.saveAndFlush(DMUHdfs.builder().impalaCnctnFlag("Y").impalaHostName(connectionDto.getImpalaHostName())
						.impalaPortNmbr(Long.valueOf(connectionDto.getImpalaPortNmbr())).srNo(1L).build());
	}

}
