package com.dataeconomy.migration.app.service;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.conn.service.HiveConnectionService;
import com.dataeconomy.migration.app.conn.service.ImaplaConnectionService;
import com.dataeconomy.migration.app.conn.service.SparkConnectionService;
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
import com.dataeconomy.migration.app.service.aws.DMULongTermAwsCredentialsService;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ConnectionService {

	// private Map<String, String> cache = Collections
	// .synchronizedMap(new PassiveExpiringMap<String, String>(2, TimeUnit.HOURS));

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
	private HiveConnectionService hiveConnectionService;

	@Autowired
	private ImaplaConnectionService imaplaConnectionService;

	@Autowired
	private SparkConnectionService sparkConnectionService;

	@Autowired
	private AwsFederatedTempCredentialsService awsFederatedTempCredentialsService;

	@Autowired
	private DMULongTermAwsCredentialsService awsLongTermAwsCredentialsService;

	@Autowired
	private DMUAwsAssumeRoleCredentialsService awsAssumeRoleCredentialsService;

	private String hiveConnString;

	private String impalaConnString;

	private String sparkConnString;

	public boolean validateConnection(ConnectionDto connectionDto) throws DataMigrationException {
		try {
			log.info(" ConnectionService :: validateConnection :: connectionDto {}",
					ObjectUtils.toString(connectionDto));
			if (StringUtils.equalsIgnoreCase(Constants.HIVE, connectionDto.getConnectionType())) {
				Optional<String> hiveConnStringOpt = hiveConnectionService.getHiveConnectionDetails(connectionDto);
				if (hiveConnStringOpt.isPresent()) {
					hiveConnString = hiveConnStringOpt.get();
					log.info(" ConnectionService :: validateConnection :: hiveConnString {}", hiveConnString);
					return true;
				} else {
					throw new DataMigrationException("Invalid Connection Details for short term AWS Validation ");
				}
			}
			if (StringUtils.equalsIgnoreCase(Constants.IMPALA, connectionDto.getConnectionType())) {
				Optional<String> impalaConnStringOpt = imaplaConnectionService
						.getImpalaConnectionDetails(connectionDto);
				if (impalaConnStringOpt.isPresent()) {
					impalaConnString = impalaConnStringOpt.get();
					log.info(" ConnectionService :: validateConnection :: impalaConnString {}", impalaConnString);
					return true;
				} else {

				}
			}
			if (StringUtils.equalsIgnoreCase(Constants.SPARK, connectionDto.getConnectionType())) {
				Optional<String> sparkConnStringOpt = sparkConnectionService.getSparkConnectionDetails(connectionDto);
				if (sparkConnStringOpt.isPresent()) {
					sparkConnString = sparkConnStringOpt.get();
					log.info(" ConnectionService :: validateConnection :: sparkConnString {}", sparkConnString);
					return true;
				} else {

				}
			}
			if (StringUtils.equalsIgnoreCase(Constants.DIRECT_LC, connectionDto.getConnectionType())) {
				awsLongTermAwsCredentialsService.validateLongTermAWSCredentials(connectionDto);
				return true;
			}

			if (StringUtils.equalsIgnoreCase(Constants.DIRECT_SC, connectionDto.getConnectionType())) {
				if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME)) {
					awsAssumeRoleCredentialsService.getAwsAssumeRoleRequestCredentials(connectionDto);
					return true;
				} else if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME_SAML))
					awsAssumeRoleCredentialsService.getAwsAssumeRoleRequestCredentials(connectionDto);
				return true;
			} else if (StringUtils.equalsIgnoreCase(Constants.AWS_FEDERATED_USER, connectionDto.getConnectionType())) {
				awsFederatedTempCredentialsService.getFederatedCredentials(connectionDto);
				return true;
			} else {
				throw new DataMigrationException("Invalid Connection Details for short term AWS Validation ");
			}
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionObject :: validateConnection {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Invalid Connection Details for Connection Validation");
		}
	}

	public boolean saveConnectionDetails(String requestParam, String awsRequestParam, ConnectionDto connectionDto) {
		log.info(" ConnectionService :: saveConnectionDetails :: requestParam : {} :: connection details {} ",
				requestParam, Objects.toString(connectionDto, "Invlaid Connection parameters to test "));
		try {
			if (StringUtils.equalsIgnoreCase(awsRequestParam, Constants.PROVIDE)) {
				persistConnectionDetailsForAws(connectionDto);
			} else if (StringUtils.equalsIgnoreCase(requestParam, Constants.HIVE)) {
				persistConnectionDetailsForHive(connectionDto);
			} else if (StringUtils.equalsIgnoreCase(requestParam, Constants.IMPALA)) {
				persistConnectionDetailsForImpala(connectionDto);
			}
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: saveConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
		}
		return false;
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
				connectionDto.setKerberosCnctnFlag("Y");
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

	public void getConnectionObject(ConnectionDto connectionDto, String userId, String password) throws Exception {
		try {
			if (StringUtils.equalsIgnoreCase(Constants.YES, connectionDto.getHiveCnctnFlag())) {
				Optional<String> hiveConnectionUrl = hiveConnectionService.getHiveConnectionDetails(connectionDto);
				if (hiveConnectionUrl.isPresent()) {

				} else {
					throw new Exception("Not a valid Hive Connection Details!");
				}
			} else if (StringUtils.equalsIgnoreCase(Constants.YES, connectionDto.getImpalaCnctnFlag())) {
				Optional<String> impalaConnectionUrl = imaplaConnectionService
						.getImpalaConnectionDetails(connectionDto);
				if (impalaConnectionUrl.isPresent()) {

				} else {
					throw new Exception("Not a valid Hive Connection Details!");
				}
			} else if (StringUtils.equalsIgnoreCase(Constants.YES, connectionDto.getSparkCnctnFlag())) {
				Optional<String> sparkConnectionUrl = sparkConnectionService.getSparkConnectionDetails(connectionDto);
				if (sparkConnectionUrl.isPresent()) {

				} else {
					throw new Exception("Not a valid Hive Connection Details!");
				}
			} else {
				throw new Exception("Not a valid Authentication Details!");
			}
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionObject {} ",
					ExceptionUtils.getStackTrace(exception));
			throw exception;
		}
	}

	public String getHiveConnString() {
		return hiveConnString;
	}

	public void setHiveConnString(String hiveConnString) {
		this.hiveConnString = hiveConnString;
	}

	public String getImpalaConnString() {
		return impalaConnString;
	}

	public void setImpalaConnString(String impalaConnString) {
		this.impalaConnString = impalaConnString;
	}

	public String getSparkConnString() {
		return sparkConnString;
	}

	public void setSparkConnString(String sparkConnString) {
		this.sparkConnString = sparkConnString;
	}

}
