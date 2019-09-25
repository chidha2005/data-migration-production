package com.dataeconomy.migration.app.service;

import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.math.NumberUtils;
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
import com.dataeconomy.migration.app.model.TGTFormatPropTempDto;
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

	@Autowired
	private AWSConnectionService awsConnectionService;

	@Autowired
	HDFSConnectionService hdfsConnectionService;

	@Autowired
	TGTOtherPropService tgtOtherPropService;

	@PostConstruct
	public void initConnectionDetails() {
		/*
		 * try { //getConnectionDetails(); } catch (DataMigrationException exception) {
		 * log.
		 * info(" Exception occured at ConnectionService :: getConnectionDetails {} ",
		 * ExceptionUtils.getStackTrace(exception)); }
		 */
	}

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
				saveDMUS3Properties(connectionDto);
				awsConnectionService.populateAWSCredentials();
			}
			if (Constants.HDFS.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				saveDMUHdfsEntityProperties(connectionDto);
				hdfsConnectionService.initDataSourceConfig();
			}
			if (Constants.TARGET_FILE_PROPS.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				saveTGTFormatProperties(connectionDto);
			}
			if (Constants.OTHER_PROPS.equalsIgnoreCase(connectionDto.getConnectionGroup())) {
				saveTGTOtherProperties(connectionDto);
			}

		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: saveConnectionDetails ::   {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Exception while saving connection properties ");
		}
		return true;
	}

	private void saveTGTOtherProperties(ConnectionDto connectionDto) {
		try {
			if (connectionDto.getTgtOtherPropDto() != null) {
				Optional<TGTOtherProp> tgtOtherPropOpt = tgtOtherPropRepository.findById(1L);
				if (tgtOtherPropOpt.isPresent()) {
					TGTOtherProp tgtOtherPropOptEntity = tgtOtherPropOpt.get();
					tgtOtherPropOptEntity.setParallelJobs(tgtOtherPropOptEntity.getParallelJobs());
					tgtOtherPropOptEntity.setParallelUsrRqst(connectionDto.getTgtOtherPropDto().getParallelUsrRqst());
					tgtOtherPropOptEntity.setTempHiveDB(connectionDto.getTgtOtherPropDto().getTempHiveDB());
					tgtOtherPropOptEntity.setTempHdfsDir(connectionDto.getTgtOtherPropDto().getTempHdfsDir());
					tgtOtherPropOptEntity.setTokenizationInd(connectionDto.getTgtOtherPropDto().getTokenizationInd());
					tgtOtherPropOptEntity.setPtgyDirPath(connectionDto.getTgtOtherPropDto().getPtgyDirPath());
					tgtOtherPropOptEntity.setHdfsEdgeNode(connectionDto.getTgtOtherPropDto().getHdfsEdgeNode());
					tgtOtherPropOptEntity.setHdfsUserName(connectionDto.getTgtOtherPropDto().getHdfsUserName());
					tgtOtherPropOptEntity.setHdfsPemLocation(connectionDto.getTgtOtherPropDto().getHdfsPemLocation());
					tgtOtherPropRepository.save(tgtOtherPropOptEntity);
				}
			}
		} catch (Exception exception) {
			log.info(" Exception occured at TGTOtherPropService :: getAllTGTOtherProp {} ",
					ExceptionUtils.getStackTrace(exception));
		}
	}

	private void saveTGTFormatProperties(ConnectionDto connectionDto) {
		if (connectionDto.getTgtFormatPropTempDto() != null) {
			TGTFormatPropTempDto tgtFormatPropObj = connectionDto.getTgtFormatPropTempDto();
			TGTFormatProp tgtFormatPropEntity = tgtFormatPropRepository.getOne(1L);
			if (tgtFormatPropEntity != null) {

				tgtFormatPropEntity.setTextFormatFlag(Constants.NO);
				tgtFormatPropEntity.setFieldDelimiter(Constants.NO);
				tgtFormatPropEntity.setSrcFormatFlag(Constants.NO);
				tgtFormatPropEntity.setSqncFormatFlag(Constants.NO);
				tgtFormatPropEntity.setRcFormatFlag(Constants.NO);
				tgtFormatPropEntity.setAvroFormatFlag(Constants.NO);
				tgtFormatPropEntity.setOrcFormatFlag(Constants.NO);
				tgtFormatPropEntity.setParquetFormatFlag(Constants.NO);
				tgtFormatPropEntity.setSrcCmprsnFlag(Constants.NO);
				tgtFormatPropEntity.setUncmprsnFlag(Constants.NO);
				tgtFormatPropEntity.setGzipCmprsnFlag(Constants.NO);

				if (StringUtils.isNotBlank(tgtFormatPropObj.getFieldDelimiter())) {
					tgtFormatPropEntity.setFieldDelimiter(Constants.YES);
				}

				if (StringUtils.equalsIgnoreCase(Constants.SOURCE, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setSrcFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.TEXT, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setTextFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.SEQUENCE, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setSqncFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.RECORD_COLUMNAR, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setRcFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.ORC, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setOrcFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.PARQUET, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setParquetFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.UN_COMPRESSED, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setUncmprsnFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.GZIP, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setGzipCmprsnFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.AVRO, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setAvroFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.SRC_COMPRESSION, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setSrcCmprsnFlag(Constants.YES);
				}

				tgtFormatPropRepository.save(tgtFormatPropEntity);
			}
		}
	}

	private void saveDMUHdfsEntityProperties(ConnectionDto connectionDto) {
		DMUHdfs dmuHdfsEntity = hdfsRepository.getOne(1L);
		if (dmuHdfsEntity != null) {
			dmuHdfsEntity.setHiveCnctnFlag(Constants.NO);
			dmuHdfsEntity.setHiveHostName(null);
			dmuHdfsEntity.setHivePortNmbr(null);
			dmuHdfsEntity.setImpalaCnctnFlag(Constants.NO);
			dmuHdfsEntity.setImpalaHostName(null);
			dmuHdfsEntity.setImpalaPortNmbr(null);
			dmuHdfsEntity.setSparkCnctnFlag(Constants.NO);
			dmuHdfsEntity.setSqlWhDir(null);
			dmuHdfsEntity.setHiveMsUri(null);

			if (connectionDto.isHiveConnEnabled()) {
				dmuHdfsEntity.setHiveCnctnFlag(Constants.YES);
				dmuHdfsEntity.setHiveHostName(connectionDto.getHiveHostName());
				dmuHdfsEntity.setHivePortNmbr(NumberUtils.toLong(connectionDto.getHivePortNmbr(), 0L));
			}
			if (connectionDto.isImpalaConnEnabled()) {
				dmuHdfsEntity.setImpalaCnctnFlag(Constants.YES);
				dmuHdfsEntity.setImpalaHostName(connectionDto.getImpalaHostName());
				dmuHdfsEntity.setImpalaPortNmbr(NumberUtils.toLong(connectionDto.getImpalaPortNmbr(), 0L));
			}
			if (connectionDto.isSparkConnEnabled()) {
				dmuHdfsEntity.setSparkCnctnFlag(Constants.YES);
				dmuHdfsEntity.setHiveMsUri(connectionDto.getHiveMsUri());
				dmuHdfsEntity.setSqlWhDir(connectionDto.getSqlWhDir());
			}
			hdfsRepository.save(dmuHdfsEntity);
		}
	}

	private void saveDMUS3Properties(ConnectionDto connectionDto) {
		DMUS3 dmuS3Entity = dmuS3Repository.getOne(1L);

		if (dmuS3Entity != null) {
			dmuS3Entity.setCredentialStrgType(null);
			dmuS3Entity.setAwsAccessIdLc(null);
			dmuS3Entity.setAwsSecretKeyLc(null);
			dmuS3Entity.setAwsAccessIdSc(null);
			dmuS3Entity.setAwsSecretKeySc(null);
			dmuS3Entity.setRoleArn(null);
			dmuS3Entity.setPrincipalArn(null);
			dmuS3Entity.setSamlProviderArn(null);
			dmuS3Entity.setRoleSesnName(null);
			dmuS3Entity.setPolicyArnMembers(null);
			dmuS3Entity.setExternalId(null);
			dmuS3Entity.setFdrtdUserName(null);
			dmuS3Entity.setInlineSesnPolicy(null);
			dmuS3Entity.setDuration(0L);
			dmuS3Entity.setLdapUserName(null);
			dmuS3Entity.setLdapDomain(null);
			dmuS3Entity.setLdapUserName(null);
			dmuS3Entity.setLdapUserPassw(null);
			dmuS3Entity.setScCrdntlAccessType(null);

			if (StringUtils.equalsIgnoreCase(Constants.DIRECT_HDFS, connectionDto.getConnectionType())) {
				dmuS3Entity.setCredentialStrgType(Constants.DIRECT_HDFS);
			} else if (StringUtils.equalsIgnoreCase(Constants.DIRECT_LC, connectionDto.getConnectionType())) {
				dmuS3Entity.setCredentialStrgType(Constants.DIRECT_LC);
				dmuS3Entity.setAwsAccessIdLc(connectionDto.getAwsAccessIdLc());
				dmuS3Entity.setAwsSecretKeyLc(connectionDto.getAwsSecretKeyLc());
			} else if (StringUtils.equalsIgnoreCase(Constants.DIRECT_SC, connectionDto.getConnectionType())) {
				dmuS3Entity.setCredentialStrgType(Constants.DIRECT_SC);
				dmuS3Entity.setAwsAccessIdSc(connectionDto.getAwsAccessIdSc());
				dmuS3Entity.setAwsSecretKeySc(connectionDto.getAwsSecretKeySc());
				if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME)) {
					dmuS3Entity.setScCrdntlAccessType(Constants.ASSUME);
				} else if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME_SAML)) {
					dmuS3Entity.setScCrdntlAccessType(Constants.ASSUME_SAML);
				} else if (StringUtils.equalsIgnoreCase(Constants.AWS_FEDERATED_USER,
						connectionDto.getScCrdntlAccessType())) {
					dmuS3Entity.setScCrdntlAccessType(Constants.AWS_FEDERATED_USER);
				}
			}
			dmuS3Repository.save(dmuS3Entity);
		}
	}

	@Cacheable("dataEconomyCache")
	public ConnectionDto getConnectionDetails() throws DataMigrationException {
		ConnectionDto connectionDto = ConnectionDto.builder().build();
		try {
			Optional<DMUHdfs> dmuHdfs = hdfsRepository.findById(1L);
			Optional<DMUS3> dmuS3 = dmuS3Repository.findById(1L);
			Optional<DMUAuthentication> dmuAuthentication = authenticationRepository.findById(1L);
			Optional<TGTFormatProp> tgtFormatProp = tgtFormatPropRepository.findById(1L);
			Optional<TGTOtherProp> tgtOtherProp = tgtOtherPropRepository.findById(1L);

			populateDMUHdfsProperties(connectionDto, dmuHdfs);
			populateDMUS3Properties(connectionDto, dmuS3);
			populateDMUAuthenticationProperties(connectionDto, dmuAuthentication);
			populateTGTFormatProperties(connectionDto, tgtFormatProp);
			populateTGTOtherProperties(connectionDto, tgtOtherProp);
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Exception occured while retrieving connection details ");
		}
		return connectionDto;

	}

	private void populateDMUAuthenticationProperties(ConnectionDto connectionDto,
			Optional<DMUAuthentication> dmuAuthentication) {
		if (dmuAuthentication.isPresent()) {
			DMUAuthentication dmuAuthenticationObj = dmuAuthentication.get();
			connectionDto.setAuthenticationType(dmuAuthenticationObj.getAuthenticationType());
			connectionDto.setLdapCnctnFlag(dmuAuthenticationObj.getLdapCnctnFlag());
			connectionDto.setKerberosCnctnFlag(dmuAuthenticationObj.getKerberosCnctnFlag());
		}
	}

	private void populateDMUHdfsProperties(ConnectionDto connectionDto, Optional<DMUHdfs> dmuHdfs) {
		if (dmuHdfs.isPresent()) {
			DMUHdfs dmuHdfsObj = dmuHdfs.get();
			if (Constants.YES.equalsIgnoreCase(dmuHdfsObj.getHiveCnctnFlag())) {
				connectionDto.setHiveConnEnabled(true);
			}
			if (Constants.YES.equalsIgnoreCase(dmuHdfsObj.getImpalaCnctnFlag())) {
				connectionDto.setImpalaConnEnabled(true);
			}
			if (Constants.YES.equalsIgnoreCase(dmuHdfsObj.getSparkCnctnFlag())) {
				connectionDto.setSparkConnEnabled(true);
			}
			connectionDto.setHiveCnctnFlag(dmuHdfsObj.getHiveCnctnFlag());
			connectionDto.setHiveHostName(dmuHdfsObj.getHiveHostName());
			connectionDto.setHivePortNmbr(
					dmuHdfsObj.getHivePortNmbr() != null ? String.valueOf(dmuHdfsObj.getHivePortNmbr()) : "");

			connectionDto.setImpalaCnctnFlag(dmuHdfsObj.getImpalaCnctnFlag());
			connectionDto.setImpalaPortNmbr(
					dmuHdfsObj.getImpalaPortNmbr() != null ? String.valueOf(dmuHdfsObj.getImpalaPortNmbr()) : "");
			connectionDto.setImpalaHostName(dmuHdfsObj.getImpalaHostName());

			connectionDto.setSqlWhDir(dmuHdfsObj.getSqlWhDir());
			connectionDto.setImpalaCnctnFlag(dmuHdfsObj.getImpalaCnctnFlag());
			connectionDto.setSparkCnctnFlag(dmuHdfsObj.getSparkCnctnFlag());
		}
	}

	private void populateDMUS3Properties(ConnectionDto connectionDto, Optional<DMUS3> dmuS3) {
		if (dmuS3.isPresent()) {
			DMUS3 dmuS3Obj = dmuS3.get();
			connectionDto.setCredentialStrgType(dmuS3Obj.getCredentialStrgType());
			connectionDto.setConnectionType(dmuS3Obj.getCredentialStrgType());

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

	private void populateTGTOtherProperties(ConnectionDto connectionDto, Optional<TGTOtherProp> tgtOtherProp) {
		if (tgtOtherProp.isPresent()) {
			TGTOtherProp tgtOtherPropObj = tgtOtherProp.get();
			connectionDto.setTgtOtherPropDto(TGTOtherPropDto.builder().parallelJobs(tgtOtherPropObj.getParallelJobs())
					.parallelUsrRqst(tgtOtherPropObj.getParallelUsrRqst()).tempHiveDB(tgtOtherPropObj.getTempHiveDB())
					.tempHdfsDir(tgtOtherPropObj.getTempHdfsDir()).tokenizationInd(tgtOtherPropObj.getTokenizationInd())
					.ptgyDirPath(tgtOtherPropObj.getPtgyDirPath()).hdfsEdgeNode(tgtOtherPropObj.getHdfsEdgeNode())
					.hdfsPemLocation(tgtOtherPropObj.getHdfsPemLocation())
					.hdfsUserName(tgtOtherPropObj.getHdfsUserName()).build());
		}
	}

	private void populateTGTFormatProperties(ConnectionDto connectionDto, Optional<TGTFormatProp> tgtFormatProp) {
		if (tgtFormatProp.isPresent()) {
			TGTFormatProp tgtFormatPropObj = tgtFormatProp.get();
			TGTFormatPropTempDto tgtFormatPropTempDto = TGTFormatPropTempDto.builder().build();
			if (StringUtils.isNotBlank(tgtFormatPropObj.getSrcFormatFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.SOURCE);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getTextFormatFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.TEXT);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getSqncFormatFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.SEQUENCE);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getRcFormatFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.RECORD_COLUMNAR);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getFieldDelimiter())) {
				tgtFormatPropTempDto.setFieldDelimiter(Constants.YES);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getOrcFormatFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.ORC);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getParquetFormatFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.PARQUET);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getUncmprsnFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.UN_COMPRESSED);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getGzipCmprsnFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.GZIP);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getSrcCmprsnFlag())) {
				tgtFormatPropTempDto.setFormatType(Constants.SRC_COMPRESSION);
			}
			connectionDto.setTgtFormatPropTempDto(tgtFormatPropTempDto);
		}
	}

}