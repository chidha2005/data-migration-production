package com.dataeconomy.migration.app.service;

import java.util.Optional;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.connection.DMUConnectionValidationService;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.model.TGTFormatPropDto;
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
		try {
			Optional<DMUHdfs> dmuHdfsObjOpt = hdfsRepository.findById(1L);
			Optional<DMUS3> dmuS3EntityOpt = dmuS3Repository.findById(1L);

			if (dmuS3EntityOpt.isPresent()) {
				DMUS3 dmuS3Entity = dmuS3EntityOpt.get();
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

				if (StringUtils.equalsIgnoreCase(Constants.DIRECT_LC, connectionDto.getConnectionType())) {
					dmuS3Entity.setCredentialStrgType(Constants.DIRECT_LC);
					dmuS3Entity.setAwsAccessIdLc(connectionDto.getAwsAccessIdLc());
					dmuS3Entity.setAwsSecretKeyLc(connectionDto.getAwsSecretKeyLc());
				} else if (StringUtils.equalsIgnoreCase(Constants.DIRECT_SC, connectionDto.getConnectionType())) {
					dmuS3Entity.setCredentialStrgType(Constants.DIRECT_SC);
					dmuS3Entity.setAwsAccessIdSc(connectionDto.getAwsAccessIdSc());
					dmuS3Entity.setAwsSecretKeySc(connectionDto.getAwsSecretKeySc());
					if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(), Constants.ASSUME)) {
						dmuS3Entity.setScCrdntlAccessType(Constants.ASSUME);
					} else if (StringUtils.equalsIgnoreCase(connectionDto.getScCrdntlAccessType(),
							Constants.ASSUME_SAML)) {
						dmuS3Entity.setScCrdntlAccessType(Constants.ASSUME_SAML);
					} else if (StringUtils.equalsIgnoreCase(Constants.AWS_FEDERATED_USER,
							connectionDto.getScCrdntlAccessType())) {
						dmuS3Entity.setScCrdntlAccessType(Constants.AWS_FEDERATED_USER);
					}
				}
				dmuS3Repository.save(dmuS3Entity);
			}

			if (dmuHdfsObjOpt.isPresent()) {

				DMUHdfs dmuHdfsEntity = dmuHdfsObjOpt.get();
				dmuHdfsEntity.setHiveCnctnFlag(Constants.YES);
				dmuHdfsEntity.setHiveHostName(null);
				dmuHdfsEntity.setHivePortNmbr(null);
				dmuHdfsEntity.setImpalaCnctnFlag(null);
				dmuHdfsEntity.setImpalaHostName(null);
				dmuHdfsEntity.setImpalaPortNmbr(null);
				dmuHdfsEntity.setSparkCnctnFlag(null);
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
					dmuHdfsEntity.setImpalaPortNmbr(NumberUtils.toLong(connectionDto.getHivePortNmbr(), 0L));
				}
				if (connectionDto.isSparkConnEnabled()) {
					dmuHdfsEntity.setSparkCnctnFlag(Constants.YES);
					dmuHdfsEntity.setHiveMsUri(connectionDto.getHiveMsUri());
					dmuHdfsEntity.setSqlWhDir(connectionDto.getSqlWhDir());
				}
				hdfsRepository.save(dmuHdfsEntity);
			}

			if (connectionDto.getTgtFormatPropDto() != null) {
				Optional<TGTFormatProp> tgtFormatPropEntityOpt = tgtFormatPropRepository.findById(1L);
				if (tgtFormatPropEntityOpt.isPresent()) {
					TGTFormatProp tgtFormatPropEntity = tgtFormatPropEntityOpt.get();
					tgtFormatPropEntity.setTextFormatFlag(connectionDto.getTgtFormatPropDto().getTextFormatFlag());
					tgtFormatPropEntity.setFieldDelimiter(connectionDto.getTgtFormatPropDto().getFieldDelimiter());
					tgtFormatPropEntity.setSrcFormatFlag(connectionDto.getTgtFormatPropDto().getSrcFormatFlag());
					tgtFormatPropEntity.setSqncFormatFlag(connectionDto.getTgtFormatPropDto().getSqncFormatFlag());
					tgtFormatPropEntity.setRcFormatFlag(connectionDto.getTgtFormatPropDto().getRcFormatFlag());
					tgtFormatPropEntity.setAvroFormatFlag(connectionDto.getTgtFormatPropDto().getAvroFormatFlag());
					tgtFormatPropEntity.setOrcFormatFlag(connectionDto.getTgtFormatPropDto().getOrcFormatFlag());
					tgtFormatPropEntity
							.setParquetFormatFlag(connectionDto.getTgtFormatPropDto().getParquetFormatFlag());
					tgtFormatPropEntity.setSrcCmprsnFlag(connectionDto.getTgtFormatPropDto().getSrcCmprsnFlag());
					tgtFormatPropEntity.setUncmprsnFlag(connectionDto.getTgtFormatPropDto().getUncmprsnFlag());
					tgtFormatPropEntity.setGzipCmprsnFlag(connectionDto.getTgtFormatPropDto().getGzipCmprsnFlag());
					tgtFormatPropRepository.save(tgtFormatPropEntity);
				}
			}
		} catch (Exception exception) {

		}
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

			if (dmuAuthentication.isPresent()) {
				DMUAuthentication dmuAuthenticationObj = dmuAuthentication.get();
				connectionDto.setAuthenticationType(dmuAuthenticationObj.getAuthenticationType());
				connectionDto.setLdapCnctnFlag(dmuAuthenticationObj.getLdapCnctnFlag());
				connectionDto.setKerberosCnctnFlag(dmuAuthenticationObj.getKerberosCnctnFlag());
			}

			if (tgtFormatProp.isPresent()) {
				TGTFormatProp tgtFormatPropObj = tgtFormatProp.get();
				connectionDto.setTgtFormatPropDto(
						TGTFormatPropDto.builder().textFormatFlag(tgtFormatPropObj.getTextFormatFlag())
								.srcFormatFlag(tgtFormatPropObj.getSrcFormatFlag())
								.fieldDelimiter(tgtFormatPropObj.getFieldDelimiter())
								.sqncFormatFlag(tgtFormatPropObj.getSqncFormatFlag())
								.srcCmprsnFlag(tgtFormatPropObj.getSrcCmprsnFlag())
								.rcFormatFlag(tgtFormatPropObj.getRcFormatFlag())
								.avroFormatFlag(tgtFormatPropObj.getAvroFormatFlag())
								.orcFormatFlag(tgtFormatPropObj.getOrcFormatFlag())
								.parquetFormatFlag(tgtFormatPropObj.getParquetFormatFlag())
								.srcCmprsnFlag(tgtFormatPropObj.getSrcCmprsnFlag())
								.uncmprsnFlag(tgtFormatPropObj.getUncmprsnFlag())
								.gzipCmprsnFlag(tgtFormatPropObj.getGzipCmprsnFlag()).build());
			}

			if (tgtOtherProp.isPresent()) {
				TGTOtherProp tgtOtherPropObj = tgtOtherProp.get();
				connectionDto.setTgtOtherPropDto(TGTOtherPropDto.builder()
						.parallelJobs(tgtOtherPropObj.getParallelJobs())
						.parallelUsrRqst(tgtOtherPropObj.getParallelUsrRqst())
						.tempHiveDB(tgtOtherPropObj.getTempHiveDB()).tempHdfsDir(tgtOtherPropObj.getTempHdfsDir())
						.tokenizationInd(tgtOtherPropObj.getTokenizationInd())
						.ptgyDirPath(tgtOtherPropObj.getPtgyDirPath()).hdfcEdgeNode(tgtOtherPropObj.getHdfcEdgeNode())
						.hdfsPemLocation(tgtOtherPropObj.getHdfsPemLocation())
						.hdfsUserName(tgtOtherPropObj.getHdfsUserName()).build());
			}

		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
		}
		return connectionDto;
	}

}
