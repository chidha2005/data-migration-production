package com.dataeconomy.migration.app.util;

import java.util.Optional;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.connection.HDFSConnectionService;
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
import com.dataeconomy.migration.app.service.TGTOtherPropService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMUHelperService {

	@Autowired
	private HDFSRepository hdfsRepository;

	@Autowired
	private DMUS3Repository dmuS3Repository;

	@Autowired
	private TGTFormatPropRepository tgtFormatPropRepository;

	@Autowired
	private TGTOtherPropRepository tgtOtherPropRepository;

	@Autowired
	HDFSConnectionService hdfsConnectionService;

	@Autowired
	private AuthenticationRepository authenticationRepository;

	@Autowired
	TGTOtherPropService tgtOtherPropService;

	public void saveTGTOtherProperties(ConnectionDto connectionDto) {
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

	public void saveTGTFormatProperties(ConnectionDto connectionDto) {
		if (connectionDto.getTgtFormatPropTempDto() != null) {
			TGTFormatPropTempDto tgtFormatPropObj = connectionDto.getTgtFormatPropTempDto();
			TGTFormatProp tgtFormatPropEntity = tgtFormatPropRepository.getOne(1L);
			if (tgtFormatPropEntity != null) {

				tgtFormatPropEntity.setTextFormatFlag(Constants.NO);
				tgtFormatPropEntity.setFieldDelimiter(null);
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
					tgtFormatPropEntity.setFieldDelimiter(tgtFormatPropObj.getFieldDelimiter());
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
				} else if (StringUtils.equalsIgnoreCase(Constants.AVRO, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setAvroFormatFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.SRC_COMPRESSION, tgtFormatPropObj.getFormatType())) {
					tgtFormatPropEntity.setSrcCmprsnFlag(Constants.YES);
				}

				if (tgtFormatPropObj.getCompressionType() != null
						&& tgtFormatPropObj.getCompressionType().equalsIgnoreCase(Constants.SRC_COMPRESSION)) {
					tgtFormatPropEntity.setSrcCmprsnFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.UN_COMPRESSED,
						tgtFormatPropObj.getCompressionType())) {
					tgtFormatPropEntity.setUncmprsnFlag(Constants.YES);
				} else if (StringUtils.equalsIgnoreCase(Constants.GZIP, tgtFormatPropObj.getCompressionType())) {
					tgtFormatPropEntity.setGzipCmprsnFlag(Constants.YES);
				} else {
					tgtFormatPropEntity.setSrcCmprsnFlag(Constants.NO);
				}
				System.out.println("**Compressiontype***" + tgtFormatPropObj.getCompressionType());
				tgtFormatPropRepository.save(tgtFormatPropEntity);
			}
		}
	}

	public void saveDMUHdfsEntityProperties(ConnectionDto connectionDto) {
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

	public void saveDMUS3Properties(ConnectionDto connectionDto) {
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
				dmuS3Entity.setRoleArn(connectionDto.getRoleArn());
				dmuS3Entity.setPrincipalArn(connectionDto.getPrincipalArn());
				dmuS3Entity.setSamlProviderArn(connectionDto.getSamlProviderArn());
				dmuS3Entity.setRoleSesnName(connectionDto.getRoleSesnName());
				dmuS3Entity.setPolicyArnMembers(connectionDto.getPolicyArnMembers());
				dmuS3Entity.setExternalId(connectionDto.getExternalId());
				dmuS3Entity.setFdrtdUserName(connectionDto.getFdrtdUserName());
				dmuS3Entity.setInlineSesnPolicy(connectionDto.getInlineSesnPolicy());
				dmuS3Entity.setDuration(connectionDto.getDuration().longValue());
				dmuS3Entity.setLdapUserName(connectionDto.getLdapUserName());
				dmuS3Entity.setLdapUserPassw(connectionDto.getLdapUserPassw());
				dmuS3Entity.setLdapDomain(connectionDto.getLdapDomain());
				//dmuS3Entity.setRoleArn(connectionDto.getRoleArn());



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

	public void populateDMUAuthenticationProperties(ConnectionDto connectionDto) {
		Optional<DMUAuthentication> dmuAuthentication = authenticationRepository.findById(1L);
		log.info(" => dmuAuthentication " + dmuAuthentication);
		if (dmuAuthentication.isPresent()) {
			DMUAuthentication dmuAuthenticationObj = dmuAuthentication.get();
			connectionDto.setAuthenticationType(dmuAuthenticationObj.getAuthenticationType());
			connectionDto.setLdapCnctnFlag(dmuAuthenticationObj.getLdapCnctnFlag());
			connectionDto.setKerberosCnctnFlag(dmuAuthenticationObj.getKerberosCnctnFlag());
		}
	}

	public void populateDMUHdfsProperties(ConnectionDto connectionDto) {
		Optional<DMUHdfs> dmuHdfs = hdfsRepository.findById(1L);
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
			connectionDto.setHiveMsUri(dmuHdfsObj.getHiveMsUri());
			connectionDto.setImpalaCnctnFlag(dmuHdfsObj.getImpalaCnctnFlag());
			connectionDto.setSparkCnctnFlag(dmuHdfsObj.getSparkCnctnFlag());
		}
	}

	public void populateDMUS3Properties(ConnectionDto connectionDto) {
		Optional<DMUS3> dmuS3 = dmuS3Repository.findById(1L);
		if (dmuS3.isPresent()) {
			DMUS3 dmuS3Obj = dmuS3.get();
			connectionDto.setCredentialStrgType(dmuS3Obj.getCredentialStrgType());
			connectionDto.setConnectionType(dmuS3Obj.getCredentialStrgType());

			connectionDto.setAwsAccessIdLc(dmuS3Obj.getAwsAccessIdLc());
			connectionDto.setAwsSecretKeyLc(dmuS3Obj.getAwsSecretKeyLc());
			connectionDto.setAwsAccessIdSc(dmuS3Obj.getAwsAccessIdSc());
			connectionDto.setScCrdntlAccessType(dmuS3Obj.getScCrdntlAccessType());
			connectionDto.setAwsSecretKeySc(dmuS3Obj.getAwsSecretKeySc());
			//connectionDto.setAwsAccessIdSc(dmuS3Obj.getAwsAccessIdLc());
			connectionDto.setRoleArn(dmuS3Obj.getRoleArn());
			connectionDto.setPrincipalArn(dmuS3Obj.getPrincipalArn());
			connectionDto.setSamlProviderArn(dmuS3Obj.getSamlProviderArn());
			connectionDto.setRoleSesnName(dmuS3Obj.getRoleSesnName());
			connectionDto.setPolicyArnMembers(dmuS3Obj.getPolicyArnMembers());
			connectionDto.setExternalId(dmuS3Obj.getExternalId());
			connectionDto.setDuration(dmuS3Obj.getDuration() != null ? Math.toIntExact(dmuS3Obj.getDuration()) : 0);
			connectionDto.setLdapUserName(dmuS3Obj.getLdapUserName());
			connectionDto.setLdapUserPassw(dmuS3Obj.getLdapUserPassw());
			connectionDto.setLdapDomain(dmuS3Obj.getLdapDomain());
			connectionDto.setScCrdntlAccessType(dmuS3Obj.getScCrdntlAccessType());
			connectionDto.setInlineSesnPolicy(dmuS3Obj.getInlineSesnPolicy());
		}
	}

	public void populateTGTOtherProperties(ConnectionDto connectionDto) {
		Optional<TGTOtherProp> tgtOtherProp = tgtOtherPropRepository.findById(1L);
		if (tgtOtherProp.isPresent()) {
			TGTOtherProp tgtOtherPropObj = tgtOtherProp.get();
			connectionDto.setTgtOtherPropDto(TGTOtherPropDto.builder().parallelJobs(tgtOtherPropObj.getParallelJobs())
					.parallelUsrRqst(tgtOtherPropObj.getParallelUsrRqst()).tempHiveDB(tgtOtherPropObj.getTempHiveDB())
					.tempHdfsDir(tgtOtherPropObj.getTempHdfsDir()).tokenizationInd(tgtOtherPropObj.getTokenizationInd())
					.ptgyDirPath(tgtOtherPropObj.getPtgyDirPath()).hdfsEdgeNode(tgtOtherPropObj.getHdfsEdgeNode())
					.hdfsPemLocation(tgtOtherPropObj.getHdfsPemLocation())
					.hadoopInstallDir(tgtOtherPropObj.getHadoopInstallDir())
					.hdfsUserName(tgtOtherPropObj.getHdfsUserName()).build());
		}
	}

	public void populateTGTFormatProperties(ConnectionDto connectionDto) {
		Optional<TGTFormatProp> tgtFormatProp = tgtFormatPropRepository.findById(1L);
		if (tgtFormatProp.isPresent()) {
			TGTFormatProp tgtFormatPropObj = tgtFormatProp.get();
			TGTFormatPropTempDto tgtFormatPropTempDto = TGTFormatPropTempDto.builder().build();
			if (StringUtils.isNotBlank(tgtFormatPropObj.getSrcFormatFlag()) && tgtFormatPropObj.getSrcFormatFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setFormatType(Constants.SOURCE);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getTextFormatFlag())&& tgtFormatPropObj.getTextFormatFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setFormatType(Constants.TEXT);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getSqncFormatFlag())&& tgtFormatPropObj.getSqncFormatFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setFormatType(Constants.SEQUENCE);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getRcFormatFlag())&& tgtFormatPropObj.getRcFormatFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setFormatType(Constants.RECORD_COLUMNAR);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getOrcFormatFlag())&& tgtFormatPropObj.getOrcFormatFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setFormatType(Constants.ORC);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getParquetFormatFlag())&& tgtFormatPropObj.getParquetFormatFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setFormatType(Constants.PARQUET);
			}
			else if (StringUtils.isNotBlank(tgtFormatPropObj.getAvroFormatFlag())&& tgtFormatPropObj.getAvroFormatFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setFormatType(Constants.AVRO);
			}




			if (StringUtils.isNotBlank(tgtFormatPropObj.getFieldDelimiter())) {
				tgtFormatPropTempDto.setFieldDelimiter(tgtFormatPropObj.getFieldDelimiter());
			}

			if (StringUtils.isNotBlank(tgtFormatPropObj.getUncmprsnFlag())
					&& tgtFormatPropObj.getUncmprsnFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setCompressionType(Constants.UN_COMPRESSED);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getGzipCmprsnFlag())
					&& tgtFormatPropObj.getGzipCmprsnFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setCompressionType(Constants.GZIP);
			} else if (StringUtils.isNotBlank(tgtFormatPropObj.getSrcCmprsnFlag())
					&& tgtFormatPropObj.getSrcCmprsnFlag().equalsIgnoreCase(Constants.YES)) {
				tgtFormatPropTempDto.setCompressionType(Constants.SRC_COMPRESSION);
			}

			connectionDto.setTgtFormatPropTempDto(tgtFormatPropTempDto);
		}
	}
}
