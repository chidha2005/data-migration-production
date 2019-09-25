package com.dataeconomy.migration.app.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;
import javax.transaction.Transactional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.BasicSessionCredentials;
import com.dataeconomy.migration.app.connection.AWSConnectionService;
import com.dataeconomy.migration.app.connection.HDFSConnectionService;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.model.TGTFormatPropDto;
import com.dataeconomy.migration.app.model.TGTOtherPropDto;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryDetail;
import com.dataeconomy.migration.app.mysql.entity.TGTFormatProp;
import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.HistoryDetailRepository;
import com.dataeconomy.migration.app.mysql.repository.TGTFormatPropRepository;
import com.dataeconomy.migration.app.mysql.repository.TGTOtherPropRepository;
import com.dataeconomy.migration.app.util.Constants;
import com.dataeconomy.migration.app.util.StatusConstants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ScriptGenerationService {

	@Autowired
	private DMUHistoryMainRepository dmuHistoryMainRepository;

	@Autowired
	private HistoryDetailRepository historyDetailRepository;

	@Autowired
	private HDFSConnectionService hdfsConnectionService;

	@Autowired
	private TGTFormatPropRepository tgtFormatPropRepository;

	@Autowired
	private TGTOtherPropRepository tgtOtherPropRepository;

	@Autowired
	private AWSConnectionService awsConnectionService;

	@Transactional
	public void proceedScriptGenerationForRequest(String requestNo, Long srNo) {
		log.info("ScriptGenerationService ::  proceedScriptGenerationForRequest :: requestNo :: {}  :: srNo {} ",
				requestNo, srNo);
		try {
			Optional<List<DMUHistoryDetail>> dmuHistoryDetailListOpt = Optional
					.ofNullable(historyDetailRepository.findHistoryDetailsByRequestNumberAndSrNo(requestNo, srNo));

			if (dmuHistoryDetailListOpt.isPresent()) {
				dmuHistoryDetailListOpt.get().stream().forEach(dmuHistoryDetail -> {

					dmuHistoryDetail.setStatus(Constants.IN_PROGRESS);
					historyDetailRepository.save(dmuHistoryDetail);

					Optional<TGTFormatProp> tgtFormatProp = tgtFormatPropRepository.findById(1L);
					Optional<TGTOtherProp> tgtOtherProp = tgtOtherPropRepository.findById(1L);
					ConnectionDto connectionDto = ConnectionDto.builder().build();
					populateTGTFormatProperties(connectionDto, tgtFormatProp);
					populateTGTOtherProperties(connectionDto, tgtOtherProp);

					if (StringUtils.isBlank(dmuHistoryDetail.getFilterCondition())
							&& Constants.NO.equalsIgnoreCase(dmuHistoryDetail.getIncrementalFlag())
							&& Constants.YES.equalsIgnoreCase(connectionDto.getTgtFormatPropDto().getSrcFormatFlag())) {
						String hdfspath = invokeHDFSService(dmuHistoryDetail);
						if (StringUtils.isBlank(hdfspath)) {
							dmuHistoryDetail.setStatus(Constants.FAILED);
							historyDetailRepository.save(dmuHistoryDetail);
							// TODO application.log created and store it in server log with date and time
							// Have the error ( “File Path not found in Create Table Statement”) stored in a
							// file (with file name as “Request No + Sr No”.log) on application server. Have
							// a Hyperlink created in the screen for the ”Failed” status value to this error
							// file.
							return;
						} else {
							proceedScriptGenerationForRequestHelper(dmuHistoryDetail, hdfspath, connectionDto,
									requestNo);
						}
					} else if (Constants.YES.equalsIgnoreCase(dmuHistoryDetail.getIncrementalFlag())) {
						dmuHistoryDetail.setStatus(Constants.NEW_SCENARIO);
						historyDetailRepository.save(dmuHistoryDetail);
					} else if (StringUtils.isNotBlank(dmuHistoryDetail.getFilterCondition())
							&& Constants.YES.equalsIgnoreCase(connectionDto.getTgtFormatPropDto().getSrcFormatFlag())) {
						try {
							DataSource dataSource = hdfsConnectionService.getValidDataSource(Constants.SMALLQUERY);
							String path = "";
							String stored = "";
							path = new JdbcTemplate(dataSource).query(
									"SHOW CREATE TABLE " + dmuHistoryDetail.getSchemaName(),
									new ResultSetExtractor<String>() {

										@Override
										public String extractData(ResultSet rs)
												throws SQLException, DataAccessException {
											while (rs.next()) {
												String showTable = rs.getString(Constants.HDFS_LOCATION);
												String storedAs = rs.getString(Constants.STORED_AS);
												if (StringUtils.isNotBlank(showTable)) {
													//path = (showTable.substring(3, showTable.length() - 1).trim());
												}
											}
											return null;
										}
									});
						} catch (Exception exception) {
							dmuHistoryDetail.setStatus(Constants.FAILED);
							historyDetailRepository.save(dmuHistoryDetail);
						}
					}
				});
			}
		} catch (

		Exception exception) {
			log.error(
					"Exception occurred at ScriptGenerationService ::  proceedScriptGenerationForRequest :: requestNo :: {} \n {} ",
					requestNo, ExceptionUtils.getStackTrace(exception));
		}
	}

	private void proceedScriptGenerationForRequestHelper(DMUHistoryDetail dmuHistoryDetail, String hdfsPath,
			ConnectionDto connectionDto, String requestNo) {
		log.info(
				"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hdfsPath {} :: busketName :: {} ",
				hdfsPath, dmuHistoryDetail.getTargetS3Bucket());
		BasicSessionCredentials awsCredentials = awsConnectionService.getBasicSessionCredentials();
		if (awsCredentials == null) {
			dmuHistoryDetail.setStatus(Constants.FAILED);
			historyDetailRepository.save(dmuHistoryDetail);
			return;
		}
		StringBuilder sb = new StringBuilder(400);
		if (Constants.YES.equalsIgnoreCase(connectionDto.getTgtFormatPropDto().getSrcCmprsnFlag())
				|| Constants.YES.equalsIgnoreCase(connectionDto.getTgtFormatPropDto().getUncmprsnFlag())) {
			sb.append(connectionDto.getTgtOtherPropDto().getHadoopInstallDir());
			sb.append(
					"/bin/hadoop distcp -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider -Dfs.s3a.access.key=");
			sb.append(awsCredentials.getAWSAccessKeyId());
			sb.append(" -Dfs.s3a.secret.key=");
			sb.append(awsCredentials.getAWSSecretKey());
			sb.append(" -Dfs.s3a.session.token=");
			sb.append(awsCredentials.getSessionToken());
			sb.append(" ");
			sb.append(hdfsPath);
			sb.append("/* s3a://");
			sb.append(dmuHistoryDetail.getTargetS3Bucket());
		} else if (Constants.YES.equalsIgnoreCase(connectionDto.getTgtFormatPropDto().getGzipCmprsnFlag())) {
			sb.append(" mkdir ");
			sb.append(connectionDto.getTgtOtherPropDto().getTempHdfsDir());
			sb.append("/");
			sb.append(requestNo);
			sb.append("-");
			sb.append(dmuHistoryDetail.getTableName());
			sb.append(" ");
			sb.append(" gzip –rk  ");
			sb.append(hdfsPath);
			sb.append("/* ");
			sb.append(connectionDto.getTgtOtherPropDto().getTempHdfsDir());
			sb.append("/");
			sb.append(requestNo);
			sb.append("-");
			sb.append(dmuHistoryDetail.getTableName());
			sb.append(" ");
			sb.append(connectionDto.getTgtOtherPropDto().getHadoopInstallDir());
			sb.append(
					"/bin/hadoop distcp -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider -Dfs.s3a.access.key=");
			sb.append(awsCredentials.getAWSAccessKeyId());
			sb.append(" -Dfs.s3a.secret.key=");
			sb.append(awsCredentials.getAWSSecretKey());
			sb.append(" -Dfs.s3a.session.token=");
			sb.append(awsCredentials.getSessionToken());
			sb.append(" ");
			sb.append(hdfsPath);
			sb.append("/* s3a://");
			sb.append(dmuHistoryDetail.getTargetS3Bucket());
			sb.append(" ");
			sb.append(" rm –r ");
			sb.append(connectionDto.getTgtOtherPropDto().getTempHdfsDir());
			sb.append(requestNo);
			sb.append("-");
			sb.append(dmuHistoryDetail.getTableName());

		}
		try {
			StringBuilder sshBuilder = new StringBuilder();
			sshBuilder.append("ssh -i ");
			sb.append(" ");
			sshBuilder.append(connectionDto.getTgtOtherPropDto().getHdfsEdgeNode());
			sb.append(" ");
			sshBuilder.append(connectionDto.getTgtOtherPropDto().getHdfsUserName());
			sb.append(" ");
			sshBuilder.append(connectionDto.getTgtOtherPropDto().getHdfsPemLocation());
			sb.append(sb.toString());
			log.info(" ssh commands executi => " + sb.toString());
			Process p = Runtime.getRuntime().exec(sb.toString());
			InputStreamReader ise = new InputStreamReader(p.getErrorStream());
			BufferedReader bre = new BufferedReader(ise);
			InputStreamReader iso = new InputStreamReader(p.getInputStream());
			BufferedReader bro = new BufferedReader(iso);

			// TODO Store the log details of the above command in a log file (with file name
			// as “Request No + Sr No”.log) on application server.

			String errorLine = null;
			String successLine = null;
			while ((errorLine = bre.readLine()) != null) {
				errorLine = (errorLine + errorLine);
			}
			while ((successLine = bro.readLine()) != null) {
				successLine = (successLine + successLine);
			}
			log.info(
					"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} :: success response => {} ",
					hdfsPath, dmuHistoryDetail.getTargetS3Bucket(), successLine);

			log.info(
					"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} :: error response => {} ",
					hdfsPath, dmuHistoryDetail.getTargetS3Bucket(), errorLine);
			int exitVal = p.waitFor();
			if (exitVal == 0) {
				dmuHistoryDetail.setStatus(StatusConstants.HttpConstants.SUCCESS.name());
				historyDetailRepository.save(dmuHistoryDetail);
				log.info(
						"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} :: status => {} ",
						hdfsPath, dmuHistoryDetail.getTargetS3Bucket(), StatusConstants.HttpConstants.SUCCESS);
			} else {
				dmuHistoryDetail.setStatus(StatusConstants.HttpConstants.FAILURE.name());
				historyDetailRepository.save(dmuHistoryDetail);
				log.info(
						"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} :: status => {} ",
						hdfsPath, dmuHistoryDetail.getTargetS3Bucket(), StatusConstants.HttpConstants.FAILURE);
			}
		} catch (Exception exception) {
			log.error(
					"Exception occurred at ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: {}   ",
					ExceptionUtils.getStackTrace(exception));
		}
	}

	private String invokeHDFSService(DMUHistoryDetail dmuHistoryDetail) {
		log.info(" called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: invokeHDFSService ");
		try {
			return new JdbcTemplate(hdfsConnectionService.getValidDataSource(Constants.REGULAR))
					.query("SHOW CREATE TABLE " + dmuHistoryDetail.getSchemaName(), new ResultSetExtractor<String>() {

						@Override
						public String extractData(ResultSet rs) throws SQLException, DataAccessException {
							while (rs.next()) {
								String showTable = rs.getString(1);
								if (StringUtils.equalsIgnoreCase(showTable, Constants.HDFS_LOCATION)) {
									return (showTable.substring(3, showTable.length() - 1).trim());
								}
							}
							return null;
						}
					});
		} catch (Exception exception) {
			dmuHistoryDetail.setStatus(Constants.FAILED);
			historyDetailRepository.save(dmuHistoryDetail);
			log.error(
					"Exception occurred at ScriptGenerationService ::  proceedScriptGenerationForRequest :: invokeHDFSService :: {}   ",
					ExceptionUtils.getStackTrace(exception));
			// TODO application.log created and store it in server log with date and time
			return null;
		}
	}

	private String invokeHDFSServiceForFilterCondition(DMUHistoryDetail dmuHistoryDetail) {
		log.info(" called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: invokeHDFSService ");
		try {
			Map<String, String> map = new HashMap<>();
			new JdbcTemplate(hdfsConnectionService.getValidDataSource(Constants.SMALLQUERY)).query(
					" SELECT COUNT(*) FROM" + dmuHistoryDetail.getSchemaName() + "." + dmuHistoryDetail.getTableName()
							+ "WHERE " + dmuHistoryDetail.getFilterCondition(),
					new ResultSetExtractor<String>() {

						@Override
						public String extractData(ResultSet rs) throws SQLException, DataAccessException {
							while (rs.next()) {
								String showTable = rs.getString(Constants.HDFS_LOCATION);
								String storedAs = rs.getString(Constants.STORED_AS);
								map.put("HDFS_PATH", (showTable.substring(3, showTable.length() - 1).trim()));
								map.put("STORED_AS", (showTable.substring(3, storedAs.length() - 1).trim()));
							}
							return null;
						}
					});
			return null;
		} catch (Exception exception) {
			dmuHistoryDetail.setStatus(Constants.FAILED);
			historyDetailRepository.save(dmuHistoryDetail);
			log.error(
					"Exception occurred at ScriptGenerationService ::  proceedScriptGenerationForRequest :: invokeHDFSService :: {}   ",
					ExceptionUtils.getStackTrace(exception));
			// TODO application.log created and store it in server log with date and time
			return null;
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
					.hdfsUserName(tgtOtherPropObj.getHdfsUserName())
					.hadoopInstallDir(tgtOtherPropObj.getHadoopInstallDir()).build());
		}
	}

	private void populateTGTFormatProperties(ConnectionDto connectionDto, Optional<TGTFormatProp> tgtFormatProp) {
		if (tgtFormatProp.isPresent()) {
			TGTFormatProp tgtFormatPropObj = tgtFormatProp.get();
			connectionDto.setTgtFormatPropDto(TGTFormatPropDto.builder()
					.textFormatFlag(tgtFormatPropObj.getTextFormatFlag())
					.srcFormatFlag(tgtFormatPropObj.getSrcFormatFlag())
					.fieldDelimiter(tgtFormatPropObj.getFieldDelimiter())
					.sqncFormatFlag(tgtFormatPropObj.getSqncFormatFlag())
					.srcCmprsnFlag(tgtFormatPropObj.getSrcCmprsnFlag()).rcFormatFlag(tgtFormatPropObj.getRcFormatFlag())
					.avroFormatFlag(tgtFormatPropObj.getAvroFormatFlag())
					.orcFormatFlag(tgtFormatPropObj.getOrcFormatFlag())
					.parquetFormatFlag(tgtFormatPropObj.getParquetFormatFlag())
					.srcCmprsnFlag(tgtFormatPropObj.getSrcCmprsnFlag()).uncmprsnFlag(tgtFormatPropObj.getUncmprsnFlag())
					.gzipCmprsnFlag(tgtFormatPropObj.getGzipCmprsnFlag()).build());
		}
	}

}
