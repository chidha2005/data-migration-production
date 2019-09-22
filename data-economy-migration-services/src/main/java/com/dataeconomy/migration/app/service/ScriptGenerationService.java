package com.dataeconomy.migration.app.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import javax.transaction.Transactional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.mysql.entity.DMUHistoryDetail;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.HistoryDetailRepository;
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
	private JdbcTemplate hiveJdbcTemplate;

	@Transactional
	public void proceedScriptGenerationForRequest(String requestNo) {
		log.info("ScriptGenerationService ::  proceedScriptGenerationForRequest :: requestNo :: {} ", requestNo);
		try {
			Optional<DMUHistoryMain> dmuHistoryMainOpt = dmuHistoryMainRepository.findById(requestNo);
			if (dmuHistoryMainOpt.isPresent()) {

				DMUHistoryMain dmuHistoryMainEntity = dmuHistoryMainOpt.get();
				dmuHistoryMainEntity.setStatus(Constants.IN_PROGRESS);
				dmuHistoryMainRepository.save(dmuHistoryMainEntity);

				Optional<List<DMUHistoryDetail>> dmuHistoryDetailListOpt = Optional
						.ofNullable(historyDetailRepository.findHistoryDetailsByRequestNumber(requestNo));

				if (dmuHistoryDetailListOpt.isPresent()) {
					dmuHistoryDetailListOpt.get().stream().forEach(dmuHistoryDetail -> {

						dmuHistoryDetail.setStatus(Constants.IN_PROGRESS);
						historyDetailRepository.save(dmuHistoryDetail);

						String hiveLocation = invokeHDFSService(dmuHistoryDetail);

						if (StringUtils.isBlank(hiveLocation)) {
							dmuHistoryDetail.setStatus(Constants.FAILED);
							historyDetailRepository.save(dmuHistoryDetail);
						} else {
							if (StringUtils.isBlank(dmuHistoryDetail.getFilterCondition()) && StringUtils
									.equalsIgnoreCase(dmuHistoryDetail.getIncrementalClmn(), Constants.NO)) {
								proceedScriptGenerationForRequestHelper(dmuHistoryDetail, hiveLocation);
							} else if (StringUtils.equalsIgnoreCase(dmuHistoryDetail.getIncrementalClmn(),
									Constants.YES) || StringUtils.isNotBlank(dmuHistoryDetail.getFilterCondition())) {
								dmuHistoryDetail.setStatus(Constants.NEW_SCENARIO);
								historyDetailRepository.save(dmuHistoryDetail);
							} else {
								dmuHistoryDetail.setStatus(Constants.UNKNOWN_CASE);
								historyDetailRepository.save(dmuHistoryDetail);
							}
						}
					});
				}
			}
		} catch (Exception exception) {
			log.error(
					"Exception occurred at ScriptGenerationService ::  proceedScriptGenerationForRequest :: requestNo :: {} \n {} ",
					requestNo, ExceptionUtils.getStackTrace(exception));
		}
	}

	private void proceedScriptGenerationForRequestHelper(DMUHistoryDetail dmuHistoryDetail, String hiveLocation) {
		log.info(
				"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} ",
				hiveLocation, dmuHistoryDetail.getTargetS3Bucket());
		String AWS_ACCESS_ID = null;
		String AWS_SECRET_KEY = null;
		String sftpcmd = null;
		try {
			if (StringUtils.isBlank(AWS_ACCESS_ID)) {
				sftpcmd = "ssh -i /Users/chaturvedula/Downloads/dmu-user.pem dmu-user@18.216.202.239 /opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/hadoop/bin/hadoop distcp "
						+ hiveLocation + "/*  s3a://@" + dmuHistoryDetail.getTargetS3Bucket();
			} else {
				sftpcmd = "ssh -i /Users/chaturvedula/Downloads/dmu-user.pem dmu-user@18.216.202.239 /opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/hadoop/bin/hadoop distcp "
						+ hiveLocation + "/*  s3a://" + AWS_ACCESS_ID + ":" + AWS_SECRET_KEY + "@"
						+ dmuHistoryDetail.getTargetS3Bucket();
			}

			Process p = Runtime.getRuntime().exec(sftpcmd);
			InputStreamReader ise = new InputStreamReader(p.getErrorStream());
			BufferedReader bre = new BufferedReader(ise);
			InputStreamReader iso = new InputStreamReader(p.getInputStream());
			BufferedReader bro = new BufferedReader(iso);

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
					hiveLocation, dmuHistoryDetail.getTargetS3Bucket(), successLine);

			log.info(
					"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} :: error response => {} ",
					hiveLocation, dmuHistoryDetail.getTargetS3Bucket(), errorLine);
			int exitVal = p.waitFor();
			if (exitVal == 0) {
				dmuHistoryDetail.setStatus(StatusConstants.HttpConstants.SUCCESS.name());
				historyDetailRepository.save(dmuHistoryDetail);
				log.info(
						"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} :: status => {} ",
						hiveLocation, dmuHistoryDetail.getTargetS3Bucket(), StatusConstants.HttpConstants.SUCCESS);
			} else {
				dmuHistoryDetail.setStatus(StatusConstants.HttpConstants.FAILURE.name());
				historyDetailRepository.save(dmuHistoryDetail);
				log.info(
						"called=> ScriptGenerationService ::  proceedScriptGenerationForRequest :: proceedScriptGenerationForRequestHelper :: hiveLocation {} :: busketName :: {} :: status => {} ",
						hiveLocation, dmuHistoryDetail.getTargetS3Bucket(), StatusConstants.HttpConstants.FAILURE);
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
			return hiveJdbcTemplate.query("SHOW CREATE TABLE " + dmuHistoryDetail.getSchemaName(),
					new ResultSetExtractor<String>() {

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
			log.error(
					"Exception occurred at ScriptGenerationService ::  proceedScriptGenerationForRequest :: invokeHDFSService :: {}   ",
					ExceptionUtils.getStackTrace(exception));
			return null;
		}
	}

}
