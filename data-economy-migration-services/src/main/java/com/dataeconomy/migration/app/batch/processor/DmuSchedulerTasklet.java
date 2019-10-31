package com.dataeconomy.migration.app.batch.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.skip.NonSkippableReadException;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.BasicSessionCredentials;
import com.dataeconomy.migration.app.aop.Timed;
import com.dataeconomy.migration.app.connection.DmuAwsConnectionService;
import com.dataeconomy.migration.app.connection.DmuHdfsConnectionService;
import com.dataeconomy.migration.app.mysql.entity.DmuHistoryDetailEntity;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryDetailRepository;
import com.dataeconomy.migration.app.util.DmuConstants;
import com.dataeconomy.migration.app.util.DmuServiceHelper;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@StepScope
public class DmuSchedulerTasklet implements Tasklet {

	@Autowired
	DmuHistoryDetailRepository historyDetailRepository;

	@Autowired
	DmuHdfsConnectionService hdfsConnectionService;

	@Autowired
	DmuAwsConnectionService awsConnectionService;

	@Autowired
	DmuServiceHelper dmuServiceHelper;

	@Autowired
	DmuFilterConditionProcessor dmuFilterConditionProcessor;

	@Value("#{jobParameters['parallelJobs']}")
	Long parallelJobs;

	@Value("#{jobParameters['requestNo']}")
	String requestNo;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		try {

			log.info(" ## DmuSchedulerTasklet => job parameters => requestNo {} ,  parallelJobs {} ", requestNo,
					parallelJobs);
			Optional.ofNullable(historyDetailRepository.findHistoryDetailsByRequestNumberByPageable(requestNo,
					PageRequest.of(0, Math.toIntExact(parallelJobs),
							Sort.by(Sort.Direction.ASC, "dmuHIstoryDetailPK.srNo"))))
					.orElse(new ArrayList<>()).parallelStream().forEach(dmuHistoryDetailEntity -> {
						chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put(
								DmuConstants.REQUEST_NO, dmuHistoryDetailEntity.getDmuHIstoryDetailPK().getRequestNo());
						Thread.currentThread()
								.setName("Thread-" + dmuHistoryDetailEntity.getDmuHIstoryDetailPK().getSrNo());
						log.info(" ## DmuSchedulerTasklet => current thread {} executing the srNo {} ",
								Thread.currentThread().getName(),
								dmuHistoryDetailEntity.getDmuHIstoryDetailPK().getSrNo());
						try {
							updateStatus(dmuHistoryDetailEntity, DmuConstants.IN_PROGRESS);
							if (StringUtils.isBlank(dmuHistoryDetailEntity.getFilterCondition())
									&& StringUtils.equalsIgnoreCase(DmuConstants.NO,
											dmuHistoryDetailEntity.getIncrementalFlag())
									&& StringUtils.equalsIgnoreCase(DmuConstants.YES,
											dmuServiceHelper.getProperty(DmuConstants.SRC_FORMAT_FLAG))) {
								getHdfsPath(dmuHistoryDetailEntity)
										.ifPresent(hdfsPath -> migrateDataToS3(dmuHistoryDetailEntity, hdfsPath));
							} else if (StringUtils.equalsIgnoreCase(DmuConstants.YES,
									dmuHistoryDetailEntity.getIncrementalFlag())) {
								updateStatus(dmuHistoryDetailEntity, DmuConstants.NEW_SCENARIO);
							} else if (StringUtils.isNotBlank(dmuHistoryDetailEntity.getFilterCondition())
									&& StringUtils.equalsIgnoreCase(DmuConstants.YES,
											dmuServiceHelper.getProperty(DmuConstants.SRC_FORMAT_FLAG))) {
								dmuFilterConditionProcessor.processFilterCondition(dmuHistoryDetailEntity);
								migrateDataToS3ForFilterCondition(dmuHistoryDetailEntity);
							} else {
								updateStatus(dmuHistoryDetailEntity, DmuConstants.UNKNOWN_CASE);
							}
						} catch (Exception exception) {
							log.info(" Exception occured at DmuSchedulerProcessor :: process {} ",
									ExceptionUtils.getStackTrace(exception));
							updateStatus(dmuHistoryDetailEntity, DmuConstants.FAILED);
						}
					});
			return RepeatStatus.FINISHED;
		} catch (NonSkippableReadException e) {
			chunkContext.getStepContext().getStepExecution().getJobExecution().stop();
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}

	@Timed
	public synchronized Optional<String> getHdfsPath(DmuHistoryDetailEntity historyEntity) {
		StringBuilder locationHDFS = new StringBuilder(100);
		locationHDFS.append(DmuConstants.SHOW_CREATE_TABLE);
		locationHDFS.append(historyEntity.getSchemaName());
		locationHDFS.append(DmuConstants.DOT_OPERATOR);
		locationHDFS.append(historyEntity.getTableName());

		try (Connection conn = hdfsConnectionService.getValidDataSource(DmuConstants.REGULAR).getConnection();
				Statement statement = conn.createStatement();
				ResultSet resultSet = statement.executeQuery(locationHDFS.toString());) {
			locationHDFS.delete(0, locationHDFS.length());
			while (resultSet.next()) {
				locationHDFS.append(resultSet.getString(1));
			}
			if (StringUtils.isBlank(locationHDFS.toString())) {
				updateStatus(historyEntity, DmuConstants.FAILED);
				return Optional.empty();
			}
			return Optional.ofNullable(
					StringUtils.substring(locationHDFS.toString(), locationHDFS.toString().indexOf("LOCATION") + 8,
							locationHDFS.toString().indexOf("TBLPROPERTIES") - 1).replace("'", ""));
		} catch (Exception exception) {
			updateStatus(historyEntity, DmuConstants.FAILED);
			log.error("Exception occurred at DmuSchedulerProcessor ::  getHdfsPath =>  {}   ",
					ExceptionUtils.getStackTrace(exception));
			return Optional.empty();
		}
	}

	@Timed
	private void migrateDataToS3(DmuHistoryDetailEntity historyEntity, String hdfsPath) {
		BasicSessionCredentials awsCredentials = awsConnectionService.getBasicSessionCredentials();
		if (awsCredentials == null) {
			updateStatus(historyEntity, DmuConstants.FAILED);
		} else {
			String sftcpCommand;
			if (DmuConstants.YES.equalsIgnoreCase(dmuServiceHelper.getProperty(DmuConstants.SRC_CMPRSN_FLAG))
					|| DmuConstants.YES.equalsIgnoreCase(dmuServiceHelper.getProperty(DmuConstants.UNCMPRSN_FLAG))) {
				sftcpCommand = dmuServiceHelper.buildS3MigrationUrl(historyEntity, awsCredentials, hdfsPath);
			} else {
				sftcpCommand = dmuServiceHelper.buildS3MigrationUrl(historyEntity, awsCredentials, hdfsPath);
			}

			StringBuilder sshBuilder = new StringBuilder();
			sshBuilder.append("ssh -i ");
			sshBuilder.append(" ");
			sshBuilder.append(dmuServiceHelper.getProperty(DmuConstants.HDFS_PEM_LOCATION));
			sshBuilder.append(" ");
			sshBuilder.append(dmuServiceHelper.getProperty(DmuConstants.HDFS_USER_NAME));
			if (StringUtils.isNotBlank(dmuServiceHelper.getProperty(DmuConstants.HDFS_EDGE_NODE))) {
				sshBuilder.append("@");
				sshBuilder.append(dmuServiceHelper.getProperty(DmuConstants.HDFS_EDGE_NODE));
				sshBuilder.append(" ");
			} else {
				sshBuilder.append(" ");
			}
			sshBuilder.append(sftcpCommand);
			try {
				executeSSHCommand(historyEntity, sshBuilder);
			} catch (Exception exception) {
				updateStatus(historyEntity, DmuConstants.FAILED);
				log.error("Exception occurred at DmuSchedulerTasklet :: migrateDataToS3 :: {} ",
						ExceptionUtils.getStackTrace(exception));
			}
		}
	}

	@Timed
	private void migrateDataToS3ForFilterCondition(DmuHistoryDetailEntity historyEntity) {
		BasicSessionCredentials awsCredentials = awsConnectionService.getBasicSessionCredentials();
		if (awsCredentials == null) {
			updateStatus(historyEntity, DmuConstants.FAILED);
		} else {
			String sftcpCommand = dmuServiceHelper.buildS3MigrationUrlForFilterCondition(historyEntity, awsCredentials);
			StringBuilder sshBuilder = new StringBuilder();
			sshBuilder.append("ssh -i ");
			sshBuilder.append(" ");
			sshBuilder.append(dmuServiceHelper.getProperty(DmuConstants.HDFS_PEM_LOCATION));
			sshBuilder.append(" ");
			sshBuilder.append(dmuServiceHelper.getProperty(DmuConstants.HDFS_USER_NAME));
			if (StringUtils.isNotBlank(dmuServiceHelper.getProperty(DmuConstants.HDFS_EDGE_NODE))) {
				sshBuilder.append("@");
				sshBuilder.append(dmuServiceHelper.getProperty(DmuConstants.HDFS_EDGE_NODE));
				sshBuilder.append(" ");
			} else {
				sshBuilder.append(" ");
			}
			sshBuilder.append(sftcpCommand);
			try {
				executeSSHCommand(historyEntity, sshBuilder);
			} catch (Exception exception) {
				updateStatus(historyEntity, DmuConstants.FAILED);
				log.error("## Exception occurred at DmuSchedulerTasklet ::  migrateDataToS3ForFilterCondition :: {}   ",
						ExceptionUtils.getStackTrace(exception));
			}
		}
	}

	@Timed
	private void executeSSHCommand(DmuHistoryDetailEntity historyEntity, StringBuilder sshBuilder)
			throws IOException, InterruptedException {
		Process process = Runtime.getRuntime().exec(sshBuilder.toString());
		try (InputStreamReader errorStream = new InputStreamReader(process.getErrorStream());
				BufferedReader errorBuffer = new BufferedReader(errorStream);
				InputStreamReader inputStream = new InputStreamReader(process.getInputStream());
				BufferedReader inputBuffer = new BufferedReader(inputStream)) {
			log.info(" ssh command success => {} ", inputBuffer.lines().collect(Collectors.joining()));
			log.info(" ssh command error => {} ", errorBuffer.lines().collect(Collectors.joining()));
			if (process.waitFor() == 0) {
				updateStatus(historyEntity, DmuConstants.SUCCESS);
			} else {
				updateStatus(historyEntity, DmuConstants.FAILED);
			}
		} catch (Exception e) {
			log.error(" ssh command error ");
			updateStatus(historyEntity, DmuConstants.FAILED);
		}
	}

	@Timed
	public void updateStatus(DmuHistoryDetailEntity historyEntity, String status) {
		historyEntity.setStatus(status);
		historyDetailRepository.save(historyEntity);
	}

}