package com.dataeconomy.migration.app.batch.listener;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dataeconomy.migration.app.aop.Timed;
import com.dataeconomy.migration.app.mysql.entity.DmuHistoryMainEntity;
import com.dataeconomy.migration.app.mysql.entity.DmuReconMainentity;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUReconMainRepository;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryDetailRepository;
import com.dataeconomy.migration.app.util.DmuConstants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DmuJobCompletionNotificationListener implements JobExecutionListener, Ordered {

	private static final int DEFAULT_WIDTH = 80;

	private long startTime;

	@Autowired
	DMUHistoryMainRepository historyMainRepository;

	@Autowired
	DmuHistoryDetailRepository historyDetailRepository;

	@Autowired
	DMUReconMainRepository dmuReconMainRepository;

	@Override
	@Timed
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public synchronized void afterJob(JobExecution jobExecution) {
		ExecutionContext executionContext = jobExecution.getExecutionContext();

		List<Throwable> exceptions = jobExecution.getAllFailureExceptions();

		log.info("This job has occurred some exceptions as follow. " + "[job-name:{}] [size:{}]",
				jobExecution.getJobInstance().getJobName(), exceptions.size());
		exceptions.forEach(th -> log.error("exception has occurred in job.", th));

		if (executionContext.containsKey(DmuConstants.REQUEST_NO)) {
			log.info(" => DmuJobCompletionNotificationListener :: afterJob :: requestNo {} ",
					executionContext.getString(DmuConstants.REQUEST_NO));
			String requestNo = executionContext.getString(DmuConstants.REQUEST_NO);
			historyMainRepository.findById(requestNo).ifPresent(historyMainEntity -> {
				if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
					Long failedCount = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
							DmuConstants.FAILED);
					Long submittedCount = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
							DmuConstants.SUBMITTED);
					if (failedCount > 0) {
						historyMainEntity.setExctnCmpltTime(LocalDateTime.now());
						updateReconMainEntity(requestNo, DmuConstants.FAILED);
						updateStatusForHistoryMain(historyMainEntity, DmuConstants.FAILED);
					} else if (failedCount == 0 && submittedCount == 0) {
						historyMainEntity.setExctnCmpltTime(LocalDateTime.now());
						Long successCount = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
								DmuConstants.SUCCESS);
						if (successCount > 0) {
							updateReconMainEntity(requestNo, DmuConstants.SUBMITTED);
						} else {
							updateReconMainEntity(requestNo, DmuConstants.FAILED);
						}
						updateStatusForHistoryMain(historyMainEntity, DmuConstants.SUCCESS);
					} else {
						updateStatusForHistoryMain(historyMainEntity, DmuConstants.SUBMITTED);
					}
				} else if (jobExecution.getStatus() == BatchStatus.FAILED) {
					log.info("Job finished with status : {} , requestNo : {} ", jobExecution.getStatus(), requestNo);
					updateStatusForHistoryMain(historyMainEntity, DmuConstants.FAILED);
					log.info("BATCH JOB FAILED WITH EXCEPTIONS => ");
					Optional.ofNullable(jobExecution.getAllFailureExceptions()).orElse(new ArrayList<>()).stream()
							.forEach(throwable -> log.error("exception : {} ", throwable.getLocalizedMessage()));
				}
			});
		}

		StringBuilder logBuilder = new StringBuilder();
		logBuilder.append("\n");
		logBuilder.append(createFilledLine('*'));
		logBuilder.append(createFilledLine('-'));
		logBuilder.append("Protocol for " + jobExecution.getJobInstance().getJobName() + " \n");
		logBuilder.append("  Started:      " + jobExecution.getStartTime() + "\n");
		logBuilder.append("  Finished:     " + jobExecution.getEndTime() + "\n");
		logBuilder.append("  Exit-Code:    " + jobExecution.getExitStatus().getExitCode() + "\n");
		logBuilder.append("  Exit-Descr:   " + jobExecution.getExitStatus().getExitDescription() + "\n");
		logBuilder.append("  Status:       " + jobExecution.getStatus() + "\n");
		logBuilder.append("  Content of Job-ExecutionContext:\n");
		for (Entry<String, Object> entry : jobExecution.getExecutionContext().entrySet()) {
			logBuilder.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
		}
		logBuilder.append("  Job-Parameter: \n");
		JobParameters jp = jobExecution.getJobParameters();
		for (Iterator<Entry<String, JobParameter>> iter = jp.getParameters().entrySet().iterator(); iter.hasNext();) {
			Entry<String, JobParameter> entry = iter.next();
			logBuilder.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
		}
		logBuilder.append(createFilledLine('-'));

		jobExecution.getStepExecutions().forEach(stepExecution -> {
			logBuilder.append("Step " + stepExecution.getStepName() + " \n");
			logBuilder.append("  ReadCount:    " + stepExecution.getReadCount() + "\n");
			logBuilder.append("  WriteCount:   " + stepExecution.getWriteCount() + "\n");
			logBuilder.append("  Commits:      " + stepExecution.getCommitCount() + "\n");
			logBuilder.append("  SkipCount:    " + stepExecution.getSkipCount() + "\n");
			logBuilder.append("  Rollbacks:    " + stepExecution.getRollbackCount() + "\n");
			logBuilder.append("  Filter:       " + stepExecution.getFilterCount() + "\n");
			logBuilder.append("  Content of Step-ExecutionContext:\n");
			for (Entry<String, Object> entry : stepExecution.getExecutionContext().entrySet()) {
				logBuilder.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
			}
			logBuilder.append(createFilledLine('-'));
		});

		logBuilder.append(createFilledLine('*'));

		log.info(logBuilder.toString());

		log.info(" ## DmuJobCompletionNotificationListener >> total time take in seconds : {} to complete job {}",
				TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime),
				jobExecution.getJobInstance().getJobName());
	}

	private void updateReconMainEntity(String requestNo, String status) {
		Optional<DmuReconMainentity> reconMainEntityOpt = dmuReconMainRepository.findById(requestNo);
		if (reconMainEntityOpt.isPresent()) {
			DmuReconMainentity dmuReconEntity = reconMainEntityOpt.get();
			dmuReconEntity.setStatus(status);
			dmuReconMainRepository.saveAndFlush(dmuReconEntity);
		}
	}

	@Override
	public synchronized void beforeJob(JobExecution jobExecution) {

		startTime = System.currentTimeMillis();
		log.info("  ## DmuJobCompletionNotificationListener >> Job starts at : {} ", LocalDateTime.now());

		StringBuilder logBuilder = new StringBuilder();
		logBuilder.append(createFilledLine('-'));
		logBuilder.append("Job " + jobExecution.getJobInstance().getJobName() + " started with Job-Execution-Id "
				+ jobExecution.getId() + " \n");
		logBuilder.append("Job-Parameter: \n");
		JobParameters jp = jobExecution.getJobParameters();
		for (Iterator<Entry<String, JobParameter>> iter = jp.getParameters().entrySet().iterator(); iter.hasNext();) {
			Entry<String, JobParameter> entry = iter.next();
			logBuilder.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
		}
		logBuilder.append(createFilledLine('-'));

		log.info(logBuilder.toString());
	}

	public void updateStatusForHistoryMain(DmuHistoryMainEntity dmuHistoryMainEntity, String status) {
		dmuHistoryMainEntity.setStatus(status);
		historyMainRepository.saveAndFlush(dmuHistoryMainEntity);
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE + 10;
	}

	private String createFilledLine(char filler) {
		return StringUtils.leftPad("", DEFAULT_WIDTH, filler) + "\n";
	}
}