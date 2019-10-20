package com.dataeconomy.migration.app.batch.listener;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dataeconomy.migration.app.aop.Timed;
import com.dataeconomy.migration.app.mysql.entity.DmuHistoryMainEntity;
import com.dataeconomy.migration.app.mysql.entity.DmuReconMainentity;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryDetailRepository;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.DmuReconMainRepository;
import com.dataeconomy.migration.app.util.DmuConstants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DmuJobCompletionNotificationListener implements JobExecutionListener {

	private long startTime;

	@Autowired
	DmuHistoryMainRepository historyMainRepository;

	@Autowired
	DmuHistoryDetailRepository historyDetailRepository;

	@Autowired
	DmuReconMainRepository dmuReconMainRepository;

	@Override
	@Timed
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public synchronized void afterJob(JobExecution jobExecution) {
		ExecutionContext executionContext = jobExecution.getExecutionContext();
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
	}

	public void updateStatusForHistoryMain(DmuHistoryMainEntity dmuHistoryMainEntity, String status) {
		dmuHistoryMainEntity.setStatus(status);
		historyMainRepository.saveAndFlush(dmuHistoryMainEntity);
	}
}