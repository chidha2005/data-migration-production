package com.dataeconomy.migration.app.batch;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dataeconomy.migration.app.batch.listener.DmuJobCompletionNotificationListener;
import com.dataeconomy.migration.app.batch.listener.DmuStepExecutionNotificationListener;
import com.dataeconomy.migration.app.batch.processor.DmuSchedulerTasklet;
import com.dataeconomy.migration.app.batch.writer.DmuSchedulerJdbcWriter;
import com.dataeconomy.migration.app.mysql.entity.DmuTgtOtherPropEntity;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryDetailRepository;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.DmuTgtOtherPropRepository;
import com.dataeconomy.migration.app.util.DmuConstants;
import com.dataeconomy.migration.app.util.DmuServiceHelper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableBatchProcessing
public class DmuBatchScheduler {

	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	DmuTgtOtherPropRepository otherPropRepository;

	@Autowired
	DmuHistoryMainRepository historyMainRepository;

	@Autowired
	DmuHistoryDetailRepository historyDetailRepository;

	@Autowired
	DmuServiceHelper dmuServiceHelper;

	@Autowired
	JobBuilderFactory jobBuilderFactory;

	@Autowired
	Step s1;

	@Autowired
	DmuJobCompletionNotificationListener jobCompletionListener;

	@Scheduled(fixedDelay = 120000)
	@Transactional(propagation = Propagation.REQUIRED, readOnly = false)
	public void performDataMigrationProcess() {
		Long noOfParallelusers = 0L;
		Long noOfParallelJobs = 0L;
		Optional<DmuTgtOtherPropEntity> otherPropEntityOpt = otherPropRepository.findById(1L);
		if (otherPropEntityOpt.isPresent()) {
			noOfParallelusers = otherPropEntityOpt.get().getParallelUsrRqst();
			noOfParallelJobs = otherPropEntityOpt.get().getParallelJobs();
		}
		long inProgressCount = historyMainRepository.getTaskDetailsCount(DmuConstants.IN_PROGRESS);
		log.info(" => DmuBatchScheduler : inProgressCount : {} , noOfParallelusers: {} , noOfParallelJobs : {} ",
				inProgressCount, noOfParallelusers, noOfParallelJobs);
		if (inProgressCount < noOfParallelusers) {
			long taskSubmittedCount = historyMainRepository.getTaskDetailsCount(DmuConstants.SUBMITTED);
			log.info(" DmuBatchScheduler : taskSubmittedCount : {} ", taskSubmittedCount);
			if (taskSubmittedCount > 0) {
				long limitCount = ((noOfParallelusers - inProgressCount) > taskSubmittedCount) ? taskSubmittedCount
						: (noOfParallelusers - inProgressCount);
				log.info(" DmuBatchScheduler :  limitCount : {}  ", taskSubmittedCount, limitCount);
				log.info("Job Started at : {} ", dateTimeFormatter.format(LocalDateTime.now()));
				Optional.ofNullable(
						historyMainRepository.findHistoryMainDetailsByStatusSchedulerByPageable(DmuConstants.SUBMITTED,
								PageRequest.of(0, Math.toIntExact(limitCount),
										Sort.by(Sort.Direction.ASC, "requestedTime"))))
						.ifPresent(entityList -> entityList.parallelStream().forEach(historyEntity -> {
							historyMainRepository.updateForRequestNo(historyEntity.getRequestNo(),
									DmuConstants.IN_PROGRESS);
							try {
								Thread.currentThread().setName("Thread-" + historyEntity.getRequestNo());
								log.info("current thread {} executing the requestNo {} ",
										Thread.currentThread().getName(), historyEntity.getRequestNo());

								JobExecution jobExecution = jobLauncher.run(
										jobBuilderFactory.get("job-" + historyEntity.getRequestNo())
												.incrementer(new RunIdIncrementer()).start(s1)
												.listener(jobCompletionListener).build(),
										new JobParametersBuilder().addString("requestNo", historyEntity.getRequestNo())
												.addLong("parallelJobs", getJobCount(historyEntity.getRequestNo()))
												.addString("name",
														historyEntity.getRequestNo() + "-" + historyEntity.getUserId())
												.addDate("date", new Date()).addLong("time", System.currentTimeMillis())
												.toJobParameters());
								log.info("Job name, started with time : {} , completed with time  {} ",
										jobExecution.getJobConfigurationName(), jobExecution.getCreateTime(),
										jobExecution.getEndTime());
							} catch (Exception e) {
								log.error(" => Exception occured at DmuBatchScheduler :: {} ",
										ExceptionUtils.getStackTrace(e));
								historyMainRepository.updateForRequestNo(historyEntity.getRequestNo(),
										DmuConstants.FAILED);
							}
						}));
			} else {
				log.info(" DmuBatchScheduler : no tasks submitted for scheduler ");
			}
		} else {
			log.info(" DmuBatchScheduler : Job not executed due to number of users limit => {} exceeded => {} ",
					inProgressCount);
		}
	}

	@Bean
	public Step step1(StepBuilderFactory stepBuilderFactory, DmuSchedulerTasklet schedulerProcessor,
			DmuSchedulerJdbcWriter stepWriter, DmuStepExecutionNotificationListener stepListener,
			TaskExecutor taskExecutor) {
		return stepBuilderFactory.get("step1").tasklet(schedulerProcessor).build();
	}

	private synchronized Long getJobCount(String requestNo) {
		Long numberOfThreads = 0L;
		try {
			Long numberOfJobs = historyDetailRepository.findHistoryDetailsByRequestNoAndStatusAscOrder(requestNo,
					DmuConstants.SUBMITTED);
			log.info(" DmuBatchScheduler => getJobCount => numberOfJobs : {} ", numberOfJobs);
			if (numberOfJobs > 0) {
				Long inProgressJobs = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
						DmuConstants.IN_PROGRESS);
				Long parallelJobs = NumberUtils.toLong(dmuServiceHelper.getProperty(DmuConstants.PARALLEL_JOBS));
				log.info(
						" DmuBatchScheduler => getJobCount => numberOfJobs : {} , inProgressJobs : {} , parallelJobs : {}  ",
						numberOfJobs, inProgressJobs, parallelJobs);
				if (inProgressJobs < parallelJobs) {
					if ((parallelJobs - inProgressJobs) > numberOfJobs) {
						numberOfThreads = numberOfJobs;
					} else {
						numberOfThreads = (parallelJobs - inProgressJobs);
					}
				}
				log.info(
						" DmuBatchScheduler => getJobCount => numberOfThreads : {} to process the request for requestNo :: {} ",
						numberOfThreads, requestNo);
			}
			return numberOfThreads;
		} catch (Exception exception) {
			log.info(" Exception Occured at => DmuBatchScheduler => getJobCount => numberOfJobs : {} ",
					ExceptionUtils.getStackTrace(exception));
			return 0L;
		}
	}

}