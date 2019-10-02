package com.dataeconomy.migration.app.batch;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import com.dataeconomy.migration.app.batch.listener.DmuJobCompletionNotificationListener;
import com.dataeconomy.migration.app.batch.listener.DmuStepExecutionNotificationListener;
import com.dataeconomy.migration.app.batch.processor.DmuSchedulerProcessor;
import com.dataeconomy.migration.app.batch.writer.DmuSchedulerJdbcWriter;
import com.dataeconomy.migration.app.mysql.entity.DmuHistoryDetailEntity;
import com.dataeconomy.migration.app.mysql.entity.DmuTgtOtherPropEntity;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryDetailRepository;
import com.dataeconomy.migration.app.mysql.repository.DmuHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.DmuTgtOtherPropRepository;
import com.dataeconomy.migration.app.util.DmuConstants;
import com.dataeconomy.migration.app.util.DmuServiceHelper;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableBatchProcessing
public class DmuBatchScheduler implements SchedulingConfigurer {

	private static final String REQUEST_NO_UPDATED_WITH_STATUS = "requestNo : {} , updated with status : {} ";

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
	Job job;

	@Scheduled(fixedDelay = 120000)
	public void performDataMigrationProcess() {
		Optional<DmuTgtOtherPropEntity> otherPropEntityOpt = otherPropRepository.findById(1L);
		if (otherPropEntityOpt.isPresent()) {
			Long noOfParallelusers = otherPropEntityOpt.get().getParallelUsrRqst();
			Long noOfParallelJobs = otherPropEntityOpt.get().getParallelJobs();
			long inProgressCount = historyMainRepository.getTaskDetailsCount(DmuConstants.IN_PROGRESS);
			log.info(" => DmuBatchScheduler : inProgressCount : {} , noOfParallelusers: {} , noOfParallelJobs : {} ",
					inProgressCount, noOfParallelusers, noOfParallelJobs);
			if (inProgressCount < noOfParallelusers) {
				long taskSubmittedCount = historyMainRepository.getTaskDetailsCount(DmuConstants.SUBMITTED);
				if (taskSubmittedCount != 0L) {
					log.info(" DmuBatchScheduler : taskSubmittedCount : {} ", taskSubmittedCount);
					log.info("Job Started at : {} ", dateTimeFormatter.format(LocalDateTime.now()));
					Optional.ofNullable(
							historyMainRepository.findHistoryMainDetailsByStatusScheduler(DmuConstants.SUBMITTED))
							.ifPresent(historyDetails -> historyDetails.parallelStream().forEach(historyEntity -> {
								historyMainRepository.updateForRequestNo(historyEntity.getRequestNo(),
										DmuConstants.IN_PROGRESS);
								try {
									JobExecution jobExecution = jobLauncher.run(job, new JobParametersBuilder()
											.addString("requestNo", historyEntity.getRequestNo())
											.addString("name",
													historyEntity.getRequestNo() + " -" + historyEntity.getUserId())
											.addDate("date", new Date()).addLong("time", System.currentTimeMillis())
											.toJobParameters());
									log.info("Job started with time : {} ", jobExecution.getCreateTime());
									log.info("Job finished with name : {} ", jobExecution.getJobConfigurationName());
									log.info("Job finished with id : {} ", jobExecution.getJobId());
									log.info("Job finished with time : {} ", jobExecution.getEndTime());

									if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
										long submittedCount = historyDetailRepository
												.findHistoryDetailsByRequestNoAndStatus(historyEntity.getRequestNo(),
														DmuConstants.SUBMITTED);
										log.info("Job finished with status : {} ", jobExecution.getStatus());
										if (historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(
												historyEntity.getRequestNo(), DmuConstants.FAILED) > 0) {
											log.info(REQUEST_NO_UPDATED_WITH_STATUS, historyEntity.getRequestNo(),
													DmuConstants.FAILED);
											if (submittedCount > 0) {
												historyEntity.setStatus(DmuConstants.SUBMITTED);
												historyMainRepository.save(historyEntity);
											} else {
												historyEntity.setStatus(DmuConstants.SUCCESS);
												historyMainRepository.save(historyEntity);
											}
										} else {
											if (submittedCount > 0) {
												historyEntity.setStatus(DmuConstants.SUBMITTED);
												historyMainRepository.save(historyEntity);
											}
											log.info(REQUEST_NO_UPDATED_WITH_STATUS, historyEntity.getRequestNo(),
													DmuConstants.SUCCESS);
											historyMainRepository.updateForRequestNo(historyEntity.getRequestNo(),
													DmuConstants.SUCCESS);
											historyEntity.setStatus(DmuConstants.SUCCESS);
											historyMainRepository.save(historyEntity);
										}
									} else if (jobExecution.getStatus() == BatchStatus.FAILED) {
										log.info("Job finished with status : {} ", jobExecution.getStatus());
										log.info(REQUEST_NO_UPDATED_WITH_STATUS, historyEntity.getRequestNo(),
												DmuConstants.FAILED);
										historyEntity.setStatus(DmuConstants.FAILED);
										historyMainRepository.save(historyEntity);
										log.info("BATCH JOB FAILED WITH EXCEPTIONS");
										Optional.ofNullable(jobExecution.getAllFailureExceptions())
												.orElse(new ArrayList<>()).stream().forEach(throwable -> log
														.error("exception : {} ", throwable.getLocalizedMessage()));
									}
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
				log.info(
						" DmuBatchScheduler : inProgressCount : {} Job not executed due to numbers users requests limit exceeded => {} ",
						inProgressCount);
			}
		}
	}

	@Bean
	public RepositoryMetadata repositoryMetadata() {
		return new DefaultRepositoryMetadata(DmuHistoryDetailRepository.class);
	}

	@Bean
	@StepScope
	public RepositoryItemReader<DmuHistoryDetailEntity> reader(@Value("#{jobParameters['requestNo']}") String requestNo,
			DmuHistoryDetailRepository historyDetailRepository) {
		log.info(" processing item reader for requestNo : {} ", requestNo);
		RepositoryItemReader<DmuHistoryDetailEntity> fullfillment = new RepositoryItemReader<>();
		fullfillment.setRepository(historyDetailRepository);
		fullfillment.setMethodName("findHistoryDetailsByRequestNoAndStatusListForBatch");
		List<Object> list = Lists.newArrayList();
		list.add(requestNo);
		list.add(DmuConstants.SUBMITTED);
		fullfillment.setArguments(list);
		HashMap<String, Sort.Direction> sorts = new HashMap<>();
		sorts.put("dmuHIstoryDetailPK.requestNo", Direction.ASC);
		fullfillment.setSort(sorts);
		fullfillment.setPageSize(Math.toIntExact(getJobCount(requestNo)));
		return fullfillment;
	}

	@Bean
	public Step step1(StepBuilderFactory stepBuilderFactory, DmuSchedulerProcessor schedulerProcessor,
			DmuSchedulerJdbcWriter stepWriter, DmuStepExecutionNotificationListener stepListener,
			RepositoryItemReader<DmuHistoryDetailEntity> reader) {
		return stepBuilderFactory.get("step1").<DmuHistoryDetailEntity, DmuHistoryDetailEntity>chunk(10).reader(reader)
				.processor(schedulerProcessor).writer(stepWriter).listener(stepListener).build();
	}

	@Bean
	public Job job(JobBuilderFactory jobBuilderFactory, Step s1,
			DmuJobCompletionNotificationListener jobCompletionListener) {
		return jobBuilderFactory.get("job").incrementer(new RunIdIncrementer()).start(s1)
				.listener(jobCompletionListener).build();
	}

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.setTaskScheduler(poolScheduler());
	}

	@Bean
	public TaskScheduler poolScheduler() {
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setThreadNamePrefix("poolScheduler");
		scheduler.setPoolSize(20);
		return scheduler;
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