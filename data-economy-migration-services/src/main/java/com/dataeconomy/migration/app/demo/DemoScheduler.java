package com.dataeconomy.migration.app.demo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.TGTOtherPropRepository;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DemoScheduler {

	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

	@Autowired
	private DMUHistoryMainRepository historyMainRepository;

	@Autowired
	private DemoRequestProcessor demorequestProcessor;

	@Autowired
	private TGTOtherPropRepository propOtherRepository;

//	@Scheduled(cron = "* */2 * * * ?")
	@Scheduled(fixedDelay = 180000)
	public void schedulerConfig() {
		try {
			log.info(" => dmuScheduler Task :: Execution Time - {}", dateTimeFormatter.format(LocalDateTime.now()));

			TGTOtherProp tgtOtherPropOpt = propOtherRepository.findById(1L)
					.orElse(TGTOtherProp.builder().parallelUsrRqst(0L).parallelJobs(0L).build());

			Long taskInProgressCount = historyMainRepository.getTaskDetailsCount(Constants.IN_PROGRESS);
			log.info(" => dmuScheduler Task :: current tasks in progress count  Time - {} - count {} ",
					dateTimeFormatter.format(LocalDateTime.now()), taskInProgressCount);

			if (taskInProgressCount == 0L) {
				Long tasksSubmittedCount = historyMainRepository.getTaskDetailsCount(Constants.SUBMITTED);
				log.info(" => dmuScheduler Task ::tasksSubmittedCount from DMU_HISTORY_MAIN  {} ", tasksSubmittedCount);
				if (tasksSubmittedCount != 0L) {
					Optional.ofNullable(
							historyMainRepository.findHistoryMainDetailsByStatusScheduler(Constants.SUBMITTED))
							.ifPresent(historyList -> {
								historyList.parallelStream().forEach(historyEntity -> {
									log.info(
											" dmuTaskScheduler => created new thead with name  :: {} :: {} to process request No :: {} at time => ",
											Thread.currentThread().getName(),
											String.valueOf(Thread.currentThread().getId()),
											historyEntity.getRequestNo(),
											dateTimeFormatter.format(LocalDateTime.now()));
									log.info(" => dmuScheduler Task :: Thread {}  Execution Time - {}",
											Thread.currentThread().getName(),
											dateTimeFormatter.format(LocalDateTime.now()));
									demorequestProcessor.processRequest(historyEntity.getRequestNo(), tgtOtherPropOpt);
								});
							});
				}
			}
		} catch (Exception e) {
			log.error(" => DemoScheduler => schedulerConfig exception - {}", ExceptionUtils.getStackTrace(e));
		}
	}
}
