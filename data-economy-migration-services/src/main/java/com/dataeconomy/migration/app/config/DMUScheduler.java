package com.dataeconomy.migration.app.config;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.TGTOtherPropRepository;
import com.dataeconomy.migration.app.scheduler.RequestProcessorClass;
import com.dataeconomy.migration.app.util.Constants;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DMUScheduler {

	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

	private static List<String> STATUS_LIST = new ArrayList<String>() {
		private static final long serialVersionUID = 1L;
		{
			add(Constants.SUBMITTED);
		}
	};

	@Autowired
	private TGTOtherPropRepository propOtherRepository;

	@Autowired
	private DMUHistoryMainRepository historyMainRepository;

	@Autowired
	private RequestProcessorClass requestProcessorClass;

	@Autowired
	private ExecutorService cachedThreadPool;

	@Scheduled(cron = "* */5 * * * ?")
	public void dmuScheduler() {
		try {
			log.info(" => dmuScheduler Task :: Execution Time - {}", dateTimeFormatter.format(LocalDateTime.now()));
			TGTOtherProp tgtOtherPropOpt = propOtherRepository.findById(1L)
					.orElse(TGTOtherProp.builder().parallelUsrRqst(0L).parallelJobs(0L).build());

			Long taskInProgressCount = historyMainRepository.getTaskDetailsCount(Constants.IN_PROGRESS);

			log.info(" => dmuScheduler Task :: taskInProgressCount - {}", taskInProgressCount);
			log.info(" => dmuScheduler Task :: parallelUsersRequest - {}", tgtOtherPropOpt.getParallelUsrRqst());
			log.info(" => dmuScheduler Task :: parallelJobsRequest - {}", tgtOtherPropOpt.getParallelJobs());

			if (taskInProgressCount < tgtOtherPropOpt.getParallelUsrRqst()) {
				Long taskSubmittedCount = historyMainRepository.getTaskDetailsCount(Constants.SUBMITTED);
				log.info(" => dmuScheduler Task :: taskSubmittedCount - {}", taskSubmittedCount);
				if (taskSubmittedCount != 0) {
					if ((tgtOtherPropOpt.getParallelUsrRqst() - taskInProgressCount) > taskSubmittedCount) {
						log.info(
								" => parallel user requests greater than submitted count  - taskSubmittedCount :: {}  ",
								taskSubmittedCount);
						List<DMUHistoryMain> historyMainList = historyMainRepository
								.findHistoryMainDetailsByStatus(STATUS_LIST);
						if (historyMainList != null && historyMainList.size() > 0) {
							ArrayList<Future> futureList = Lists.newArrayList();
							historyMainList.stream().limit(taskSubmittedCount).forEach(entity -> {
								futureList.add(cachedThreadPool.submit(new Callable<Void>() {
									@Override
									public Void call() throws Exception {
										requestProcessorClass.processRequest(entity.getRequestNo(), tgtOtherPropOpt);
										return null;
									};
								}));
							});

							if (!futureList.isEmpty()) {
								for (int t = 0; t < futureList.size(); t++) {
									try {
										futureList.get(t).get();
									} catch (InterruptedException | ExecutionException e) {
									}
								}
							}
						}
					} else {
						long count = (tgtOtherPropOpt.getParallelUsrRqst() - taskInProgressCount);
						log.info(" => parallel user requests less than submitted count  - {} ", count);
						List<DMUHistoryMain> historyMainList = historyMainRepository
								.findHistoryMainDetailsByStatus(STATUS_LIST);
						if (historyMainList != null && historyMainList.size() > 0) {
							for (int i = 0; i < count; i++) {
								requestProcessorClass.processRequest(historyMainList.get(i).getRequestNo(),
										tgtOtherPropOpt);
							}
						}
					}
				} else {
					log.info(" => There are no submitted records exiting the scheduler - {}", taskSubmittedCount);
				}
			} else if (taskInProgressCount < tgtOtherPropOpt.getParallelUsrRqst()) {
				log.info(" => taskInProgressCount > parallelUserRequest -> so terminated the execution - {}");
			}
		} catch (Exception e) {
			log.info(" => taskInProgressCount > parallelUserRequest -> so terminated the execution - {}",
					ExceptionUtils.getStackTrace(e));
		}

	}

}
