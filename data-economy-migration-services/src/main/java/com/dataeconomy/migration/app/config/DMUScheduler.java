package com.dataeconomy.migration.app.config;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.TGTOtherPropRepository;
import com.dataeconomy.migration.app.service.ScriptGenerationService;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DMUScheduler {

	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

	@Autowired
	private ExecutorService cachedThreadPool;;

	@Autowired
	private ScriptGenerationService scriptGenerationService;

	@Autowired
	private TGTOtherPropRepository propOtherRepository;

	@Autowired
	private DMUHistoryMainRepository historyMainRepository;

	private static List<String> STATUS_LIST = new ArrayList<String>() {
		private static final long serialVersionUID = 1L;
		{
			add(Constants.IN_PROGRESS);
			add(Constants.SUBMITTED);
		}
	};

//	@Scheduled(cron = "*/5 * * * * ?")
	public void dmuScheduler() {
		log.info(" => dmuScheduler Task :: Execution Time - {}", dateTimeFormatter.format(LocalDateTime.now()));
		Long parallelUsersRequest = 1L;
		Long parallelJobsRequest = 1L;
		Optional<TGTOtherProp> tgtOtherPropOpt = propOtherRepository.findById(1L);
		if (tgtOtherPropOpt.isPresent()) {
			TGTOtherProp tgtOtherPropObj = tgtOtherPropOpt.get();
			parallelUsersRequest = tgtOtherPropObj.getParallelUsrRqst();
			parallelJobsRequest = tgtOtherPropObj.getParallelJobs();
		}

		List<DMUHistoryMain> historyMainList = historyMainRepository.findHistoryMainDetailsByStatus(STATUS_LIST);
		if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(historyMainList)) {
			Long inprogressCount = historyMainList.stream()
					.filter(historyMain -> StringUtils.equalsIgnoreCase(historyMain.getStatus(), Constants.IN_PROGRESS))
					.count();
			if (inprogressCount != 0 && inprogressCount <= parallelUsersRequest) {
				Long submittedCount = historyMainList.stream().filter(
						historyMain -> StringUtils.equalsIgnoreCase(historyMain.getStatus(), Constants.SUBMITTED))
						.count();
				if (submittedCount != 0 && submittedCount <= parallelJobsRequest) {
					historyMainList.parallelStream().map(historyMainObj -> {
						return cachedThreadPool.submit(() -> {
							scriptGenerationService.proceedScriptGenerationForRequest(historyMainObj.getRequestNo(),
									0L);
						});
					}).collect(Collectors.toList()).parallelStream().forEach(future -> {
						try {
							future.get();
						} catch (InterruptedException | ExecutionException e) {
							log.info(
									" => Exception occured at  dmuScheduler processing request at  - {} for thread {} ",
									dateTimeFormatter.format(LocalDateTime.now()), Thread.currentThread().getName());
						}
					});
				}
			}
		}

	}

}
