package com.dataeconomy.migration.app.demo;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.dataeconomy.migration.app.mysql.entity.DMUHistoryDetail;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.HistoryDetailRepository;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DemoRequestProcessor {

	@Autowired
	private HistoryDetailRepository historyDetailRepository;

	@Autowired
	private DemoTableCopyProcessor demoTableCopyProcessor;

	@Autowired
	private DMUHistoryMainRepository historyMainRepository;

	ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

//	@Transactional
	public void processRequest(String requestNo, TGTOtherProp tgtOtherPropOpt) {
		try {
			log.info(" updates history main table with staus => 'In Progress' for requestNo {} ", requestNo);
//			historyMainRepository.updateForRequestNo(requestNo, Constants.IN_PROGRESS);

			Optional<DMUHistoryMain> dmuHistoryOpt = historyMainRepository.findById(requestNo);

			if (dmuHistoryOpt.isPresent()) {
				DMUHistoryMain updated = dmuHistoryOpt.get();
				updated.setStatus(Constants.IN_PROGRESS);
				historyMainRepository.save(updated);
			}

			Long numberOfJobs = historyDetailRepository.findHistoryDetailsByRequestNoAndStatusAscOrder(requestNo,
					Constants.SUBMITTED);
			log.info(" DemoRequestProcessor >>  processRequest >> number fo jobs submitted  {} ", numberOfJobs);

			Long inprogressJobs = historyDetailRepository.findHistoryDetailsByRequestNoAndStatusAscOrder(requestNo,
					Constants.IN_PROGRESS);
			log.info(" DemoRequestProcessor >>  processRequest >> number fo jobs inp rogress   {} ", inprogressJobs);

			if (numberOfJobs > 0) {
				log.info(" retrieving the records for requestNo {} from DMU_HISTORY_DTL {} ", requestNo);
				List<DMUHistoryDetail> dmuHistoryDetailList = historyDetailRepository
						.findHistoryDetailsByRequestNoAndStatusList(requestNo, Constants.SUBMITTED);

				if (CollectionUtils.isNotEmpty(dmuHistoryDetailList)) {
					log.info(" retrieved the records for requestNo {} from DMU_HISTORY_DTL {} with count {} ",
							dmuHistoryDetailList.size());
//					dmuHistoryDetailList.parallelStream().limit(tgtOtherPropOpt.getParallelJobs())
					dmuHistoryDetailList.stream().limit(tgtOtherPropOpt.getParallelJobs())
							.forEach(historyDetailEntity -> {
								log.info("Thread : " + Thread.currentThread().getName() + ", value: " + requestNo
										+ " - " + historyDetailEntity.getDmuHIstoryDetailPK().getSrNo());
								demoTableCopyProcessor.processTableCopy(requestNo,
										historyDetailEntity.getDmuHIstoryDetailPK().getSrNo());
							});

					ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
					executor.scheduleAtFixedRate(new Runnable() {

						@Override
						public void run() {
							Long inProgressCount = historyDetailRepository
									.findHistoryDetailsByRequestNoAndStatus(requestNo, Constants.IN_PROGRESS);
							log.info(" At ScheduledExecutorService =>>>>>>>>>>> inProgressCount :: ", inProgressCount);
							if (inProgressCount == 0) {
								Long failedCount = historyDetailRepository
										.findHistoryDetailsByRequestNoAndStatus(requestNo, Constants.FAILED);
								DMUHistoryMain historyMainEntity = historyMainRepository
										.getDMUHistoryMainBySrNo(requestNo);
								log.info(" => dmuScheduler Task :: no jobs to process :: failedCount {} ", failedCount);
								if (historyMainEntity != null) {
									if (failedCount > 0) {
										historyMainEntity.setStatus(Constants.FAILED);
										historyMainRepository.save(historyMainEntity);
										log.info(" => requestprocessor class status {} ", Constants.FAILED);
									} else {
										historyMainEntity.setStatus(Constants.SUCCESS);
										historyMainRepository.save(historyMainEntity);
										log.info(" => requestprocessor class status {} ", Constants.SUCCESS);
									}
								}
								executor.shutdown();
							}
						}
					}, 0, 1, TimeUnit.MINUTES);
				}
			} else {
				log.info(" => requestprocessor Task :: no jobs to process ");
				Long failedCount = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
						Constants.FAILED);
				if (failedCount > 0) {
					DMUHistoryMain historyMainEntity = historyMainRepository.getDMUHistoryMainBySrNo(requestNo);
					log.info(" => dmuScheduler Task :: no jobs to process :: failedCount {} ", failedCount);
					historyMainEntity.setStatus(Constants.FAILED);
					historyMainRepository.save(historyMainEntity);
					log.info(" => requestprocessor class status {} ", Constants.FAILED);
				} else {
					DMUHistoryMain historyMainEntity = historyMainRepository.getDMUHistoryMainBySrNo(requestNo);
					log.info(" => dmuScheduler Task :: no jobs to process :: failedCount {} ", failedCount);
					historyMainEntity.setStatus(Constants.SUCCESS);
					historyMainRepository.save(historyMainEntity);
					log.info(" => requestprocessor class status {} ", Constants.SUCCESS);
				}
			}
		} catch (Exception exceptin) {

		}
	}

}