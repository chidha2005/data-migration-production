package com.dataeconomy.migration.app.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.dataeconomy.migration.app.mysql.entity.DMUHistoryDetail;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.HistoryDetailRepository;
import com.dataeconomy.migration.app.util.Constants;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@Scope("prototype")
public class RequestProcessorClass {

	@Autowired
	private HistoryDetailRepository historyDetailRepository;

	@Autowired
	private ExecutorService requestProcessorThread;

	@Autowired
	private TableCopySchedulerClass tableCopySchedulerClass;

	@Autowired
	private DMUHistoryMainRepository historyMainRepository;

	@Transactional
	public void processRequest(String requestNo, TGTOtherProp tgtOtherPropOpt) {
		try {
			log.info("RequestProcessorClass invoked with requestNo : {}", requestNo);
			historyMainRepository.updateForRequestNo(Constants.IN_PROGRESS, requestNo);

			List<DMUHistoryDetail> historyDetailsList = historyDetailRepository
					.findHistoryDetailsByRequestNumber(requestNo);

			log.info("RequestProcessorClass invoked with requestNo : {}", requestNo);

			if (historyDetailsList != null && historyDetailsList.size() > 0) {
				Long noOfJobs = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
						Constants.SUBMITTED);
				log.info(" => RequestProcessorClass ::processRequest :: noOfJobs - {}", noOfJobs);

				if (noOfJobs > 0) {
					while (noOfJobs == 0) {
						List<DMUHistoryDetail> inProgressCountTempList = historyDetailRepository
								.findHistoryDetailsByRequestNoAndStatusList(requestNo, Constants.IN_PROGRESS);
						log.info("RequestProcessorClass :: noOfJobs  :: inProgressCountTempList  {}",
								inProgressCountTempList);
						int inProgressCountTemp = 0;
						if (inProgressCountTempList != null) {
							inProgressCountTemp = inProgressCountTempList.size();
						}
						log.info("RequestProcessorClass :: noOfJobs  :: inProgressCountTemp  {}", inProgressCountTemp);
						if (inProgressCountTemp < tgtOtherPropOpt.getParallelJobs()) {
							log.info(
									"RequestProcessorClass :: noOfJobs  :: inProgressCountTemp < tgtOtherPropOpt.getParallelJobs()  {}",
									inProgressCountTemp < tgtOtherPropOpt.getParallelJobs());
							long noOfJobsTempCount = (tgtOtherPropOpt.getParallelJobs() - inProgressCountTemp);
							log.info("RequestProcessorClass :: noOfJobs  :: noOfJobsTempCount  {}", noOfJobsTempCount);
							if (noOfJobsTempCount > noOfJobs) {
								log.info(
										"RequestProcessorClass :: noOfJobs  :: inProgressCountTemp < noOfJobsTempCount > noOfJobs {}",
										noOfJobsTempCount > noOfJobs);
								List<DMUHistoryDetail> dmuHistoryDetailList = historyDetailRepository
										.findHistoryDetailsByRequestNoAndStatusList(requestNo, Constants.SUBMITTED);
								ArrayList<Future<?>> futureList = Lists.newArrayList();

								dmuHistoryDetailList.stream().limit(noOfJobsTempCount).forEach(entity -> {
									futureList.add(requestProcessorThread.submit(new Callable<Void>() {
										@Override
										public Void call() throws Exception {
											tableCopySchedulerClass.processTableCopy(
													entity.getDmuHIstoryDetailPK().getRequestNo(),
													entity.getDmuHIstoryDetailPK().getSrNo());
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
									--noOfJobs;
								}
							} else {
								Long inprogressCountJobs = (tgtOtherPropOpt.getParallelJobs() - inProgressCountTemp);
								ArrayList<Future> futureList = Lists.newArrayList();
								List<DMUHistoryDetail> dmuHistoryDetailList = historyDetailRepository
										.findHistoryDetailsByRequestNoAndStatusList(requestNo, Constants.SUBMITTED);
								for (int l = 0; l < inprogressCountJobs; l++) {
									futureList.add(requestProcessorThread.submit(() -> {
										tableCopySchedulerClass.processTableCopy(
												dmuHistoryDetailList.get(0).getDmuHIstoryDetailPK().getRequestNo(),
												dmuHistoryDetailList.get(0).getDmuHIstoryDetailPK().getSrNo());
									}));
								}

								if (!futureList.isEmpty()) {
									for (int t = 0; t < futureList.size(); t++) {
										try {
											futureList.get(0).get();
										} catch (InterruptedException | ExecutionException e) {
										}
									}
									noOfJobs = (noOfJobs - inprogressCountJobs);
								}
							}
						} else {
							TimeUnit.MINUTES.sleep(1);
						}
					}

					Long inProgressCountAfterWhileLoop = historyDetailRepository
							.findHistoryDetailsByRequestNoAndStatus(requestNo, Constants.IN_PROGRESS);
					while (inProgressCountAfterWhileLoop == 0) {
						TimeUnit.MINUTES.sleep(5);
						inProgressCountAfterWhileLoop = historyDetailRepository
								.findHistoryDetailsByRequestNoAndStatus(requestNo, Constants.IN_PROGRESS);
					}

					Long failedCount = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
							Constants.FAILED);
					DMUHistoryMain historyMainEntity = historyMainRepository.getDMUHistoryMainBySrNo(requestNo);
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

				} else {
					log.info(" => requestprocessor Task :: no jobs to process ");
					Long failedCount = historyDetailRepository.findHistoryDetailsByRequestNoAndStatus(requestNo,
							Constants.FAILED);
					DMUHistoryMain historyMainEntity = historyMainRepository.getDMUHistoryMainBySrNo(requestNo);
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
				}
			}

		} catch (Exception exception) {
			log.info(" => RequestProcessorClass :: processRequest:: exception :: {} ",
					ExceptionUtils.getStackTrace(exception));
		}
	}

}
