package com.dataeconomy.migration.app.service;

import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.ReconAndRequestStatusDto;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUReconMainRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class HomeService {

	@Autowired
	private DMUReconMainRepository dmuReconMainRepository;

	@Autowired
	private DMUHistoryMainRepository dmuHistoryMainRepository;

	public ReconAndRequestStatusDto getRequestAndReconStatus() {
		log.info(" HomeService :: getRequestAndReconStatus ");
		try {
			Map<String, Long> reconMainCount = dmuReconMainRepository.findReconMainStatusCount();
			Map<String, Long> reconHistoryMainCount = dmuHistoryMainRepository.findReconHistoryStatusCount();
			ReconAndRequestStatusDto reconAndRequestStatusDto = ReconAndRequestStatusDto.builder()
					.reconMainCount(reconMainCount).reconHistoryMainCount(reconHistoryMainCount).build();
			log.info(" HomeService ::   getRequestAndReconStatus  :: reconMainCount :: {} ", reconMainCount);
			log.info(" HomeService ::   getRequestAndReconStatus  :: reconHistoryMainCount :: {} ",
					reconHistoryMainCount);
			log.info(" HomeService ::   getRequestAndReconStatus  :: all count :: {} ", reconAndRequestStatusDto);
			return reconAndRequestStatusDto;
		} catch (Exception exception) {
			log.info(" Exception occured at HomeService :: getRequestAndReconStatus {} ",
					ExceptionUtils.getStackTrace(exception));
			return ReconAndRequestStatusDto.builder().build();
		}

	}

}
