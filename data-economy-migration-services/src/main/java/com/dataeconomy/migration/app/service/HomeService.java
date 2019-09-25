package com.dataeconomy.migration.app.service;

import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.ReconAndRequestStatusDto;
import com.dataeconomy.migration.app.mysql.entity.ReconAndRequestCountProjection;
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
		ReconAndRequestStatusDto reconAndRequestStatusDto = new ReconAndRequestStatusDto();
		try {

			List<ReconAndRequestCountProjection> reconMainCountList = dmuReconMainRepository.findReconMainStatusCount();
			List<ReconAndRequestCountProjection> reconHistoryMainCountList = dmuHistoryMainRepository
					.findReconHistoryStatusCount();

			if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(reconMainCountList)) {
				reconMainCountList.stream().forEach(reconMainObj -> {
					reconAndRequestStatusDto.getReconMainCount().put(reconMainObj.getStatus(), reconMainObj.getCount());
					reconAndRequestStatusDto.setReconMainTotalCount(
							reconAndRequestStatusDto.getReconMainTotalCount() + reconMainObj.getCount());
				});
			}

			if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(reconHistoryMainCountList)) {
				reconHistoryMainCountList.stream().forEach(reconMainObj -> {
					reconAndRequestStatusDto.getReconHistoryMainCount().put(reconMainObj.getStatus(),
							reconMainObj.getCount());
					reconAndRequestStatusDto.setReconHistoryMainTotalCount(
							reconAndRequestStatusDto.getReconHistoryMainTotalCount() + reconMainObj.getCount());
				});
			}

			log.info(" HomeService ::   getRequestAndReconStatus  :: all count :: {} ", reconAndRequestStatusDto);
			return reconAndRequestStatusDto;
		} catch (Exception exception) {
			log.info(" Exception occured at HomeService :: getRequestAndReconStatus {} ",
					ExceptionUtils.getStackTrace(exception));
			return reconAndRequestStatusDto;
		}

	}

}
