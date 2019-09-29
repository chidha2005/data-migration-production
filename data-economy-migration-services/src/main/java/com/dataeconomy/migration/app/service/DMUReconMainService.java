package com.dataeconomy.migration.app.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.DMUReconMainDto;
import com.dataeconomy.migration.app.mysql.entity.DMUReconMain;
import com.dataeconomy.migration.app.mysql.repository.DMUReconMainRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMUReconMainService {

	@Autowired
	DMUReconMainRepository dmuReconMainRepository;

	public List<DMUReconMainDto> getDMUReconMainDetailsList() {
		log.info(" DMUReconMainService :: getDMUReconMainDetailsList ");
		try {
			List<DMUReconMain> reconDetailsList = dmuReconMainRepository
					.findAll(Sort.by(Direction.ASC, "requestedTime"));
			return Optional.ofNullable(reconDetailsList).orElse(new ArrayList<>()).stream().map(reconObj -> {
				return DMUReconMainDto.builder().requestNo(reconObj.getRequestNo()).userId(reconObj.getUserId())
						.requestedTime(reconObj.getRequestedTime()).status(reconObj.getStatus())
						.requestType(reconObj.getRequestType()).reconStartTime(reconObj.getReconStartTime())
						.reconCmpltTime(reconObj.getReconCmpltTime()).build();
			}).collect(Collectors.toList());
		} catch (Exception exception) {
			log.info(" Exception occured at DMUReconMainService :: getDMUReconMainDetailsList {} ",
					ExceptionUtils.getStackTrace(exception));
			return Collections.emptyList();
		}
	}

	public DMUReconMainDto getReconDetailsBySearch(String requestNo) {
		log.info(" DMUReconMainService :: getReconDetailsBySearch ");
		try {
			Optional<DMUReconMain> reconDetailsEntity = dmuReconMainRepository.findById(requestNo);
			if (reconDetailsEntity.isPresent()) {
				DMUReconMain dmuReconMain = reconDetailsEntity.get();
				return DMUReconMainDto.builder()
						.requestNo(dmuReconMain.getRequestNo())
						.userId(dmuReconMain.getUserId())
						.requestedTime(dmuReconMain.getRequestedTime())
						.status(dmuReconMain.getStatus())
						.requestType(dmuReconMain.getRequestType())
						.reconStartTime(dmuReconMain.getReconStartTime())
						.reconCmpltTime(dmuReconMain.getReconCmpltTime()).build();
			}
			return DMUReconMainDto.builder().build();
		} catch (Exception exception) {
			log.info(" Exception occured at DMUReconMainService :: getReconDetailsBySearch {} ",
					ExceptionUtils.getStackTrace(exception));
			return DMUReconMainDto.builder().build();
		}
	}

}
