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

import com.dataeconomy.migration.app.model.DMUReconDetailDto;
import com.dataeconomy.migration.app.mysql.entity.DMUReconDetail;
import com.dataeconomy.migration.app.mysql.repository.DMUReconDetailRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMUReconDetailService {

	@Autowired
	private DMUReconDetailRepository dmuReconDetailRepository;

	public List<DMUReconDetailDto> getDMUReconDetailsList() {
		log.info(" DMUReconDetailService :: getDMUReconMainDetailsList ");
		try {
			List<DMUReconDetail> reconDetailsList = dmuReconDetailRepository
					.findAll(Sort.by(Direction.ASC, "dmuHIstoryDetailPK.srNo"));
			return Optional.ofNullable(reconDetailsList).orElse(new ArrayList<>()).stream().map(reconObj -> {
				return DMUReconDetailDto.builder().srNo(reconObj.getDmuHIstoryDetailPK().getSrNo())
						.filterCondition(reconObj.getFilterCondition()).schemaName(reconObj.getSchemaName())
						.tableName(reconObj.getTableName()).targetS3Bucket(reconObj.getTargetS3Bucket())
						.incrementalFlag(reconObj.getIncrementalFlag())
						.incrementalColumn(reconObj.getIncrementalColumn()).sourceCount(reconObj.getSourceCount())
						.targetCount(reconObj.getTargetCount()).status(reconObj.getStatus()).build();
			}).collect(Collectors.toList());
		} catch (Exception exception) {
			log.info(" Exception occured at DMUReconDetailService :: getDMUReconDetailsList {} ",
					ExceptionUtils.getStackTrace(exception));
			return Collections.emptyList();
		}
	}

	public DMUReconDetailDto getReconDetailsBySearch(String requestNo) {
		log.info(" DMUReconDetailService :: getReconDetailsBySearch ");
		try {
			Optional<DMUReconDetail> reconDetailsEntity = dmuReconDetailRepository.findById(requestNo);
			if (reconDetailsEntity.isPresent()) {
				DMUReconDetail dmuReconDetail = reconDetailsEntity.get();
				return DMUReconDetailDto.builder().srNo(dmuReconDetail.getDmuHIstoryDetailPK().getSrNo())
						.filterCondition(dmuReconDetail.getFilterCondition()).schemaName(dmuReconDetail.getSchemaName())
						.tableName(dmuReconDetail.getTableName()).targetS3Bucket(dmuReconDetail.getTargetS3Bucket())
						.incrementalFlag(dmuReconDetail.getIncrementalFlag())
						.incrementalColumn(dmuReconDetail.getIncrementalColumn())
						.sourceCount(dmuReconDetail.getSourceCount()).targetCount(dmuReconDetail.getTargetCount())
						.status(dmuReconDetail.getStatus()).build();
			}
			return DMUReconDetailDto.builder().build();
		} catch (Exception exception) {
			log.info(" Exception occured at DMUReconDetailService :: getReconDetailsBySearch {} ",
					ExceptionUtils.getStackTrace(exception));
			return DMUReconDetailDto.builder().build();
		}
	}

}
