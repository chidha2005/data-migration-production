package com.dataeconomy.migration.app.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.TGTOtherPropDto;
import com.dataeconomy.migration.app.mysql.entity.TGTOtherProp;
import com.dataeconomy.migration.app.mysql.repository.TGTOtherPropRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class TGTOtherPropService {

	@Autowired
	private TGTOtherPropRepository tgtOtherPropRepository;

	public List<TGTOtherPropDto> getAllTGTOtherProp() {
		log.info(" TGTFormatPropService :: getAllTGTFormatProp {} ");
		try {
			List<TGTOtherProp> tgtOtherPropList = tgtOtherPropRepository.findAll();
			return Optional.ofNullable(tgtOtherPropList).orElse(new ArrayList<>()).stream().map(tgtOtherPropEntity -> {
				return TGTOtherPropDto.builder().srNo(tgtOtherPropEntity.getSrNo())
						.parallelJobs(tgtOtherPropEntity.getParallelJobs())
						.parallelUsrRqst(tgtOtherPropEntity.getParallelUsrRqst())
						.tempHiveDB(tgtOtherPropEntity.getTempHiveDB()).tempHdfsDir(tgtOtherPropEntity.getTempHdfsDir())
						.tokenizationInd(tgtOtherPropEntity.getTokenizationInd())
						.ptgyDirPath(tgtOtherPropEntity.getPtgyDirPath())
						.hdfsEdgeNode(tgtOtherPropEntity.getHdfsEdgeNode())
						.hdfsPemLocation(tgtOtherPropEntity.getHdfsPemLocation()).build();
			}).collect(Collectors.toList());
		} catch (Exception exception) {
			log.info(" Exception occured at TGTOtherPropService :: getAllTGTOtherProp {} ",
					ExceptionUtils.getStackTrace(exception));
			return Collections.emptyList();
		}
	}

	public TGTOtherPropDto getAllTGTOtherProp(Long requestNumber) {
		log.info(" TGTFormatPropService :: getAllTGTFormatProp {} ");
		try {
			Optional<TGTOtherProp> tgtOtherPropOpt = tgtOtherPropRepository.findById(requestNumber);
			if (tgtOtherPropOpt.isPresent()) {
				TGTOtherProp tgtOtherPropEntity = tgtOtherPropOpt.get();
				return TGTOtherPropDto.builder().srNo(tgtOtherPropEntity.getSrNo())
						.parallelJobs(tgtOtherPropEntity.getParallelJobs())
						.parallelUsrRqst(tgtOtherPropEntity.getParallelUsrRqst())
						.tempHiveDB(tgtOtherPropEntity.getTempHiveDB()).tempHdfsDir(tgtOtherPropEntity.getTempHdfsDir())
						.tokenizationInd(tgtOtherPropEntity.getTokenizationInd())
						.ptgyDirPath(tgtOtherPropEntity.getPtgyDirPath())
						.hdfsEdgeNode(tgtOtherPropEntity.getHdfsEdgeNode())
						.hdfsUserName(tgtOtherPropEntity.getHdfsUserName())
						.hdfsPemLocation(tgtOtherPropEntity.getHdfsPemLocation()).build();
			}
			return TGTOtherPropDto.builder().build();
		} catch (Exception exception) {
			log.info(" Exception occured at TGTOtherPropService :: getAllTGTOtherProp {} ",
					ExceptionUtils.getStackTrace(exception));
			return TGTOtherPropDto.builder().build();
		}
	}

	public TGTOtherPropDto saveTGTOther(TGTOtherPropDto tgtOtherPropDto) {
		try {
			TGTOtherProp tgtOtherProp = TGTOtherProp.builder().srNo(tgtOtherPropDto.getSrNo())
					.parallelJobs(tgtOtherPropDto.getParallelJobs())
					.parallelUsrRqst(tgtOtherPropDto.getParallelUsrRqst()).tempHiveDB(tgtOtherPropDto.getTempHiveDB())
					.tempHdfsDir(tgtOtherPropDto.getTempHdfsDir()).tokenizationInd(tgtOtherPropDto.getTokenizationInd())
					.ptgyDirPath(tgtOtherPropDto.getPtgyDirPath()).hdfsEdgeNode(tgtOtherPropDto.getHdfsEdgeNode())
					.hdfsUserName(tgtOtherPropDto.getHdfsUserName())
					.hdfsPemLocation(tgtOtherPropDto.getHdfsPemLocation()).build();
			tgtOtherPropRepository.save(tgtOtherProp);
			return tgtOtherPropDto;
		} catch (Exception exception) {
			log.info(" Exception occured at TGTOtherPropService :: getAllTGTOtherProp {} ",
					ExceptionUtils.getStackTrace(exception));
			return TGTOtherPropDto.builder().build();
		}
	}

}
