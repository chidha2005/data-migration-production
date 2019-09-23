package com.dataeconomy.migration.app.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.TGTFormatPropDto;
import com.dataeconomy.migration.app.mysql.entity.TGTFormatProp;
import com.dataeconomy.migration.app.mysql.repository.TGTFormatPropRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class TGTFormatPropService {

	@Autowired
	private TGTFormatPropRepository tgtFormatPropRepository;

	public List<TGTFormatPropDto> getAllTGTFormatProp() {
		log.info(" TGTFormatPropService :: getAllTGTFormatProp {} ");
		try {
			List<TGTFormatProp> tgtFormatPropList = tgtFormatPropRepository.findAll();
			return Optional.ofNullable(tgtFormatPropList).orElse(new ArrayList<>()).stream()
					.map(tgtFormatPropEntity -> {
						return TGTFormatPropDto.builder().srNo(tgtFormatPropEntity.getSrNo())
								.srcFormatFlag(tgtFormatPropEntity.getSrcFormatFlag())
								.textFormatFlag(tgtFormatPropEntity.getTextFormatFlag())
								.fieldDelimiter(tgtFormatPropEntity.getFieldDelimiter())
								.sqncFormatFlag(tgtFormatPropEntity.getSqncFormatFlag())
								.rcFormatFlag(tgtFormatPropEntity.getRcFormatFlag())
								.avroFormatFlag(tgtFormatPropEntity.getAvroFormatFlag())
								.orcFormatFlag(tgtFormatPropEntity.getOrcFormatFlag())
								.parquetFormatFlag(tgtFormatPropEntity.getParquetFormatFlag())
								.srcCmprsnFlag(tgtFormatPropEntity.getSrcCmprsnFlag())
								.uncmprsnFlag(tgtFormatPropEntity.getUncmprsnFlag())
								.gzipCmprsnFlag(tgtFormatPropEntity.getGzipCmprsnFlag()).build();
					}).collect(Collectors.toList());

		} catch (Exception exception) {
			log.info(" Exception occured at TGTFormatPropRepository :: getAllTGTFormatProp {} ",
					ExceptionUtils.getStackTrace(exception));
			return Collections.emptyList();
		}
	}

	public TGTFormatPropDto saveTGTFormat(TGTFormatPropDto tgtFormatPropDto) {
		log.info(" TGTFormatPropService :: saveTGTFormat {} ", ObjectUtils.toString(tgtFormatPropDto));
		try {
			TGTFormatProp tgtFormatPropEntity = TGTFormatProp.builder().srNo(tgtFormatPropDto.getSrNo())
					.srcFormatFlag(tgtFormatPropDto.getSrcFormatFlag())
					.textFormatFlag(tgtFormatPropDto.getTextFormatFlag())
					.fieldDelimiter(tgtFormatPropDto.getFieldDelimiter())
					.sqncFormatFlag(tgtFormatPropDto.getSqncFormatFlag())
					.rcFormatFlag(tgtFormatPropDto.getRcFormatFlag())
					.avroFormatFlag(tgtFormatPropDto.getAvroFormatFlag())
					.orcFormatFlag(tgtFormatPropDto.getOrcFormatFlag())
					.parquetFormatFlag(tgtFormatPropDto.getParquetFormatFlag())
					.srcCmprsnFlag(tgtFormatPropDto.getSrcCmprsnFlag()).uncmprsnFlag(tgtFormatPropDto.getUncmprsnFlag())
					.gzipCmprsnFlag(tgtFormatPropDto.getGzipCmprsnFlag()).build();
			tgtFormatPropRepository.save(tgtFormatPropEntity);
		} catch (Exception exception) {
			log.info(" Exception occured at TGTFormatPropRepository :: saveTGTFormat {} ",
					ExceptionUtils.getStackTrace(exception));
		}

		return null;
	}

	public TGTFormatPropDto getAllTGTFormatProp(Long requestNumber) {
		log.info(" TGTFormatPropService :: getAllTGTFormatProp  :: requestNumber :: {} ", requestNumber);
		try {
			Optional<TGTFormatProp> tgtFormatPropOpt = tgtFormatPropRepository.findById(requestNumber);
			if (tgtFormatPropOpt.isPresent()) {
				TGTFormatProp tgtFormatPropEntity = tgtFormatPropOpt.get();
				return TGTFormatPropDto.builder().srNo(tgtFormatPropEntity.getSrNo())
						.srcFormatFlag(tgtFormatPropEntity.getSrcFormatFlag())
						.textFormatFlag(tgtFormatPropEntity.getTextFormatFlag())
						.fieldDelimiter(tgtFormatPropEntity.getFieldDelimiter())
						.sqncFormatFlag(tgtFormatPropEntity.getSqncFormatFlag())
						.rcFormatFlag(tgtFormatPropEntity.getRcFormatFlag())
						.avroFormatFlag(tgtFormatPropEntity.getAvroFormatFlag())
						.orcFormatFlag(tgtFormatPropEntity.getOrcFormatFlag())
						.parquetFormatFlag(tgtFormatPropEntity.getParquetFormatFlag())
						.srcCmprsnFlag(tgtFormatPropEntity.getSrcCmprsnFlag())
						.uncmprsnFlag(tgtFormatPropEntity.getUncmprsnFlag())
						.gzipCmprsnFlag(tgtFormatPropEntity.getGzipCmprsnFlag()).build();
			}
			return TGTFormatPropDto.builder().build();
		} catch (Exception exception) {
			log.info(
					" Exception occured at TGTFormatPropRepository :: getAllTGTFormatProp :: requestNumber :: {} :: exception => {} ",
					requestNumber, ExceptionUtils.getStackTrace(exception));
			return TGTFormatPropDto.builder().build();
		}
	}

}
