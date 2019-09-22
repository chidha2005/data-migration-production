package com.dataeconomy.migration.app.service;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.model.HistoryMainDto;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMURequestService {

	@Autowired
	private DMUHistoryMainRepository dmuHistoryMainRepository;

	public List<ConnectionDto> saveRequest(HistoryMainDto historyMainDto) {
		log.info("DMURequestService :: saveRequest :: historyMainDto {} ",
				ObjectUtils.toString(historyMainDto, "Invalid details"));
		try {
			DMUHistoryMain dmuHistoryMain = DMUHistoryMain.builder().requestNo(historyMainDto.getRequestNo())
					.status(historyMainDto.getStatus()).requestedTime(LocalDateTime.now())
					.requestType(historyMainDto.getRequestType()).tknztnEnabled(historyMainDto.getTknztnEnabled())
					.tknztnFilePath(historyMainDto.getTknztnFilePath()).build();
			dmuHistoryMainRepository.save(dmuHistoryMain);
		} catch (Exception exception) {
			log.error(" Exception occured at DMURequestService :: saveRequest :: {} ",
					ExceptionUtils.getStackTrace(exception));
		}

		return null;
	}

}
