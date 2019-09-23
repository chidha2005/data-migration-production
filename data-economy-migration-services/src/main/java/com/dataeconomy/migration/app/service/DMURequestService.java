package com.dataeconomy.migration.app.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.connection.HDFSConnectionService;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.model.HistoryMainDto;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.repository.DMUHistoryMainRepository;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMURequestService {

	@Autowired
	private DMUHistoryMainRepository dmuHistoryMainRepository;

	@Autowired
	private HDFSConnectionService hdfcConnectionService;

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

	public List<String> getAllRequestDatabases() {
		log.info(" invoked =>  RequestService :: getAllRequestDatabases ");
		try {
			return new JdbcTemplate(hdfcConnectionService.getValidDataSource(Constants.REGULAR))
					.query("SHOW DATABASES", new ResultSetExtractor<List<String>>() {

						@Override
						public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
							List<String> databaseList = new ArrayList<>();
							while (rs.next()) {
								databaseList.add(rs.getString(1));
							}
							return Collections.emptyList();
						}
					});
		} catch (Exception exception) {
			log.info(" Exception occured at RequestService :: getAllRequestDatabases {} ",
					ExceptionUtils.getStackTrace(exception));
			return Collections.emptyList();
		}
	}
}
