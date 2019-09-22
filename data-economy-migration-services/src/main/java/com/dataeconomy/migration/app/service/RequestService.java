package com.dataeconomy.migration.app.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class RequestService {

	@Autowired
	private JdbcTemplate hiveJdbcTemplate;

	public List<String> getAllRequestDatabases() {
		log.info(" invoked =>  RequestService :: getAllRequestDatabases ");
		try {
			return hiveJdbcTemplate.query("SHOW DATABASES", new ResultSetExtractor<List<String>>() {

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
