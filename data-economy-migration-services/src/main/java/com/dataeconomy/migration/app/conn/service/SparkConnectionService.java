package com.dataeconomy.migration.app.conn.service;

import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SparkConnectionService {
	// TO DO
	public Optional<String> getSparkConnectionDetails(ConnectionDto connectionDto) throws DataMigrationException {
		try {
			if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.UNSECURED)) {

			} else if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.LDAP)) {

			} else if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.KERBEROS)) {

			} else {

			}
			return Optional.empty();
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionObject :: getImpalaConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Invalid Connection Details for Spark Validation");
		}
	}
}
