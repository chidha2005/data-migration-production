package com.dataeconomy.migration.app.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.conn.service.HiveConnectionService;
import com.dataeconomy.migration.app.conn.service.ImaplaConnectionService;
import com.dataeconomy.migration.app.conn.service.SparkConnectionService;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMUConnectionValidationService {

	@PostConstruct
	public void loadDriverClass() {
		log.info(" DMUConnectionValidationService :: loadDriverClass :: initializing driver classes ");
		try {
			Class.forName(Constants.HIVE_DRIVER_CLASS_NAME).newInstance();
			Class.forName(Constants.IMPALA_DRIVER_CLASS_NAME).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			log.error(
					" Exception occured at DMUConnectionValidationService :: loadDriverClass :: initializing driver classes {} ",
					ExceptionUtils.getStackTrace(e));
		}
	}

	@Autowired
	private HiveConnectionService hiveConnectionService;

	@Autowired
	private ImaplaConnectionService imaplaConnectionService;

	@Autowired
	private SparkConnectionService sparkConnectionService;

	public boolean validateConnectionDetails(ConnectionDto connectionDto) throws Exception {
		if (StringUtils.equalsIgnoreCase(Constants.HIVE, connectionDto.getConnectionType())) {
			Optional<String> hiveConnStringOpt = hiveConnectionService.getHiveConnectionDetails(connectionDto);
			if (hiveConnStringOpt.isPresent()) {
				String hiveConnString = hiveConnStringOpt.get();
				log.info(" ConnectionService :: validateConnection :: hiveConnString {}", hiveConnString);
				if (!validateConnection(hiveConnString)) {
					throw new DataMigrationException("Invalid Hive Connection Details");
				}
				return true;
			} else {
				throw new DataMigrationException("Invalid Connection Details for HIVE connection Validation ");
			}
		}
		if (StringUtils.equalsIgnoreCase(Constants.IMPALA, connectionDto.getConnectionType())) {
			Optional<String> impalaConnStringOpt = imaplaConnectionService.getImpalaConnectionDetails(connectionDto);
			if (impalaConnStringOpt.isPresent()) {
				String impalaConnString = impalaConnStringOpt.get();
				log.info(" ConnectionService :: validateConnection :: impalaConnString {}", impalaConnString);
				if (!validateConnection(impalaConnString)) {
					throw new DataMigrationException("Invalid Impala Connection Details");
				}
				return true;
			} else {
				throw new DataMigrationException("Invalid Connection Details for IMPALA connection Validation ");
			}
		}
		if (StringUtils.equalsIgnoreCase(Constants.SPARK, connectionDto.getConnectionType())) {
			Optional<String> sparkConnStringOpt = sparkConnectionService.getSparkConnectionDetails(connectionDto);
			if (sparkConnStringOpt.isPresent()) {
				String sparkConnString = sparkConnStringOpt.get();
				log.info(" ConnectionService :: validateConnection :: sparkConnString {}", sparkConnString);
				if (!validateConnection(sparkConnString)) {
					throw new DataMigrationException("Invalid Spark Connection Details");
				}
				return true;
			} else {
				throw new DataMigrationException("Invalid Connection Details for SPARK connection Validation ");
			}
		}
		return false;
	}

	private boolean validateConnection(String validateConnString) {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(validateConnString);
			log.info(" ConnectionService :: validateConnection :: connection {}", connection);
			log.info(" ConnectionService :: validateConnection :: validateConnString {}", validateConnString);
			return (connection != null && connection.isValid(5));
		} catch (Exception exception) {
			log.error(" Exception occured at DMUConnectionValidationService :: validateConnection {} ",
					ExceptionUtils.getStackTrace(exception));
			return false;
		} finally {
			DbUtils.closeQuietly(connection);
		}

	}
}
