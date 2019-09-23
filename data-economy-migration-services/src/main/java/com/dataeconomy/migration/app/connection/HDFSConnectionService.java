package com.dataeconomy.migration.app.connection;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
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
public class HDFSConnectionService {

	@Value("${hs2.datasource.jdbc-url}")
	public String hiveConnUrl;

	@Autowired
	private HiveConnectionService hiveConnectionService;

	@Autowired
	private ImaplaConnectionService imaplaConnectionService;

	@Autowired
	private SparkConnectionService sparkConnectionService;

	@Autowired
	private DMUConnectionPool dmuConnectionPool;

	private DataSource hiveDataSource;

	private DataSource impalaDataSource;

	private DataSource sparkDataSource;

	public void populateDataSourceConfig(ConnectionDto connectionDto) throws Exception {
		if (StringUtils.equalsIgnoreCase(Constants.HIVE, connectionDto.getConnectionType())) {
			Optional<String> hiveConnStringOpt = hiveConnectionService.getHiveConnectionDetails(connectionDto);
			if (hiveConnStringOpt.isPresent()) {
				String hiveConnString = hiveConnStringOpt.get();
				retrieveDataSource(Constants.HIVE_CONN_POOL, Constants.HIVE_DRIVER_CLASS_NAME, hiveConnString);
				log.info(" ConnectionService :: validateConnection :: hiveConnString {}", hiveConnString);
			} else {
				throw new DataMigrationException("Invalid Connection Details for HIVE connection Validation ");
			}
		}
		if (StringUtils.equalsIgnoreCase(Constants.IMPALA, connectionDto.getConnectionType())) {
			Optional<String> impalaConnStringOpt = imaplaConnectionService.getImpalaConnectionDetails(connectionDto);
			if (impalaConnStringOpt.isPresent()) {
				String impalaConnString = impalaConnStringOpt.get();
				retrieveDataSource(Constants.IMPALA, Constants.IMPALA_DRIVER_CLASS_NAME, impalaConnString);
				log.info(" ConnectionService :: validateConnection :: impalaConnString {}", impalaConnString);
			} else {
				throw new DataMigrationException("Invalid Connection Details for IMPALA connection Validation ");
			}
		}
		if (StringUtils.equalsIgnoreCase(Constants.SPARK, connectionDto.getConnectionType())) {
			Optional<String> sparkConnStringOpt = sparkConnectionService.getSparkConnectionDetails(connectionDto);
			if (sparkConnStringOpt.isPresent()) {
				String sparkConnString = sparkConnStringOpt.get();
				log.info(" ConnectionService :: validateConnection :: sparkConnString {}", sparkConnString);
			} else {
				throw new DataMigrationException("Invalid Connection Details for SPARK connection Validation ");
			}
		}
	}

	private void retrieveDataSource(String connPoolName, String hiveDriverClassName, String hiveConnString)
			throws DataMigrationException {
		log.error(
				"called => HDFSConnectionService  :: retrieveDataSource :: connPoolName {} , hiveDriverClassName {} , hiveConnString {} ",
				connPoolName, hiveDriverClassName, hiveConnString);
		try {
			dmuConnectionPool.getDataSourceFromConfig(connPoolName, hiveDriverClassName, hiveConnString);
		} catch (Exception exception) {
			log.error(
					"Exception occured at HDFSConnectionService  :: retrieveDataSource :: connPoolName {} , hiveDriverClassName {} , hiveConnString {} exception \n {} ",
					connPoolName, hiveDriverClassName, hiveConnString);
			throw new DataMigrationException("Unable to retrieve datasource object ");
		}
	}

	public DataSource getValidDataSource(String dataSourceType) {
		log.error("called => HDFSConnectionService  :: getValidDataSource :: dataSourceType  {} ", dataSourceType);
		DataSource dataSource = null;
		try {
			switch (dataSourceType) {
			case Constants.REGULAR:
				dataSource = hiveDataSource;
				break;
			case Constants.LARGEQUERY:
				dataSource = impalaDataSource;
				break;
			case Constants.MEDIUMQUERY:
				dataSource = sparkDataSource;
				break;
			case Constants.SMALLQUERY:
				dataSource = sparkDataSource;
				break;
			default:
				dataSource = hiveDataSource;
			}
		} catch (Exception exception) {
			log.error("Exception while retrieving datasource for given type {} , {} ", dataSourceType,
					ExceptionUtils.getStackTrace(exception));
		}
		if (dataSource == null) {
			dataSource = dmuConnectionPool.getDataSourceFromConfig(Constants.DEFAULT_HIVE_POOL,
					Constants.HIVE_DRIVER_CLASS_NAME, hiveConnUrl);
			hiveDataSource = dataSource;
		}
		return dataSource;
	}

}
