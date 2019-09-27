package com.dataeconomy.migration.app.connection;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.conn.service.HiveConnectionService;
import com.dataeconomy.migration.app.conn.service.ImaplaConnectionService;
import com.dataeconomy.migration.app.conn.service.SparkConnectionService;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.util.Constants;
import com.dataeconomy.migration.app.util.DMUHelperService;
import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariDataSource;

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

	@Autowired
	private DMUHelperService dmuHelperService;

	private Map<String, DataSource> dataSourceMap = Collections.synchronizedMap(Maps.newHashMap());

	@PostConstruct
	public void initDataSourceConfig() {
		try {
			log.info(" initializing datasource connections while server start up ");
			Class.forName(Constants.HIVE_DRIVER_CLASS_NAME).newInstance();
			Class.forName(Constants.IMPALA_DRIVER_CLASS_NAME).newInstance();
			ConnectionDto connectionDto = ConnectionDto.builder().build();
			dmuHelperService.populateDMUAuthenticationProperties(connectionDto);
			dmuHelperService.populateDMUHdfsProperties(connectionDto);
			populateDataSourceConfigAtServerStarts(connectionDto);
		} catch (Exception exception) {
			log.error("Exception while creating datasources while server up {} ",
					ExceptionUtils.getStackTrace(exception));
		}
	}

	public void populateDataSourceConfig(ConnectionDto connectionDto) throws Exception {
		if (StringUtils.equalsIgnoreCase(Constants.HIVE, connectionDto.getConnectionType())) {
			Optional<String> hiveConnStringOpt = hiveConnectionService.getHiveConnectionDetails(connectionDto);
			if (hiveConnStringOpt.isPresent()) {
				String hiveConnString = hiveConnStringOpt.get();
				DataSource hiveDataSource = retrieveDataSource(Constants.HIVE_CONN_POOL,
						Constants.HIVE_DRIVER_CLASS_NAME, hiveConnString);
				dataSourceMap.put(Constants.REGULAR, hiveDataSource);
				log.info(" ConnectionService :: validateConnection :: hiveConnString {}", hiveConnString);
			} else {
				throw new DataMigrationException("Invalid Connection Details for HIVE connection Validation ");
			}
		}
		if (StringUtils.equalsIgnoreCase(Constants.IMPALA, connectionDto.getConnectionType())) {
			Optional<String> impalaConnStringOpt = imaplaConnectionService.getImpalaConnectionDetails(connectionDto);
			if (impalaConnStringOpt.isPresent()) {
				String impalaConnString = impalaConnStringOpt.get();
				DataSource impalaDataSource = retrieveDataSource(Constants.IMPALA, Constants.IMPALA_DRIVER_CLASS_NAME,
						impalaConnString);
				dataSourceMap.put(Constants.LARGEQUERY, impalaDataSource);
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

	public void populateDataSourceConfigAtServerStarts(ConnectionDto connectionDto) throws Exception {
		if (connectionDto.isHiveConnEnabled()) {
			Optional<String> hiveConnStringOpt = hiveConnectionService.getHiveConnectionDetails(connectionDto);
			if (hiveConnStringOpt.isPresent()) {
				String hiveConnString = hiveConnStringOpt.get();
				DataSource hiveDataSource = retrieveDataSource(Constants.HIVE_CONN_POOL,
						Constants.HIVE_DRIVER_CLASS_NAME, hiveConnString);
				dataSourceMap.put(Constants.REGULAR, hiveDataSource);
				log.info(" ConnectionService :: validateConnection :: hiveConnString {}", hiveConnString);
			} else {
				throw new DataMigrationException("Invalid Connection Details for HIVE connection Validation ");
			}
		}
		if (connectionDto.isImpalaConnEnabled()) {
			Optional<String> impalaConnStringOpt = imaplaConnectionService.getImpalaConnectionDetails(connectionDto);
			if (impalaConnStringOpt.isPresent()) {
				String impalaConnString = impalaConnStringOpt.get();
				DataSource impalaDataSource = retrieveDataSource(Constants.IMPALA, Constants.IMPALA_DRIVER_CLASS_NAME,
						impalaConnString);
				dataSourceMap.put(Constants.LARGEQUERY, impalaDataSource);
				log.info(" ConnectionService :: validateConnection :: impalaConnString {}", impalaConnString);
			} else {
				throw new DataMigrationException("Invalid Connection Details for IMPALA connection Validation ");
			}
		}
		if (connectionDto.isSparkConnEnabled()) {
			Optional<String> sparkConnStringOpt = sparkConnectionService.getSparkConnectionDetails(connectionDto);
			if (sparkConnStringOpt.isPresent()) {
				String sparkConnString = sparkConnStringOpt.get();
				log.info(" ConnectionService :: validateConnection :: sparkConnString {}", sparkConnString);
			} else {
				throw new DataMigrationException("Invalid Connection Details for SPARK connection Validation ");
			}
		}
	}

	private HikariDataSource retrieveDataSource(String connPoolName, String hiveDriverClassName, String hiveConnString)
			throws DataMigrationException {
		log.error(
				"called => HDFSConnectionService  :: retrieveDataSource :: connPoolName {} , hiveDriverClassName {} , hiveConnString {} ",
				connPoolName, hiveDriverClassName, hiveConnString);
		try {
			return dmuConnectionPool.getDataSourceFromConfig(connPoolName, hiveDriverClassName, hiveConnString);
		} catch (Exception exception) {
			log.error(
					"Exception occured at HDFSConnectionService  :: retrieveDataSource :: connPoolName {} , hiveDriverClassName {} , hiveConnString {} exception \n {} ",
					connPoolName, hiveDriverClassName, hiveConnString);
			throw new DataMigrationException("Unable to retrieve datasource object ");
		}
	}

	public synchronized DataSource getValidDataSource(String dataSourceType) throws DataMigrationException {
		log.error("called => HDFSConnectionService  :: getValidDataSource :: dataSourceType  {} ", dataSourceType);
		try {
			if (dataSourceMap.get(dataSourceType) != null) {
				return dataSourceMap.get(dataSourceType);
			} else {
				DataSource dataSource = dmuConnectionPool.getDataSourceFromConfig(Constants.DEFAULT_HIVE_POOL,
						Constants.HIVE_DRIVER_CLASS_NAME, hiveConnUrl);
				dataSourceMap.put(Constants.REGULAR, dataSource);
				return dataSource;
			}
		} catch (Exception exception) {
			log.error("Exception while retrieving datasource for given type {} , {} ", dataSourceType,
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException(" DataSource config not found");
		}
	}

}
