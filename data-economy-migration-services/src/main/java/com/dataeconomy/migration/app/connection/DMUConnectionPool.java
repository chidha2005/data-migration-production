package com.dataeconomy.migration.app.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.stereotype.Service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMUConnectionPool {

	public HikariDataSource getDataSourceFromConfig(String connectionPoolName, String driverClassName,
			String databaseUrl) {
		log.info(
				" ConnectionPool :: getDataSourceFromConfig =>    connectionPoolName {} ,  driverClassName {} , databaseUrl ",
				connectionPoolName, driverClassName, databaseUrl);

		HikariConfig jdbcConfig = new HikariConfig();
		jdbcConfig.setPoolName(connectionPoolName);
		jdbcConfig.setMaximumPoolSize(20);
		jdbcConfig.setMinimumIdle(10);
		jdbcConfig.setMaxLifetime(86430000);
		jdbcConfig.setConnectionTimeout(300000);
		jdbcConfig.setIdleTimeout(30000);
		jdbcConfig.setJdbcUrl(databaseUrl);
		jdbcConfig.setDriverClassName(driverClassName);

		jdbcConfig.addDataSourceProperty("cachePrepStmts", true);
		jdbcConfig.addDataSourceProperty("prepStmtCacheSize", 256);
		jdbcConfig.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
		jdbcConfig.addDataSourceProperty("useServerPrepStmts", true);
		jdbcConfig.addDataSourceProperty("useLocalSessionState", "true");
		jdbcConfig.addDataSourceProperty("useLocalTransactionState", "true");
		jdbcConfig.addDataSourceProperty("rewriteBatchedStatements", "true");
		jdbcConfig.addDataSourceProperty("cacheResultSetMetadata", "true");
		jdbcConfig.addDataSourceProperty("cacheServerConfiguration", "true");
		jdbcConfig.addDataSourceProperty("elideSetAutoCommits", "true");
		jdbcConfig.addDataSourceProperty("maintainTimeStats", "false");

		return new HikariDataSource(jdbcConfig);

	}

	public static void main(String[] args) throws SQLException {

		try {

			//HikariDataSource dataSource = new DMUConnectionPool().getDataSourceFromConfig("hive",
				//	DmuConstants.HIVE_DRIVER_CLASS_NAME,
				//	"jdbc:hive2://3.15.70.125:10000/default/;transportMode=http;httpPath=/hive2");
			//Connection conn = dataSource.getConnection();

			 Connection conn = DriverManager .getConnection(
			 "jdbc:hive2://3.15.70.125:10000/;transportMode=http;httpPath=/hive2");
			Statement stmt = conn.createStatement();
			ResultSet res = stmt.executeQuery("SHOW DATABASES");
			while (res.next()) {
				System.out.println(">>>>>>>>>>> " + res.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
