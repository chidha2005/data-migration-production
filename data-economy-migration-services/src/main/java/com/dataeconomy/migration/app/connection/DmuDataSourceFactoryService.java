package com.dataeconomy.migration.app.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.dataeconomy.migration.app.util.DmuConstants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DmuDataSourceFactoryService {

	public static void main(String[] args) throws Exception {
		String str = "jdbc:hive2://34.193.240.243:10000/retaildb";
		System.out.println(validateConnection(str));
	}

	@SuppressWarnings("deprecation")
	private static boolean validateConnection(String validateConnString)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		Class.forName(DmuConstants.HIVE_DRIVER_CLASS_NAME).newInstance();

		try (Connection connection = DriverManager.getConnection(validateConnString);) {
			System.out
					.println(" ConnectionService :: connection validate successfully for url {}" + validateConnString);
			Statement stmt = connection.createStatement();
			ResultSet res = stmt.executeQuery("show tables");
			while (res.next()) {
				System.out.println(res.getString(1));
			}

			HikariDataSource datasource = getDataSourceFromConfig(DmuConstants.HIVE_CONN_POOL,
					DmuConstants.HIVE_DRIVER_CLASS_NAME, validateConnString);

			System.out.println(" dataSource " + datasource);
			return true;
		} catch (Exception exception) {
			System.out.println(" Exception occured at DMUConnectionValidationService :: validateConnection {} "
					+ ExceptionUtils.getStackTrace(exception));
			return false;
		}
	}

	public static HikariDataSource getDataSourceFromConfig(String connectionPoolName, String driverClassName,
			String databaseUrl) {
		System.out.println(
				" ConnectionPool :: getDataSourceFromConfig =>    connectionPoolName {} ,  driverClassName {} , databaseUrl "
						+ connectionPoolName + driverClassName + databaseUrl);

		HikariConfig jdbcConfig = new HikariConfig();
		jdbcConfig.setPoolName(connectionPoolName);
		jdbcConfig.setMaximumPoolSize(20);
		jdbcConfig.setMinimumIdle(5);
		jdbcConfig.setMaxLifetime(2000000);
		jdbcConfig.setConnectionTimeout(30000);
		jdbcConfig.setIdleTimeout(30000);
		jdbcConfig.setJdbcUrl(databaseUrl);
		jdbcConfig.setDriverClassName(driverClassName);

		jdbcConfig.addDataSourceProperty("cachePrepStmts", true);
		jdbcConfig.addDataSourceProperty("prepStmtCacheSize", 256);
		jdbcConfig.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
		jdbcConfig.addDataSourceProperty("useServerPrepStmts", true);

		return new HikariDataSource(jdbcConfig);

	}
}
