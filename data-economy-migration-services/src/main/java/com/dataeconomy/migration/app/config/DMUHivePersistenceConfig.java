package com.dataeconomy.migration.app.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

//@Configuration
public class DMUHivePersistenceConfig {

	@Bean(name = "hiveJdbcDataSource")
	@ConfigurationProperties(prefix = "hs2.datasource")
	public DataSource dataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean(name = "hiveJdbcTemplate")
	public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource") DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}

	/*
	@PostConstruct
	public void init() {
		Connection con = null;
		try {
			Class.forName("com.cloudera.hive.jdbc41.HS2Driver");
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}

		// get connection System.out.println("before trying to connect"); Connection
		try {
			con = DriverManager.getConnection("jdbc:hive2://18.216.202.239:10000/retaildb", "", "");
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SHOW DATABASES");
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}

			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("connected");

		// create statement Statement stmt = con.createStatement();

	} */
}
