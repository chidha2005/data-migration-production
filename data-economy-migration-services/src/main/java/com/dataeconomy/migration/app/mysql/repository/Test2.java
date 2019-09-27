package com.dataeconomy.migration.app.mysql.repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;

public class Test2 {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Class.forName("com.cloudera.hive.jdbc41.HS2Driver");
		Connection con = DriverManager.getConnection("jdbc:hive2://18.216.202.239:10000/retaildb", "", "");
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE retaildb.products");
		StringBuilder showTable = new StringBuilder();
		while (rs.next()) {
			showTable.append(rs.getString(1));
			showTable.append(" ");
		}
		System.out.println(" showTable=>>>>>>>>>>>" + showTable.toString());
		System.out.println(StringUtils.substring(showTable.toString(), showTable.toString().indexOf("LOCATION") + 7,
				showTable.toString().indexOf("TBLPROPERTIES") - 1).replaceAll("'", ""));
		con.close();
	}
}
