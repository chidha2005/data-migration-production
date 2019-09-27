package com.dataeconomy.migration.app.mysql.repository;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Test {

	public static void main(String[] args) {

		String sftpcmd = "  ssh -i /Users/dataeconomy/dmu-user.pem \r\n"
				+ "dmu-user@18.216.202.239 /opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/bin/hadoop \r\n"
				+ "distcp -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider \r\n"
				+ "-Dfs.s3a.access.key=ASIAV6TWR75UHOHPUYNT -Dfs.s3a.secret.key=w1qbuvc51GNxtkx6OyHt3m5MIQ9YfAjb8xQSG0k4 \r\n"
				+ "-Dfs.s3a.session.token=FQoGZXIvYXdzEK7//////////wEaDJUNF7gcDQNyZOMAzyKrAbXofBZVSpJlXAG4KxjtAdF8zCe/4M2i8V62+FKPHAaTFU0oJrdkk/mrG9Chua438obTrv0xNSLPDlAqd8o3yaHx+dxmKJZ/bkfVMuS71eBH4QdX+btT5P5KBTqiqakIUXoa+DxNBY7JUEsreBj6cXhBhi9OvNupGRgb0bdvfg50qXiNPUG+sPSKlpamqqjLyQBB2CN34z8rX7oOune/o5es/JJFFvbTJdoIHiic+rfsBQ== \r\n"
				+ "'hdfs://ip-172-31-20-195.us-east-2.compute.internal:8020/user/testfolder/backkup/products'/* s3a://dmutestbucket/products";

		try {

			/*
			 * Process p; p = Runtime.getRuntime().exec(sftpcmd);
			 * 
			 * InputStreamReader ise = new InputStreamReader(p.getErrorStream());
			 * BufferedReader bre = new BufferedReader(ise); InputStreamReader iso = new
			 * InputStreamReader(p.getInputStream()); BufferedReader bro = new
			 * BufferedReader(iso);
			 * 
			 * String line = null; while ((line = bre.readLine()) != null) { line = line +
			 * line; } while ((line = bro.readLine()) != null) { line = line + line; }
			 * System.out.println("line =>>>>>>>>>" + line); int exitVal = p.waitFor();
			 * System.out.println(" exitVal =>>>> " + exitVal);
			 */
			
			
			
				StringBuilder sb = new StringBuilder();
			
			sb.append(
					"/hadoop distcp -Dfs.s3a.aws.credentials.provider=");
			sb.append("\"");
			sb.append("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
			sb.append("\"");
			sb.append(" -Dfs.s3a.access.key=");
			sb.append("\"");
			System.out.println(sb.toString());
			
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
		
		
		

	}

}
