package com.dataeconomy.migration.app.util;

public final class Constants {

	private Constants() {
		super();
	}

	public static final String HIVE = "HIVE";
	public static final String IMPALA = "IMPALA";
	public static final String PROVIDE = "PROVIDE";
	public static final String DIRECT_LC = "DIRECT LC";
	public static final String ASSUME = "Assume";
	public static final String ASSUME_SAML = "AssumeSAML";
	public static final String FEDERATED = "Federated";
	public static final String DIRECT_SC = "DIRECT SC";
	public static final String CLIENT_REGION = "us-east-2";
	public static final String YES = "Y";
	public static final String UNSECURED = "UnSecured";
	public static final String LDAP = "ldap";
	public static final String KERBEROS = "kerberos";
	public static final String IN_PROGRESS = "In Progress";
	public static final String FAILED = "Failed";
	public static final String HDFS_DELIMETER = "-";
	public static final String HDFS_LOCATION = "LOCATION";
	public static final String NO = "N";
	public static final String NEW_SCENARIO = "New Scenario";
	public static final String UNKNOWN_CASE = "Unknown Case";
	public static final String SPARK = "spark";
	public static final String AWS_LC = "AWS_LC";
	public static final String AWS_SC = "AWS_SC";
	public static final String AWS_HDFS = "AWS_HDFS";
	public static final String AWS_FEDERATED_USER = "AWS_FEDERATED_USER";
	public static final String AWS_ASSUME_ROLE = "AWS_ASSUME_ROLE";
	public static final String END_POINT_EAST2 = "sts.us-east-2.amazonaws.com";

	public static final String HIVE_CONN_POOL = "HiveConnectionPool";
	public static final String IMPALA_CONN_POOL = "ImpalaHikariConfigPool";
	public static final String SPARK_CONN_POOL = "SparkHikariConfigPool";
	public static final String HIVE_DRIVER_CLASS_NAME = "com.cloudera.hive.jdbc41.HS2Driver";
	public static final String REGULAR = "REGULAR";
	public static final String LARGEQUERY = "LARGEQUERY";
	public static final String MEDIUMQUERY = "MEDIUMQUERY";
	public static final String SMALLQUERY = "SMALLQUERY";
	public static final String DEFAULT_HIVE_POOL = "DefaultHiveConnectionPool";
	public static final String IMPALA_DRIVER_CLASS_NAME = "com.cloudera.impala.jdbc41.DataSource";
	public static final String SUBMITTED = "Submitted";
	public static final String NOT_STARTED = "Not Started";
	public static final String SECURED = "SCRD";

	public static final String AWS_TO_S3 = "AWS_TO_S3";
	public static final String HDFS = "HDFS";
	public static final String TARGET_FILE_PROPS = "TARGET_FILE_PROPS";
	public static final String OTHER_PROPS = "OTHER_PROPS";

	public static final String SOURCE = "SOURCE";
	public static final String TEXT = "TEXT";
	public static final String SEQUENCE = "SEQUENCE";
	public static final String RECORD_COLUMNAR = "RECORD_COLUMNAR";
	public static final String AVRO = "AVRO";
	public static final String ORC = "ORC";
	public static final String PARQUET = "PARQUET";
	public static final String UN_COMPRESSED = "UN_COMPRESSED";
	public static final String GZIP = "GZIP";
	public static final String SRC_COMPRESSION = "SRC_COMPRESSION";
	public static final String STORED_AS = "STORED_AS";
	public static final String DIRECT_HDFS = "DIRHDFS";

}