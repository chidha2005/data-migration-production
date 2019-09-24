package com.dataeconomy.migration.app.conn.service;

import java.text.MessageFormat;
import java.util.Optional;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class HiveConnectionService {

	@Value("${hive.connection.url}")
	public String hiveConnectionUrl;

	@Value("${hive.connection.unsecured.url}")
	public String hiveConnectionUnSecuredUrl;

	@Value("${hive.connection.ldap.url}")
	public String hiveConnectionLdapUrl;

	@Value("${hive.connection.kerberos.url}")
	public String hiveConnectionkerberosUrl;

	public Optional<String> getHiveConnectionDetails(ConnectionDto connectionDto) throws Exception {
		try {
			log.info(" HiveConnectionService :: getHiveConnectionDetails :: hiveConnString {}",
					ObjectUtils.toString(connectionDto));
			String hiveConnectionString = StringUtils.EMPTY;
			if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.UNSECURED)
					|| StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), "UNSCRD")) {
				hiveConnectionString = MessageFormat.format(hiveConnectionUnSecuredUrl, connectionDto.getHiveHostName(),
						String.valueOf(connectionDto.getHivePortNmbr()));
				log.info(" HiveConnectionService :: getHiveConnectionDetails :: unsecured url {}",
						hiveConnectionString);
			} else if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.SECURED)
					|| StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), "UNSCRD")) {
				if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.LDAP)) {
					hiveConnectionString = MessageFormat.format(hiveConnectionLdapUrl, connectionDto.getHiveHostName(),
							String.valueOf(connectionDto.getHivePortNmbr()), connectionDto.getLdapUserName(),
							connectionDto.getLdapDomain(), connectionDto.getLdapUserPassw());
					log.info(" HiveConnectionService :: getHiveConnectionDetails :: ldap url {}", hiveConnectionString);
				} else if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.KERBEROS)) {
					hiveConnectionString = MessageFormat.format(hiveConnectionkerberosUrl,
							connectionDto.getHiveHostName(), String.valueOf(connectionDto.getHivePortNmbr()),
							connectionDto.getKerberosHostRealm(), connectionDto.getKerberosHostFqdn(),
							connectionDto.getKerberosServiceName());
					log.info(" HiveConnectionService :: getHiveConnectionDetails :: kerberos url {}",
							hiveConnectionString);
				} else {
					log.info(" HiveConnectionService :: getHiveConnectionDetails :: invalid credentials");
					throw new Exception("Not a valid Hive Validation Details!");
				}
			} else {
				log.info(" HiveConnectionService :: getHiveConnectionDetails :: invalid credentials");
				throw new Exception("Not a valid Hive Validation Details!");
			}
			return Optional.ofNullable(hiveConnectionString);
		} catch (

		Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionObject :: getImpalaConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Invalid Connection Details for Hive Validation");
		}
	}

}
