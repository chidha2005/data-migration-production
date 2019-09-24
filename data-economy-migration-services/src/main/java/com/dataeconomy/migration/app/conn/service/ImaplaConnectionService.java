package com.dataeconomy.migration.app.conn.service;

import java.text.MessageFormat;
import java.util.Optional;

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
public class ImaplaConnectionService {

	@Value("${impala.connection.url}")
	public String impalaConnectionUrl;

	@Value("${impala.connection.unsecured.url}")
	public String impalaConnectionUnSecuredUrl;

	@Value("${impala.connection.ldap.url}")
	public String impalaConnectionLdapUrl;

	@Value("${impala.connection.kerberos.url}")
	public String impalaConnectionkerberosUrl;

	public Optional<String> getImpalaConnectionDetails(ConnectionDto connectionDto) throws DataMigrationException {
		try {
			String impalaConnectionString = StringUtils.EMPTY;
			if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.UNSECURED)
					|| StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), "UNSCRD")) {
				impalaConnectionString = MessageFormat.format(impalaConnectionUnSecuredUrl,
						connectionDto.getImpalaHostName(), String.valueOf(connectionDto.getImpalaPortNmbr()));
			} else if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.SECURED)
					|| StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), "UNSCRD")) {
				impalaConnectionString = MessageFormat.format(impalaConnectionUnSecuredUrl,
						connectionDto.getImpalaHostName(), String.valueOf(connectionDto.getImpalaPortNmbr()));
				if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.LDAP)) {
					impalaConnectionString = MessageFormat.format(impalaConnectionLdapUrl,
							connectionDto.getImpalaHostName(), String.valueOf(connectionDto.getImpalaPortNmbr()),
							connectionDto.getLdapUserName(), connectionDto.getLdapDomain(),
							connectionDto.getLdapUserPassw());
				} else if (StringUtils.equalsIgnoreCase(connectionDto.getAuthenticationType(), Constants.KERBEROS)) {
					impalaConnectionString = MessageFormat.format(impalaConnectionkerberosUrl,
							connectionDto.getImpalaHostName(), String.valueOf(connectionDto.getImpalaPortNmbr()),
							connectionDto.getKerberosHostRealm(), connectionDto.getKerberosHostFqdn(),
							connectionDto.getKerberosServiceName());
				} else {
					throw new Exception("Not a valid Imapla Authentication Details!");
				}
			} else {
				throw new Exception("Not a valid Imapla Authentication Details!");
			}
			return Optional.ofNullable(impalaConnectionString);
		} catch (Exception exception) {
			log.info(" Exception occured at ConnectionService :: getConnectionObject :: getImpalaConnectionDetails {} ",
					ExceptionUtils.getStackTrace(exception));
			throw new DataMigrationException("Invalid Connection Details for Imapla Validation");

		}

	}
}
