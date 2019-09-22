package com.dataeconomy.migration.app.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectionDto {

	String connectionType;

	Long srNo;

	String domain;

	TGTOtherPropDto tgtOtherPropDto;

	TGTFormatPropDto tgtFormatPropDto;

	String hiveCnctnFlag;

	String hiveHostName;

	Long hivePortNmbr;

	String impalaCnctnFlag;

	String impalaHostName;

	Long impalaPortNmbr;

	String sparkCnctnFlag;

	String sqlWhDir;

	String hiveMsUri;

	String credentialStrgType;

	String awsAccessIdLc;

	String awsSecretKeyLc;

	String awsAccessIdSc;

	String awsSecretKeySc;

	String roleArn;

	String principalArn;

	String samlProviderArn;

	String roleSesnName;

	String policyArnMembers;

	String externalId;

	String fdrtdUserName;

	String inlineSesnPolicy;

	Integer duration;

	String ldapUserName;

	String ldapUserPassw;

	String ldapDomain;

	String scCrdntlAccessType;

	String authenticationType;

	String ldapCnctnFlag;

	String kerberosHostRealm;

	String kerberosHostFqdn;

	String kerberosCnctnFlag;

	String textFormatFlag;

	String kerberosServiceName;

	String sslKeystorePath;

}
