package com.dataeconomy.migration.app.mysql.entity;

import java.time.LocalDateTime;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "DMU_USERS")
public class DMUUsers {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id", length = 20, nullable = false)
	private String id;

	@Column(name = "USER_ROLE", length = 20, nullable = false)
	private String userRole;

	@Column(name = "EMAIL_ID", length = 100, nullable = false)
	private String emailid;

	@Column(name = "password", length = 20, nullable = false)
	private String password;

	@Column(name = "USER_NAME", length = 30, nullable = false)
	private String userName;

	@Column(name = "createdBy")
	private String createdBy;

	@Column(name = "created_date")
	private LocalDateTime createdDate;

	@Column(name = "updatedby")
	private String updatedBy;

	@Column(name = "updateddate")
	private LocalDateTime updatedDate;
}
