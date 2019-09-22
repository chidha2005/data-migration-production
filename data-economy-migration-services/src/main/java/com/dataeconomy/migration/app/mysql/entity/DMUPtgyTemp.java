package com.dataeconomy.migration.app.mysql.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "DMU_PTGY_TEMP")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DMUPtgyTemp {

	@Id
	@Column(name = "USER_ID", length = 20, nullable = false)
	private String userId;

	@Column(name = "LABEL_NAME", length = 50, nullable = false)
	private String labelName;

	@Column(name = "TKNZTN_ENABLED", length = 1, nullable = true)
	private String tknztnEnabled;

	@Column(name = "TKNZTN_FILE_PATH", length = 1000, nullable = true)
	private String tknztnFilePath;
}
