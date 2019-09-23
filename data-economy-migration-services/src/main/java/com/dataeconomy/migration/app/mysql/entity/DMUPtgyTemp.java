package com.dataeconomy.migration.app.mysql.entity;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
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

	@EmbeddedId
	private DMUPtgyPK id;

	@Column(name = "TKNZTN_ENABLED", length = 1, nullable = true)
	private String tknztnEnabled;

	@Column(name = "TKNZTN_FILE_PATH", length = 1000, nullable = true)
	private String tknztnFilePath;
}
