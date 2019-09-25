package com.dataeconomy.migration.app.model;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReconAndRequestStatusDto {

	Map<String, Long> reconMainCount;
	Map<String, Long> reconHistoryMainCount;

}
