package com.dataeconomy.migration.app.model;

import java.util.Map;

import com.google.common.collect.Maps;

public class ReconAndRequestStatusDto {

	Map<String, Long> reconMainCount = Maps.newLinkedHashMap();
	Map<String, Long> reconHistoryMainCount = Maps.newLinkedHashMap();

	public Map<String, Long> getReconMainCount() {
		return reconMainCount;
	}

	public void setReconMainCount(Map<String, Long> reconMainCount) {
		this.reconMainCount = reconMainCount;
	}

	public Map<String, Long> getReconHistoryMainCount() {
		return reconHistoryMainCount;
	}

	public void setReconHistoryMainCount(Map<String, Long> reconHistoryMainCount) {
		this.reconHistoryMainCount = reconHistoryMainCount;
	}

	@Override
	public String toString() {
		return "ReconAndRequestStatusDto [reconMainCount=" + reconMainCount + ", reconHistoryMainCount="
				+ reconHistoryMainCount + "]";
	}

	public ReconAndRequestStatusDto() {
		super();
	}

}
