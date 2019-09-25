package com.dataeconomy.migration.app.mysql.entity;

public class ReconAndRequestCountProjection {

	private String status;
	private Long count;

	public ReconAndRequestCountProjection(String status, Long count) {
		super();
		this.status = status;
		this.count = count;
	}

	public ReconAndRequestCountProjection() {
		super();
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "ReconAndRequestCountProjection [status=" + status + ", count=" + count + "]";
	}

}
