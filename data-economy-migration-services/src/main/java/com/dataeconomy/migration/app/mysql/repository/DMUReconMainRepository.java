package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUReconMain;
import com.dataeconomy.migration.app.mysql.entity.ReconAndRequestCountProjection;

@Repository
public interface DMUReconMainRepository extends JpaRepository<DMUReconMain, String> {

	@Query("select new com.dataeconomy.migration.app.mysql.entity.ReconAndRequestCountProjection(v.status , count(v) as cnt) from DMUReconMain v group by v.status")
	public List<ReconAndRequestCountProjection> findReconMainStatusCount();

}
