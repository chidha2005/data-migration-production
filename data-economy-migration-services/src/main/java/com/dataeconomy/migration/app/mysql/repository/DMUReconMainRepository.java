package com.dataeconomy.migration.app.mysql.repository;

import java.util.Map;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUReconMain;

@Repository
public interface DMUReconMainRepository extends JpaRepository<DMUReconMain, String> {

	@Query("select new map(v.status, count(v) as cnt) from DMUReconMain v group by v.status")
	public Map<String, Long> findReconMainStatusCount();

}
