package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;
import java.util.Map;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;

@Repository
public interface DMUHistoryMainRepository extends JpaRepository<DMUHistoryMain, String> {

	@Query("select history from DMUHistoryMain history where status in :ids")
	List<DMUHistoryMain> findHistoryMainDetailsByStatus(@Param("ids") List<String> inventoryIdList);

	@Query("select new map(v.status , count(v) as cnt) from DMUHistoryMain v group by v.status")
	public Map<String, Long> findReconHistoryStatusCount();

}
