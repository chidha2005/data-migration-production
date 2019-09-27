package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;

import javax.persistence.LockModeType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUHistoryMain;
import com.dataeconomy.migration.app.mysql.entity.ReconAndRequestCountProjection;

@Repository
public interface DMUHistoryMainRepository extends JpaRepository<DMUHistoryMain, String> {

	@Query("select history from DMUHistoryMain history where status in :ids ORDER BY requestedTime DESC")
	List<DMUHistoryMain> findHistoryMainDetailsByStatus(@Param("ids") List<String> inventoryIdList);

	@Query("select history from DMUHistoryMain history where status = :status")
	List<DMUHistoryMain> findHistoryMainDetailsByStatusScheduler(@Param("status") String status);

	@Query("select new com.dataeconomy.migration.app.mysql.entity.ReconAndRequestCountProjection(v.status , count(v) as cnt) from DMUHistoryMain v group by v.status")
	public List<ReconAndRequestCountProjection> findReconHistoryStatusCount();

	@Query("SELECT COUNT(u) FROM DMUHistoryMain u WHERE u.status=:statusValue")
	Long getTaskDetailsCount(@Param("statusValue") String statusValue);

	@Modifying
	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query(" UPDATE DMUHistoryMain u SET u.status= :status WHERE u.requestNo = :requestNo")
	void updateForRequestNo(@Param("requestNo") String requestNo, @Param("status") String status);

	@Query(" SELECT u FROM DMUHistoryMain u  WHERE u.requestNo = :requestNo")
	DMUHistoryMain getDMUHistoryMainBySrNo(@Param("requestNo") String requestNo);

	@Query("select history from DMUHistoryMain history where status = :status AND requestNo = :requestNo")
	List<DMUHistoryMain> findHistoryMainDetailsByStatusAndRequestNo(@Param("status") String status,
			@Param("requestNo") String requestNo);

}
