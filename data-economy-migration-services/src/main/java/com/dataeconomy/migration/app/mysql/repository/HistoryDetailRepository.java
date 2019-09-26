package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUHIstoryDetailPK;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryDetail;

@Repository
public interface HistoryDetailRepository extends JpaRepository<DMUHistoryDetail, DMUHIstoryDetailPK> {

	@Query("SELECT detail from DMUHistoryDetail detail where detail.dmuHIstoryDetailPK.requestNo = :requestNo")
	List<DMUHistoryDetail> findHistoryDetailsByRequestNumber(@Param("requestNo") String requestNo);

	@Query("SELECT detail from DMUHistoryDetail detail where detail.dmuHIstoryDetailPK.requestNo = :requestNo AND detail.dmuHIstoryDetailPK.srNo = :srNo")
	List<DMUHistoryDetail> findHistoryDetailsByRequestNumberAndSrNo(@Param("requestNo") String requestNo,
			@Param("srNo") Long srNo);

	@Query("SELECT count(detail) from DMUHistoryDetail detail where detail.dmuHIstoryDetailPK.requestNo = :requestNo AND detail.status = :status")
	Long findHistoryDetailsByRequestNoAndStatus(@Param("requestNo") String requestNo, @Param("status") String status);

	@Query("SELECT count(detail) from DMUHistoryDetail detail where detail.dmuHIstoryDetailPK.requestNo = :requestNo AND detail.status = :status ORDER BY detail.dmuHIstoryDetailPK.srNo  ASC")
	Long findHistoryDetailsByRequestNoAndStatusAscOrder(@Param("requestNo") String requestNo,
			@Param("status") String status);

	@Query("SELECT detail from DMUHistoryDetail detail where detail.dmuHIstoryDetailPK.requestNo = :requestNo AND detail.status = :status")
	List<DMUHistoryDetail> findHistoryDetailsByRequestNoAndStatusList(@Param("requestNo") String requestNo,
			@Param("status") String status);

	@Modifying
	@Query("UPDATE  DMUHistoryDetail detail SET detail.status= :status where detail.dmuHIstoryDetailPK.requestNo = :requestNo AND detail.status = :status")
	List<DMUHistoryDetail> updateByRequestNoAndSrNo(@Param("requestNo") String requestNo,
			@Param("status") String status);

	@Modifying
	@Query(" UPDATE DMUHistoryDetail u SET u.status='In Progress' WHERE u.dmuHIstoryDetailPK.requestNo = :requestNo AND u.dmuHIstoryDetailPK.srNo = :srNo")
	void updateForRequestNo(@Param("requestNo") String requestNo, @Param("srNo") Long srNo);

}
