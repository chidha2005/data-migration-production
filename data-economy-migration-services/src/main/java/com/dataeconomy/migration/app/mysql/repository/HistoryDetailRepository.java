package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUHIstoryDetailPK;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryDetail;

@Repository
public interface HistoryDetailRepository extends JpaRepository<DMUHistoryDetail, DMUHIstoryDetailPK> {

	@Query("SELECT detail from DMUHistoryDetail detail where detail.dmuHIstoryDetailPK.requestNo = :requestNo")
	List<DMUHistoryDetail> findHistoryDetailsByRequestNumber(@Param("requestNo") String requestNo);
}
