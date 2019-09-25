package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUHIstoryDetailPK;
import com.dataeconomy.migration.app.mysql.entity.DMUReconDetail;

@Repository
public interface DMUReconDetailRepository extends JpaRepository<DMUReconDetail, DMUHIstoryDetailPK> {

	@Query("SELECT d FROM DMUReconDetail d WHERE d.dmuHIstoryDetailPK.requestNo = :requestNo ORDER BY d.dmuHIstoryDetailPK.srNo ASC")
	List<DMUReconDetail> findByGivenRequestNo(@Param("requestNo") String requestNo);

}
