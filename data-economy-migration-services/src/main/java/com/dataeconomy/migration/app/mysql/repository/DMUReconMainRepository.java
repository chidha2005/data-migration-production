package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DmuReconAndRequestCountProjection;
import com.dataeconomy.migration.app.mysql.entity.DmuReconMainentity;

@Repository
public interface DMUReconMainRepository extends JpaRepository<DmuReconMainentity, String> {

	@Query("select new com.dataeconomy.migration.app.mysql.entity.DmuReconAndRequestCountProjection(v.status , count(v) as cnt) from DmuReconMainentity v group by v.status")
	public List<DmuReconAndRequestCountProjection> findReconMainStatusCount();

	@Query("select new com.dataeconomy.migration.app.mysql.entity.DmuReconAndRequestCountProjection(v.status , count(v) as cnt) from DmuReconMainentity v where v.userId = :userId group by v.status")
	public List<DmuReconAndRequestCountProjection> findReconMainStatusCountByuserID(@Param("userId") String userId);

	@Query("select v from DmuReconMainentity v where v.userId = :userId")
	public List<DmuReconMainentity> getAllDatabasesByUserId(@Param("userId") String userId);

}
