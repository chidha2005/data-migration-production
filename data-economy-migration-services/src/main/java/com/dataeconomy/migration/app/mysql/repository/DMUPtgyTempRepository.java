package com.dataeconomy.migration.app.mysql.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUPtgyPK;
import com.dataeconomy.migration.app.mysql.entity.DMUPtgyTemp;

@Repository
public interface DMUPtgyTempRepository extends JpaRepository<DMUPtgyTemp, DMUPtgyPK> {

	@Query("DELETE from DMUPtgyTemp pgty where pgty.id.userId = :userId")
	void deleteByRequestedUserName(@Param("userId") String userName);

}
