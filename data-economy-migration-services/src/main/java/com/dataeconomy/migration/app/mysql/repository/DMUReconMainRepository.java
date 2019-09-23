package com.dataeconomy.migration.app.mysql.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUReconMain;

@Repository
public interface DMUReconMainRepository extends JpaRepository<DMUReconMain, String> {

}
