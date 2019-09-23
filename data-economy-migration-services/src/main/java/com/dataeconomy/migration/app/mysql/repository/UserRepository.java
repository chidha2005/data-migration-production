package com.dataeconomy.migration.app.mysql.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dataeconomy.migration.app.mysql.entity.DMUUsers;

@Repository
public interface UserRepository extends JpaRepository<DMUUsers, String> {
	
	@Query(" SELECT dmu FROM DMUUsers dmu where dmu.userName = :userName and dmu.password =:password ")
	public List<DMUUsers> login(@Param("userName") String userName, @Param("password") String password);

	@Query(" SELECT dmu FROM DMUUsers dmu where dmu.userName = :userName and dmu.emailid =:emailid ")
	public List<DMUUsers> forgotPassword(@Param("userName") String userName, @Param("emailid") String emailid);

	@Query(" SELECT dmu FROM DMUUsers dmu where dmu.userName = :userName")
	public List<DMUUsers> checkUserExist(@Param("userName") String userName);
	
}
