package com.dataeconomy.migration.app.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.UserDto;
import com.dataeconomy.migration.app.mysql.entity.DMUUsers;
import com.dataeconomy.migration.app.mysql.repository.UserRepository;
import com.dataeconomy.migration.app.util.MailUtil;
import com.dataeconomy.migration.app.util.RandomUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class UserService {

	@Autowired
	private UserRepository userRepository;

	public List<UserDto> getUsers() {
		log.info(" UserService : getUsers() ");
		try {
			List<DMUUsers> usersList = userRepository.findAll();
			return Optional.ofNullable(usersList).orElse(new ArrayList<>()).stream().filter(Objects::nonNull)
					.map(user -> UserDto.builder().emailid(user.getEmailid()).id(user.getId())
							.id(user.getId())
							.userRole(user.getUserRole())
							.userName(user.getUserName())
							.createdBy(user.getCreatedBy())
							.createdDate(user.getCreatedDate())
							.updatedBy(user.getUpdatedBy())
							.updatedDate(user.getUpdatedDate())
							.build())
					.collect(Collectors.toList());

		} catch (Exception exception) {
			return Collections.emptyList();
		}
	}

	public UserDto getUser(String userName) {
		log.info(" UserService : getUser() "); 
		try {
			Optional<DMUUsers> dmuUsersOptional = userRepository.findById(userName);
			if (dmuUsersOptional.isPresent()) {
				DMUUsers dmuUser = dmuUsersOptional.get();
				return UserDto.builder().emailid(dmuUser.getEmailid()).id(dmuUser.getId())
						.id(dmuUser.getId())
						.userName(dmuUser.getUserName())
						.createdBy(dmuUser.getCreatedBy())
						.createdDate(dmuUser.getCreatedDate())
						.updatedBy(dmuUser.getUpdatedBy())
						.updatedDate(dmuUser.getUpdatedDate())
						.build();
			}
			return UserDto.builder().build();
		} catch (Exception exception) {
			return UserDto.builder().build();
		}
	}

	public String saveUser(UserDto userDto) {
		log.info(" UserService : saveUser() ");
		List<DMUUsers> dmList = userRepository.checkUserExist(userDto.getUserName());
		if(dmList!=null && dmList.size()>0)
		{
			return "User Already Exist Please try with other UserName!";
		}
		userDto.setCreatedBy(userDto.getUserName());
		userDto.setCreatedDate(LocalDateTime.now());

		try {
			userDto.setPassword(RandomUtil.getRandomPassword());
			try {
				MailUtil.sendUseralert(userDto.getUserName(), userDto.getEmailid(), userDto.getPassword());
			} catch (Exception e) {
				e.printStackTrace();
			}
			userDto.setPassword(Base64.getEncoder().encodeToString(userDto.getPassword().getBytes()));
			DMUUsers dmuUser = userRepository.save(DMUUsers.builder()
					.emailid(userDto.getEmailid())
					.id(userDto.getId())
					.password(userDto.getPassword())
					.userName(userDto.getUserName())
					.userRole(userDto.getUserRole())
					.createdBy(userDto.getCreatedBy())
					.createdDate(userDto.getCreatedDate())
					.updatedBy(userDto.getUpdatedBy())
					.updatedDate(userDto.getUpdatedDate())
					.build());
			return "User Created Successfully";
		} catch (Exception exception) {
			return "Unable to create User Please Contact Admin";
		}
	}

	public boolean purgeUsers(String userId) {
		try {
			userRepository.deleteById(userId);
			return true;
		} catch (Exception exception) {
			return false;
		}
	}
	public DMUUsers login(String userName,String password) {
		DMUUsers udto = new DMUUsers();
		try {
			List<DMUUsers> dmList = userRepository.login(userName,password);
			if(dmList!=null && dmList.size()>0)
			{
				udto = dmList.get(0);
			}

		} catch (Exception exception) {
		}
		return udto;
	}


	public boolean editUser(UserDto userDto) {
		try {
			Optional<DMUUsers> dmuUsersOpt = userRepository.findById(userDto.getId());
			if (dmuUsersOpt.isPresent()) {
				DMUUsers dmuUsers = dmuUsersOpt.get();
				dmuUsers.setEmailid(userDto.getEmailid());
				dmuUsers.setPassword(userDto.getPassword());
				dmuUsers.setUserRole(userDto.getUserRole()); 
				dmuUsers.setId(userDto.getId());
				dmuUsers.setUpdatedBy(userDto.getUserName());
				dmuUsers.setUpdatedDate(LocalDateTime.now());
				userRepository.save(dmuUsers);
				return true;
			}
			return false;
		} catch (Exception exception) {
			return false;
		}
	}
	public boolean resetPassword(String userId,String password) {
		try {
			Optional<DMUUsers> dmuUsersOpt = userRepository.findById(userId);
			if (dmuUsersOpt.isPresent()) {
				DMUUsers dmuUsers = dmuUsersOpt.get();
				dmuUsers.setPassword(password);
				userRepository.save(dmuUsers);
				return true;
			}
			return false;
		} catch (Exception exception) {
			return false;
		}
	}
	public boolean forgotPassword(String userName,String emailid) {
		try {
			List<DMUUsers> dmList = userRepository.forgotPassword(userName,emailid);
			if(dmList!=null && dmList.size()>0)
			{
				try {
					MailUtil.senForgotPasswordalert(dmList.get(0).getUserName(), dmList.get(0).getEmailid(),new String(Base64.getDecoder().decode(dmList.get(0).getPassword())));
				} catch (Exception e) {
					e.printStackTrace();
				}
				return true;
			}
			else
			{
				return false;
			}

		} catch (Exception exception) {
			return false;
		}
	}

}
