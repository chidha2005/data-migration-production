package com.dataeconomy.migration.app.controller;

import java.util.Base64;
import java.util.List;

import javax.websocket.server.PathParam;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.model.UserDto;
import com.dataeconomy.migration.app.mysql.entity.DMUUsers;
import com.dataeconomy.migration.app.service.UserService;

@RestController
@RequestMapping("/datamigration/users")
public class UserController {

	@Autowired
	private UserService userService;

	@GetMapping("/all")
	public List<UserDto> getUsers() {
		return userService.getUsers();
	}

	@GetMapping("getUser/{userId}")
	public UserDto getUsers(@PathParam("userId") String userId) {
		return userService.getUser(userId);
	}

	@PostMapping("/save")
	public boolean saveUser(@RequestBody UserDto userDto) throws Exception {
		return userService.saveUser(userDto);
	}

	@PostMapping("/edit")
	public boolean editUser(@RequestBody UserDto userDto) {
		return userService.editUser(userDto);
	}

	@GetMapping("/delete")
	public boolean purgeUsers(@RequestParam("userId") String userId) {
		return userService.purgeUsers(userId);
	}

	@GetMapping("/login")
	public DMUUsers login(@RequestParam("userName") String userName, @RequestParam("password") String password) {
		System.out.println("**userName***" + userName + "password***" + password);
		return userService.login(userName, Base64.getEncoder().encodeToString(password.getBytes()));
	}

	@GetMapping("/resetPassword")
	public boolean resetPassword(@RequestParam("id") String userId, @RequestParam("password") String password) {
		System.out.println("**userName***" + userId + "password***" + userId);
		return userService.resetPassword(userId, Base64.getEncoder().encodeToString(password.getBytes()));
	}

	@GetMapping("/forgotPassword")
	public boolean forgotPassword(@RequestParam("userName") String userName, @RequestParam("emailid") String emailid) {
		return userService.forgotPassword(userName, emailid);
	}
}
