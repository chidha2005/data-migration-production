package com.dataeconomy.migration.app.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.model.ReconAndRequestStatusDto;
import com.dataeconomy.migration.app.service.HomeService;

@RestController
@RequestMapping("/datamigration/home")
public class HomeController {

	@Autowired
	private HomeService homeService;

	@GetMapping("/status")
	public ReconAndRequestStatusDto getRequestAndReconStatus() {
		return homeService.getRequestAndReconStatus();
	}

}
