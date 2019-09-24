package com.dataeconomy.migration.app.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.service.HomeService;

@RestController
@RequestMapping("/datamigration/tgtformatprop")
public class HomeController {

	@Autowired
	private HomeService homeService;

	public void getRequestAndReconStatus() {
		homeService.getRequestAndReconStatus();
	}

}
