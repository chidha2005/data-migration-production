package com.dataeconomy.migration.app.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.model.DMUReconMainDto;
import com.dataeconomy.migration.app.service.DMUReconMainService;

@RestController
@RequestMapping("/datamigration/recon")
public class DMUReconController {

	@Autowired
	private DMUReconMainService dmuReconService;

	@GetMapping("/details/{requestNo}")
	public DMUReconMainDto getReconDetailsBySearch(@PathVariable("requestNo") String requestNo) {
		return dmuReconService.getReconDetailsBySearch(requestNo);
	}

	@GetMapping("/all")
	public List<DMUReconMainDto> getAllDatabases() {
		return dmuReconService.getDMUReconMainDetailsList();
	}
}
