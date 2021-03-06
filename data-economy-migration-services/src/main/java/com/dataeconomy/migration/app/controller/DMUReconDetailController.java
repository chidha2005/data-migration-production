package com.dataeconomy.migration.app.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.model.DMUReconDetailDTO;
import com.dataeconomy.migration.app.service.DMUReconDetailService;

@RestController
@RequestMapping("/datamigration/recon/detail")
public class DMUReconDetailController {

	@Autowired
	private DMUReconDetailService dmuReconDetailService;

	@GetMapping("/details/{requestNo}")
	public List<DMUReconDetailDTO> getReconDetailsBySearch(@PathVariable("requestNo") String requestNo) {
		return dmuReconDetailService.getReconDetailsBySearch(requestNo);
	}

	@GetMapping("/all")
	public List<DMUReconDetailDTO> getAllDatabases() {
		return dmuReconDetailService.getDMUReconDetailsList();
	}
}
