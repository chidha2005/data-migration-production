package com.dataeconomy.migration.app.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.model.HistoryMainDto;
import com.dataeconomy.migration.app.service.DMURequestService;

@RestController
@RequestMapping("/datamigration/request")
public class DMURequestController {

	@Autowired
	private DMURequestService dmuRequestService;

	@GetMapping("/save")
	public List<ConnectionDto> saveRequest(@RequestBody HistoryMainDto historyMainDto) {
		return dmuRequestService.saveRequest(historyMainDto);
	}

}
