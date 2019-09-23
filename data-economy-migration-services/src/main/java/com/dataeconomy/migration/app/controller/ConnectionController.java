package com.dataeconomy.migration.app.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.ConnectionDto;
import com.dataeconomy.migration.app.service.ConnectionService;

import io.swagger.annotations.Api;

@RestController
@RequestMapping("/datamigration/connection")
@Api(value = "DataMigration Utility Connection System")
public class ConnectionController {

	@Autowired
	private ConnectionService connectionService;

	@RequestMapping("/validate")
	public boolean validateConnection(@RequestBody ConnectionDto connectionDto) throws DataMigrationException {
		return connectionService.validateConnection(connectionDto);
	}

	@RequestMapping("/save")
	public boolean saveConnectionDetails(@RequestBody ConnectionDto connectionDto) {
		return connectionService.saveConnectionDetails(connectionDto);
	}

	@RequestMapping("/get")
	public ConnectionDto getConnectionDetails() {
		return connectionService.getConnectionDetails();
	}

}
