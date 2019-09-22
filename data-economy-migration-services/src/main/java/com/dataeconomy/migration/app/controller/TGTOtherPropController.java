package com.dataeconomy.migration.app.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.model.TGTOtherPropDto;
import com.dataeconomy.migration.app.service.TGTOtherPropService;

@RestController
@RequestMapping("/datamigration/tgtotherprop")
public class TGTOtherPropController {

	@Autowired
	TGTOtherPropService tgtOtherPropService;

	@GetMapping("/all")
	public List<TGTOtherPropDto> getAllTGTOtherProp() {
		return tgtOtherPropService.getAllTGTOtherProp();
	}

	@GetMapping("/all/{requestNumber}")
	public TGTOtherPropDto getAllTGTOtherProp(@PathVariable("requestNumber") Long requestNumber) {
		return tgtOtherPropService.getAllTGTOtherProp(requestNumber);
	}

	@PostMapping("/save")
	public TGTOtherPropDto saveTGTOther(@RequestBody TGTOtherPropDto tgtOtherPropDto) {
		return tgtOtherPropService.saveTGTOther(tgtOtherPropDto);
	}
}
