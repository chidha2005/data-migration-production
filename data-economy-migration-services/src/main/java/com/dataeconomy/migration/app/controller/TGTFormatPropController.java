package com.dataeconomy.migration.app.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataeconomy.migration.app.model.TGTFormatPropDto;
import com.dataeconomy.migration.app.service.TGTFormatPropService;

@RestController
@RequestMapping("/datamigration/tgtformatprop")
public class TGTFormatPropController {

	@Autowired
	private TGTFormatPropService tgtFormatPropService;

	@GetMapping("/all")
	public List<TGTFormatPropDto> getAllTGTFormatProp() {
		return tgtFormatPropService.getAllTGTFormatProp();
	}

	@GetMapping("/all/{requestNumber}")
	public TGTFormatPropDto getAllTGTFormatProp(@PathVariable("requestNumber") Long requestNumber) {
		return tgtFormatPropService.getAllTGTFormatProp(requestNumber);
	}

	@PostMapping("/save")
	public TGTFormatPropDto saveTGTFormat(@RequestBody TGTFormatPropDto tgtFormatPropDto) {
		return tgtFormatPropService.saveTGTFormat(tgtFormatPropDto);
	}
}
