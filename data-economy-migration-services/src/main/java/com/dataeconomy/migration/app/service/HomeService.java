package com.dataeconomy.migration.app.service;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.mysql.repository.DMUReconDetailRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class HomeService {

	@Autowired
	private DMUReconDetailRepository dmuReconDetailRepository;

	public void getRequestAndReconStatus() {
		log.info(" HomeService :: getRequestAndReconStatus ");
		try {

		} catch (Exception exception) {
			log.info(" Exception occured at HomeService :: getRequestAndReconStatus {} ",
					ExceptionUtils.getStackTrace(exception));
		}

	}

}
