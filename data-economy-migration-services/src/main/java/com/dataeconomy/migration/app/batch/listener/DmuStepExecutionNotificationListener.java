package com.dataeconomy.migration.app.batch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DmuStepExecutionNotificationListener extends StepExecutionListenerSupport {
	@Override

	public ExitStatus afterStep(StepExecution stepExecution) {
		log.info("After step");
		return super.afterStep(stepExecution);
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		log.info("Before step");
		super.beforeStep(stepExecution);
	}

}
