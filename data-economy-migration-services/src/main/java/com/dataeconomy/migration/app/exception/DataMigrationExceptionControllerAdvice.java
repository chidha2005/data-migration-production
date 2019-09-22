package com.dataeconomy.migration.app.exception;

import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class DataMigrationExceptionControllerAdvice {

	@ExceptionHandler(AuthenticationException.class)
	public ResponseMessage handleAuthenticationException(AuthenticationException ex) {
		return ResponseMessage.builder().message(ex.getLocalizedMessage()).build();
	}

	@ExceptionHandler(DataMigrationException.class)
	public ResponseMessage handleDataMigrationException(DataMigrationException ex) {
		return ResponseMessage.builder().message(ex.getLocalizedMessage()).build();
	}

	@ExceptionHandler(Exception.class)
	public ResponseMessage handleException(Exception ex) {
		return ResponseMessage.builder().message(ex.getLocalizedMessage()).build();
	}

}
