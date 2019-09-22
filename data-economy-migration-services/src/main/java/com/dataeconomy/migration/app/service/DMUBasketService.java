package com.dataeconomy.migration.app.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import com.dataeconomy.migration.app.model.DMUBasketDto;
import com.dataeconomy.migration.app.mysql.entity.DMUBasketTemp;
import com.dataeconomy.migration.app.mysql.entity.DMUPtgyTemp;
import com.dataeconomy.migration.app.mysql.repository.BasketTempRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUPgtyRepository;
import com.dataeconomy.migration.app.util.Constants;
import com.google.common.collect.Lists;

@Service
public class DMUBasketService {

	@Autowired
	private BasketTempRepository basketTempRepository;

	@Autowired
	private DMUPgtyRepository dmuPgtyRepository;

	@Autowired
	private JdbcTemplate hiveJdbcTemplate;

	public List<DMUBasketDto> getAllBasketDetails() {
		try {
			return Optional.ofNullable(basketTempRepository.findAll()).orElse(new ArrayList<>()).stream()
					.map(basketObj -> {
						return DMUBasketDto.builder().srNo(basketObj.getSrNo()).userId(basketObj.getUserId())
								.schemaName(basketObj.getSchemaName()).tableName(basketObj.getTableName())
								.filterCondition(basketObj.getFilterCondition())
								.targetS3Bucket(basketObj.getTargetS3Bucket())
								.incrementalFlag(basketObj.getIncrementalFlag())
								.incrementalClmn(basketObj.getIncrementalClmn()).labelName(basketObj.getLabelName())
								.build();
					}).collect(Collectors.toList());
		} catch (Exception exception) {
			return Collections.emptyList();
		}

	}

	public DMUBasketDto saveBasketDetails(DMUBasketDto dmuBasketDto) {
		try {
			DMUBasketTemp dmuBasketTempEntity = DMUBasketTemp.builder().srNo(dmuBasketDto.getSrNo())
					.userId(dmuBasketDto.getUserId()).schemaName(dmuBasketDto.getSchemaName())
					.tableName(dmuBasketDto.getTableName()).filterCondition(dmuBasketDto.getFilterCondition())
					.targetS3Bucket(dmuBasketDto.getTargetS3Bucket()).incrementalFlag(dmuBasketDto.getIncrementalFlag())
					.incrementalClmn(dmuBasketDto.getIncrementalClmn()).labelName(dmuBasketDto.getLabelName()).build();
			basketTempRepository.save(dmuBasketTempEntity);

			dmuPgtyRepository
					.save(DMUPtgyTemp.builder().userId(dmuBasketDto.getUserId()).labelName(dmuBasketDto.getLabelName())
							.tknztnEnabled(dmuBasketDto.isTknztnEnabled() ? Constants.YES : Constants.NO)
							.tknztnFilePath(dmuBasketDto.getTknztnFilePath()).build());
			return dmuBasketDto;
		} catch (Exception e) {
			return dmuBasketDto;
		}
	}

	public List<DMUBasketDto> getBasketDetailsByUserId(String userId) {
		try {
			return Optional.ofNullable(basketTempRepository.findAll()).orElse(new ArrayList<>()).stream()
					.map(basketObj -> {
						return DMUBasketDto.builder().srNo(basketObj.getSrNo()).userId(basketObj.getUserId())
								.schemaName(basketObj.getSchemaName()).tableName(basketObj.getTableName())
								.filterCondition(basketObj.getFilterCondition())
								.targetS3Bucket(basketObj.getTargetS3Bucket())
								.incrementalFlag(basketObj.getIncrementalFlag())
								.incrementalClmn(basketObj.getIncrementalClmn()).labelName(basketObj.getLabelName())
								.build();
					}).collect(Collectors.toList());
		} catch (Exception exception) {
			return Collections.emptyList();
		}
	}

	public boolean purgeBasketDetailsByUserId(String userId) {
		try {
			basketTempRepository.deleteById(userId);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public List<DMUBasketDto> getBasketDetailsBySearchParam(String searchParam) {
		try {
			return hiveJdbcTemplate.query("USE " + searchParam + "; SHOW TABLES;",
					new ResultSetExtractor<List<DMUBasketDto>>() {

						@Override
						public List<DMUBasketDto> extractData(ResultSet rs) throws SQLException, DataAccessException {
							List<DMUBasketDto> dmuBasketDtoList = Lists.newArrayList();
							while (rs.next()) {
								System.out.println(rs.getString(1));
							}
							return null;
						}
					});
		} catch (Exception exception) {
			return Collections.emptyList();
		}
	}

}