package com.dataeconomy.migration.app.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dataeconomy.migration.app.connection.HDFSConnectionService;
import com.dataeconomy.migration.app.exception.DataMigrationException;
import com.dataeconomy.migration.app.model.DMUBasketDto;
import com.dataeconomy.migration.app.mysql.entity.DMUBasketTemp;
import com.dataeconomy.migration.app.mysql.entity.DMUHIstoryDetailPK;
import com.dataeconomy.migration.app.mysql.entity.DMUHistoryDetail;
import com.dataeconomy.migration.app.mysql.entity.DMUPtgyPK;
import com.dataeconomy.migration.app.mysql.entity.DMUPtgyTemp;
import com.dataeconomy.migration.app.mysql.entity.DMUReconDetail;
import com.dataeconomy.migration.app.mysql.entity.DMUReconMain;
import com.dataeconomy.migration.app.mysql.repository.BasketTempRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUPgtyRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUPtgyTempRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUReconDetailRepository;
import com.dataeconomy.migration.app.mysql.repository.DMUReconMainRepository;
import com.dataeconomy.migration.app.mysql.repository.HistoryDetailRepository;
import com.dataeconomy.migration.app.util.Constants;
import com.google.common.collect.Lists;

@Service
public class DMUBasketService {

	@Autowired
	private BasketTempRepository basketTempRepository;

	@Autowired
	private DMUPgtyRepository dmuPgtyRepository;

	@Autowired
	private HDFSConnectionService hdfcConnectionService;

	@Autowired
	private DMUReconMainRepository reconMainRepository;

	@Autowired
	private HistoryDetailRepository historyDetailRepository;

	@Autowired
	private DMUReconDetailRepository dmuReconDetailRepository;

	@Autowired
	private DMUPtgyTempRepository dmuPtgyRepository;

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

	@Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
	public synchronized boolean saveBasketDetails(List<DMUBasketDto> dmuBasketDtoList, String userName)
			throws DataMigrationException {
		try {
			Optional.ofNullable(dmuBasketDtoList).orElse(new ArrayList<>()).stream()
					.filter(basketDto -> basketDto.isAddtoBasket()).forEach(dmuBasketDto -> {
						basketTempRepository.save(
								DMUBasketTemp.builder().srNo(dmuBasketDto.getSrNo()).userId(dmuBasketDto.getUserId())
										.schemaName(dmuBasketDto.getSchemaName()).tableName(dmuBasketDto.getTableName())
										.filterCondition(dmuBasketDto.getFilterCondition())
										.targetS3Bucket(dmuBasketDto.getTargetS3Bucket())
										.incrementalFlag(dmuBasketDto.getIncrementalFlag())
										.incrementalClmn(dmuBasketDto.getIncrementalClmn())
										.labelName(dmuBasketDto.getLabelName()).build());

						dmuPgtyRepository.save(DMUPtgyTemp.builder()
								.id(DMUPtgyPK.builder().userId(dmuBasketDto.getUserId())
										.labelName(dmuBasketDto.getLabelName()).build())
								.tknztnEnabled(dmuBasketDto.isTknztnEnabled() ? Constants.YES : Constants.NO)
								.tknztnFilePath(dmuBasketDto.getTknztnFilePath()).build());

						reconMainRepository.save(DMUReconMain.builder().userId(dmuBasketDto.getUserId())
								.status(Constants.NOT_STARTED).requestType(dmuBasketDto.getRequestType())
								.requestNo(dmuBasketDto.getLabelName()).build());

						historyDetailRepository.save(DMUHistoryDetail.builder()
								.dmuHIstoryDetailPK(DMUHIstoryDetailPK.builder().srNo(dmuBasketDto.getSrNo())
										.requestNo(dmuBasketDto.getLabelName() + LocalDate.now()).build())
								.schemaName(dmuBasketDto.getSchemaName()).tableName(dmuBasketDto.getTableName())
								.filterCondition(dmuBasketDto.getFilterCondition())
								.targetS3Bucket(dmuBasketDto.getTargetS3Bucket())
								.incrementalFlag(dmuBasketDto.getIncrementalFlag())
								.incrementalClmn(dmuBasketDto.getIncrementalClmn()).status(Constants.SUBMITTED)
								.build());

						dmuReconDetailRepository.save(DMUReconDetail.builder()
								.dmuHIstoryDetailPK(DMUHIstoryDetailPK.builder().srNo(dmuBasketDto.getSrNo())
										.requestNo(dmuBasketDto.getLabelName() + LocalDate.now()).build())
								.schemaName(dmuBasketDto.getSchemaName()).tableName(dmuBasketDto.getTableName())
								.filterCondition(dmuBasketDto.getFilterCondition())
								.targetS3Bucket(dmuBasketDto.getTargetS3Bucket())
								.incrementalFlag(dmuBasketDto.getIncrementalFlag())
								.incrementalColumn(dmuBasketDto.getIncrementalClmn()).status(Constants.NOT_STARTED)
								.build());
					});
			basketTempRepository.deleteById(userName);
			dmuPtgyRepository.deleteByRequestedUserName(userName);
			return true;
		} catch (Exception e) {
			throw new DataMigrationException("Unable to persist basket details to database ");
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
			return new JdbcTemplate(hdfcConnectionService.getValidDataSource(Constants.REGULAR))
					.query("USE " + searchParam + "; SHOW TABLES;", new ResultSetExtractor<List<DMUBasketDto>>() {

						@Override
						public List<DMUBasketDto> extractData(ResultSet rs) throws SQLException, DataAccessException {
							List<DMUBasketDto> dmuBasketDtoList = Lists.newArrayList();
							while (rs.next()) {
								Long i = 0L;
								dmuBasketDtoList.add(DMUBasketDto.builder().srNo(++i).schemaName(rs.getString(1))
										.tableName(rs.getString(1)).filterCondition(null)
										.targetS3Bucket(searchParam + "/" + rs.getString(1))
										.incrementalFlag(Constants.NO).incrementalClmn(null).build());
							}
							return Collections.emptyList();
						}
					});
		} catch (Exception exception) {
			return Collections.emptyList();
		}
	}

}