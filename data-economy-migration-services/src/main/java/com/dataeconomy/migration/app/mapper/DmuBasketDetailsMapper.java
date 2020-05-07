package com.dataeconomy.migration.app.mapper;

import org.mapstruct.Mapper;

import com.dataeconomy.migration.app.model.DMUBasketDto;
import com.dataeconomy.migration.app.mysql.entity.DmuBasketTempEntity;

@Mapper(componentModel = "spring")
public interface DmuBasketDetailsMapper {

	DMUBasketDto toDto(DmuBasketTempEntity dmuAuthenticationEntity);

//	@Mappings({ @Mapping(source = "requestType.value", target = "SUBMIT") })
	DmuBasketTempEntity toEntity(DMUBasketDto dmuConnectionDTO);
}
