package com.devCommandos.springbatch.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

@Mapper
public interface BatchMapper {

    List<Map<String, Object>> selectAdverIdList() throws Exception;
    List<Map<String, Object>> selectOrders() throws Exception;

}
