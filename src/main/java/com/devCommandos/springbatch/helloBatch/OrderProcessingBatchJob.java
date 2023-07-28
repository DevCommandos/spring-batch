package com.devCommandos.springbatch.helloBatch;

import com.devCommandos.springbatch.mapper.BatchMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrderProcessingBatchJob {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BatchMapper batchMapper;

    @Bean
    public Job findMonitorBuyUserJob() throws Exception {
        return jobBuilderFactory.get("0001")
                .incrementer(new RunIdIncrementer())
                .start(findMonitorBuyUserStep(null))
                .build();
    }

    @Bean
    @JobScope
    public Step findMonitorBuyUserStep(@Value("#{jobParameters[CUSTOMER_NAME]}") String customerName) throws Exception {
        return stepBuilderFactory.get("selectOrderUserList")
                .<Map<String, Object>, String>chunk(1)
                .reader(this.selectOrderUserList(customerName))
                .processor(this.findMonitorBuyUser())
                .writer(this.insertMonitorBuyUser())
                .build();
    }

    private ItemProcessor<Map<String, Object>, String> findMonitorBuyUser() {
        return item -> {
            String product = (String) item.get("PRODUCT_NAME");
            if("Monitor".equals(product)){
                return (String) item.get("CUSTOMER_NAME");
            }
            return null;
        };
    }

    private ItemWriter<String> insertMonitorBuyUser() {
        return items -> items.forEach(System.out::println);
    }

    private ItemReader<Map<String, Object>> selectOrderUserList(String customerName) throws Exception {
        log.info("외부 파라미터 주입 테스트 : {}", customerName);
        List<Map<String, Object>> result = batchMapper.selectOrders();
        return new ListItemReader<>(result);
    }
}
