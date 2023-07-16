package com.devCommandos.springbatch.helloBatch;

import com.devCommandos.springbatch.mapper.BatchMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class AdverPlusPrice {

    /**
     * 광고주의 가격을 1씩 올려본다.
     */

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BatchMapper batchMapper;

    @Bean
    public Job getAdverIdJob() throws Exception {
        return jobBuilderFactory.get("getAdverIdJob")
                .incrementer(new RunIdIncrementer())
                .start(this.getAdverIdStep())
                .next(this.chunkStep())
                .build();
    }

    @Bean
    public Step chunkStep() throws Exception {
        return stepBuilderFactory.get("chunkStep")
                .<Map<String, Object>, Map<String, Object>>chunk(1)
                .reader(itemReader())  //null을 리턴할 때 까지 반복함, 단건 처리
                .processor(ItemProcessor()) // 단건 처리
                .writer(ItemWriter()) // 단건 처리된 것들이 리스트로 한번에 처리 ( 청크 사이즈 만큼)
                .build();
    }

    private ItemWriter<Map<String, Object>> ItemWriter() {

        return items -> items.forEach(stringObjectMap -> log.info("객체 : {}", stringObjectMap.toString()));

    }

    //null이 리턴되면 writer에서 무시함.
    private ItemProcessor<Map<String, Object>, Map<String, Object>> ItemProcessor() {
        return item -> {
            Integer price = (Integer) item.get("PRICE");
            item.put("PRICE",price + 1);
            return item;
        };
    }

    private ItemReader<Map<String, Object>> itemReader() throws Exception {
        List<Map<String, Object>> list = batchMapper.selectAdverIdList();
        return new ListItemReader<>(list);
    }

    @Bean
    public Step getAdverIdStep() {
        return stepBuilderFactory.get("getAdverIdStep")
                .tasklet(tasklet())
                .build();
    }

    private Tasklet tasklet() {
        log.info("테스크랫으로 광고주 아이디 디비에서 꺼내보기");
        return (contribution, chunkContext) -> {
            batchMapper.selectAdverIdList().forEach(System.out::println);
            return RepeatStatus.FINISHED;
        };
    }
}
