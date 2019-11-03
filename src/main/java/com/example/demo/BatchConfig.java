package com.example.demo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.batch.api.chunk.ItemReader;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.Delimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.example.dao.Transaction;
import com.example.repo.TransactionRepository;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
	@Autowired private JobBuilderFactory jobBuilderFactory;
	@Autowired private StepBuilderFactory stepBuilderFactory;
	@Autowired private org.springframework.batch.item.ItemReader<Transaction> itemReader;
	@Autowired private ItemWriter<Transaction> itemWriter;
	@Autowired private ItemProcessor<Transaction,Transaction> itemProcessor;

	@Bean
	public Job bancJob() {
		Step step=stepBuilderFactory.get("step-name")
				.<Transaction,Transaction>chunk(100)
				.reader(itemReader)
				.writer(itemWriter)
				.processor(itemProcessor)
				.build();
		
		return jobBuilderFactory.get("job_name")
				.incrementer(new RunIdIncrementer())
				.start(step)
				.build();
	}
	@Bean
	public FlatFileItemReader<Transaction> flatFileItemReader(@Value("${inputFile}") Resource inputFile) {
		FlatFileItemReader<Transaction> fileItemReader = new FlatFileItemReader<Transaction>();
		fileItemReader.setName("F1");
		fileItemReader.setLinesToSkip(1);
		fileItemReader.setResource(inputFile);
		fileItemReader.setLineMapper(lineMappe());
		return fileItemReader;
	}
	
	@Bean
	public LineMapper<Transaction> lineMappe() {
		DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<Transaction>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(";");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("id","accountID","strTransactionDate","transactionType","transactionAmount");
		lineMapper.setLineTokenizer(lineTokenizer);
		BeanWrapperFieldSetMapper fieldSetMapper = new BeanWrapperFieldSetMapper();
		fieldSetMapper.setTargetType(Transaction.class);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}
	
	@Bean 
	ItemProcessor<Transaction, Transaction> itemProcessor() {
		return new ItemProcessor<Transaction, Transaction>() {
			private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy");
			@Override
			public Transaction process(Transaction item) throws Exception {
				// TODO Auto-generated method stub
				item.setTransactionDate(simpleDateFormat.parse(item.getStrTransactionDate()));
				return item;
			}
			
		};
	}
	
	@Bean
	ItemWriter<Transaction> itemWriter(){
		return new ItemWriter<Transaction>() {

			@Autowired
			private TransactionRepository transactionRepository;
			@Override
			public void write(List<? extends Transaction> items) throws Exception {
				// TODO Auto-generated method stub
				transactionRepository.saveAll(items);
				System.out.println();
			}
		};
	}
}
