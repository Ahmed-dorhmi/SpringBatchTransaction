package com.example.config;

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
public class BatchConfig2 {
	@Autowired private JobBuilderFactory jobBuilderFactory2;
	@Autowired private StepBuilderFactory stepBuilderFactory2;
	@Autowired private org.springframework.batch.item.ItemReader<Transaction> itemReader2;
	@Autowired private ItemWriter<Transaction> itemWriter2;
	@Autowired private ItemProcessor<Transaction,Transaction> itemProcessor2;

	@Bean
	public Job bancJob2() {
		Step step2=stepBuilderFactory2.get("step-name2")
				.<Transaction,Transaction>chunk(100)
				.reader(itemReader2)
				.writer(itemWriter2)
				.processor(itemProcessor2)
				.build();
		
		return jobBuilderFactory2.get("job_name2")
				.incrementer(new RunIdIncrementer())
				.start(step2)
				.build();
	}
	@Bean
	public FlatFileItemReader<Transaction> flatFileItemReader2(@Value("${inputFile2}") Resource inputFile) {
		FlatFileItemReader<Transaction> fileItemReader = new FlatFileItemReader<Transaction>();
		fileItemReader.setName("F2");
		fileItemReader.setLinesToSkip(1);
		fileItemReader.setResource(inputFile);
		fileItemReader.setLineMapper(lineMappe2());
		return fileItemReader;
	}
	
	@Bean
	public LineMapper<Transaction> lineMappe2() {
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
	ItemProcessor<Transaction, Transaction> itemProcessor2() {
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
	ItemWriter<Transaction> itemWriter2(){
		return new ItemWriter<Transaction>() {

			@Autowired
			private TransactionRepository transactionRepository;
			@Override
			public void write(List<? extends Transaction> items) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(".....2");
				transactionRepository.saveAll(items);
				System.out.println();
			}
		};
	}
}
