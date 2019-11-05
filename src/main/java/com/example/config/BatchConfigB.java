package com.example.config;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.batch.api.chunk.ItemReader;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.Delimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import com.example.dao.Transaction;
import com.example.repo.TransactionRepository;
import org.springframework.data.domain.Sort;

@Configuration
@EnableBatchProcessing
public class BatchConfigB {
	@Autowired private JobBuilderFactory jobBuilderFactoryB;
	@Autowired private StepBuilderFactory stepBuilderFactoryB;
	@Autowired private DataSource dataSource;
    @Autowired private TransactionRepository transactionRepository;

	@Autowired 
	@Qualifier("readerB")
	private org.springframework.batch.item.ItemReader<Transaction> itemReaderB;
	@Autowired 
	@Qualifier("writerB")
	private ItemWriter<Transaction> itemWriterB;
	@Autowired 
	@Qualifier("itemProcessorB")
	private ItemProcessor<Transaction,Transaction> itemProcessorB;
	
	private Resource outputResource = new FileSystemResource("output/outputData.csv");
	@Bean
	public Job bancJobB() {
		Step stepB=stepBuilderFactoryB.get("step-nameB")
				.<Transaction,Transaction>chunk(100)
				.reader(itemReaderB)
				.writer(itemWriterB)
				.processor(itemProcessorB)
				.build();
		
		return jobBuilderFactoryB.get("job_nameB")
				.incrementer(new RunIdIncrementer())
				.start(stepB)
				.build();
	}
	@Bean
	public RepositoryItemReader<Transaction> readerB() {
        RepositoryItemReader<Transaction> reader = new RepositoryItemReader<>();
        reader.setRepository(transactionRepository);
        reader.setMethodName("findAll");

        Map<String, Sort.Direction> sort = new HashMap<String, Sort.Direction>();
        sort.put("id", Sort.Direction.ASC);
        reader.setSort(sort);

        return reader;
    }

	@Bean 
	ItemProcessor<Transaction, Transaction> itemProcessorB() {
		return new ItemProcessor<Transaction, Transaction>() {
			private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy");
			@Override
			public Transaction process(Transaction item) throws Exception {
				// TODO Auto-generated method stub
				item.setStrTransactionDate(simpleDateFormat.format(item.getTransactionDate()));
				return item;
			}
		};
	}
	
	@Bean
	public FlatFileItemWriter<Transaction> writerB() 
    {
        //Create writer instance
        FlatFileItemWriter<Transaction> writer = new FlatFileItemWriter<>();
         
        //Set output file location
        writer.setResource(outputResource);
         
        //All job repetitions should "append" to same output file
        writer.setAppendAllowed(true);
         
        //Name field values sequence based on object properties 
        writer.setLineAggregator(new DelimitedLineAggregator<Transaction>() {
            {
                setDelimiter(";");
                setFieldExtractor(new BeanWrapperFieldExtractor<Transaction>() {
                    {
                        setNames(new String[] { "id","accountID","strTransactionDate","transactionType","transactionAmount"});
                    }
                });
            }
        });
        return writer;
    }
	}
