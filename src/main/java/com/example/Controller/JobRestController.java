package com.example.Controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobRestController {

	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	@Qualifier("bancJob")
	private Job job;
	
	@Autowired
	@Qualifier("bancJob2")
	private Job job2;
	
	@RequestMapping("/load")
	public BatchStatus load() throws Exception {
		Map<String, JobParameter> params=new HashMap<>();
		params.put("time", new JobParameter(System.currentTimeMillis()));
		JobParameters jobParameters = new JobParameters(params);
		JobExecution jobExecution = jobLauncher.run(job, jobParameters);
		while (jobExecution.isRunning()) {
			System.out.println(".....");
		}
		System.out.println(job.getName());
		System.out.println("--------------------1-----------------------------");
		return jobExecution.getStatus();
	}
	
	
	@RequestMapping("/load2")
	public BatchStatus load2() throws Exception {
		Map<String, JobParameter> params=new HashMap<>();
		params.put("time", new JobParameter(System.currentTimeMillis()));
		JobParameters jobParameters = new JobParameters(params);
		JobExecution jobExecution = jobLauncher.run(job2, jobParameters);
		while (jobExecution.isRunning()) {
			System.out.println(".....2");
		}
		System.out.println(job.getName());
		System.out.println("--------------------2-----------------------------");
		return jobExecution.getStatus();
	}
}
