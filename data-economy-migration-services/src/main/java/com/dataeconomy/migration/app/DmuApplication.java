package com.dataeconomy.migration.app;

import org.aspectj.lang.annotation.Aspect;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class,
		DataSourceTransactionManagerAutoConfiguration.class })
@EnableTransactionManagement
@EnableScheduling
@EnableAspectJAutoProxy
@Aspect
public class DmuApplication extends SpringBootServletInitializer {

	public static void main(String[] args) {
		SpringApplication.run(DmuApplication.class, args);
	}

	@Autowired
	PlatformTransactionManager transactionManager;

	@Bean
	public JobExplorer jobExplorer() throws Exception {
		MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(mapJobRepositoryFactory());
		jobExplorerFactory.afterPropertiesSet();
		return jobExplorerFactory.getObject();
	}

	@Bean
	public MapJobRepositoryFactoryBean mapJobRepositoryFactory() {
		MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean();
		factory.setTransactionManager(transactionManager);
		return factory;
	}

	@Bean
	public JobRepository jobRepository() throws Exception {
		return mapJobRepositoryFactory().getObject();
	}

	@Bean
	public JobLauncher jobLauncher(JobRepository jobRepository, TaskExecutor taskExecutor) throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setTaskExecutor(taskExecutor);
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	@Bean
	public TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor("DmmuScheduler-TaskExecutor");
		taskExecutor.setConcurrencyLimit(30);
		return taskExecutor;
	}

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(DmuApplication.class);
	}

}
