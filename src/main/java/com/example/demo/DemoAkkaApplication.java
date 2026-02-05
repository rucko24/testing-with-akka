package com.example.demo;

import lombok.extern.log4j.Log4j2;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@Log4j2
@SpringBootApplication
public class DemoAkkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoAkkaApplication.class, args);
	}

	@Bean
	public CommandLineRunner run(SimpleStreamService simpleStreamService,
								 ExploringFlowService exploringFlowService) {
		return args -> {
//			log.info(simpleStreamService.simpleStreamWithAnActor());
//			log.info(simpleStreamService.simpleStreamWithAnActorWithListSources());
//			log.info(simpleStreamService.simpleStreamSourceRepeat());
//			log.info(simpleStreamService.sourceCycle());
//			log.info(simpleStreamService.sourceInfiniteRangeSource());
//			log.info(simpleStreamService.sourceInfiniteRangeSourceIgnore());
//			log.info(simpleStreamService.sourceRunWith());
//			log.info(simpleStreamService.sourceRunForEach());
			log.info(exploringFlowService.sourceFilter());
		};
	}

}
