package com.example.demo;

import com.example.demo.monitorinspeed.VehicleSpeedService;
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
                                 ExploringFlowService exploringFlowService,
                                 ExploringMaterializedValuesService exploringMaterializedValuesService,
                                 VehicleSpeedService vehicleSpeedService) {
        return args -> {
//			log.info(simpleStreamService.simpleStreamWithAnActor());
//			log.info(simpleStreamService.simpleStreamWithAnActorWithListSources());
//			log.info(simpleStreamService.simpleStreamSourceRepeat());
//			log.info(simpleStreamService.sourceCycle());
//			log.info(simpleStreamService.sourceInfiniteRangeSource());
//			log.info(simpleStreamService.sourceInfiniteRangeSourceIgnore());
//			log.info(simpleStreamService.sourceRunWith());
//			log.info(simpleStreamService.sourceRunForEach());
//			log.info(exploringFlowService.sourceFilter());
//			log.info(exploringFlowService.sourceFilterMapGrouped());
//			log.info(exploringFlowService.sourceFilterMapGroupedv2());
//			log.info(exploringMaterializedValuesService.source());
//			log.info(exploringMaterializedValuesService.sourcev2());
//			log.info(exploringMaterializedValuesService.sourceMatV3());
//			log.info(exploringMaterializedValuesService.sourceMatV4());
//			log.info(exploringMaterializedValuesService.sourceReduce());
//			log.info(exploringMaterializedValuesService.sourceTwoGraph());
//			log.info(vehicleSpeedService.buildVehicleTracking());
			log.info(vehicleSpeedService.buildGraphDSL());
        };
    }

}
