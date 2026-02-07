package com.example.demo;

import com.example.demo.services.BigPrimesServices;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@Log4j2
@SpringBootApplication
public class DemoAkkaBigPrimesApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoAkkaBigPrimesApplication.class, args);
    }

    @Bean
    public CommandLineRunner run(BigPrimesServices bigPrimesServices) {
        return args -> {
//            bigPrimesServices.buildBigPrimes();
//            bigPrimesServices.buildBigPrimesV2();
//            bigPrimesServices.buildBigPrimesV3();
            bigPrimesServices.buildBigPrimesBackPressure();
        };
    }

}