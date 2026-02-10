package com.example;

import lombok.extern.log4j.Log4j2;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@Log4j2
@SpringBootApplication
public class DemoAkkaApplication {

    public static void main(String[] args) {
        try {
            SpringApplication.run(DemoAkkaApplication.class, args);

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Bean
    public CommandLineRunner run() {
        return args -> {

        };

    }

}
