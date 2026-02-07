package com.example.demo.services;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.Attributes;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.lang.management.ThreadMXBean;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

@Log4j2
@Service
public class BigPrimesServices {

    private static final ActorSystem<String> ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    public NotUsed buildBigPrimes() {
        return Source.range(1, 10)
                .via(Flow.of(Integer.class)
                        .map(number -> new BigInteger(2000, SECURE_RANDOM)))
                .via(Flow.of(BigInteger.class)
                        .map(number -> {
                            var prime = number.nextProbablePrime();
                            log.info("Prime is " + prime);
                            return prime;
                        }))
                .via(Flow.of(BigInteger.class)
                        .grouped(10)
                        .map(list -> {
                            List<BigInteger> newList = new ArrayList<>(list);
                            Collections.sort(newList, Collections.reverseOrder());
                            return newList;
                        }))
                .to(Sink.foreach(log::info))
                .run(ACTOR_SYSTEM);
    }

    /**
     * 30. Asynchronous boundaries v2
     *
     * @return NotUsed
     */
    public CompletionStage<Done> buildBigPrimesV2() {
        long startTime = System.currentTimeMillis();
        return Source.range(1, 10)
                .via(Flow.of(Integer.class)
                        .map(number -> new BigInteger(3000, SECURE_RANDOM)))
                .via(Flow.of(BigInteger.class)
                        .map(number -> {
                            var prime = number.nextProbablePrime();
                            log.info("Prime is " + prime);
                            return prime;
                        }))
                .via(Flow.of(BigInteger.class)
                        .grouped(10)
                        .map(list -> {
                            List<BigInteger> newList = new ArrayList<>(list);
                            Collections.sort(newList, Collections.reverseOrder());
                            return newList;
                        }))
                .toMat(Sink.foreach(log::info), Keep.right())
                .run(ACTOR_SYSTEM)
                .whenComplete((data, error) -> {
                    if(error == null) {
                        log.info("The application ran in: {}(ms)", System.currentTimeMillis() - startTime);
                        ACTOR_SYSTEM.terminate();
                    }
                });
    }

    /**
     * 30. Asynchronous boundaries v3
     *
     * Ejecucion en 2 actores ?
     *
     * @return NotUsed
     */
    public CompletionStage<Done> buildBigPrimesV3() {
        long startTime = System.currentTimeMillis();
        return Source.range(1, 10)
                .via(Flow.of(Integer.class)
                        .map(number -> {
                            final BigInteger bigInteger = new BigInteger(3000, SECURE_RANDOM);
                            log.info("BigInteger is {}", bigInteger);
                            return bigInteger;
                        }))
                .log("BigInteger")
                .async()
                .log("Big primer")
                .via(Flow.of(BigInteger.class)
                        .map(number -> {
                            var prime = number.nextProbablePrime();
                            log.info("Prime is " + prime);
                            return prime;
                        }))
                .async()
                .via(Flow.of(BigInteger.class)
                        .grouped(10)
                        .map(list -> {
                            List<BigInteger> newList = new ArrayList<>(list);
                            Collections.sort(newList, Collections.reverseOrder());
                            return newList;
                        }))
                .toMat(Sink.foreach(log::info), Keep.right())
                .run(ACTOR_SYSTEM)
                .whenComplete((data, error) -> {
                    if(error == null) {
                        log.info("The application ran in: {}(ms)", System.currentTimeMillis() - startTime);
                        ACTOR_SYSTEM.terminate();
                    }
                });
    }

    /**
     *
     * 31. Introducing back-pressure
     *
     * @return A CompletionStage<Done>
     */
    public CompletionStage<Done> buildBigPrimesBackPressure() {
        long startTime = System.currentTimeMillis();
        return Source.range(1, 100)
                .via(Flow.of(Integer.class)
                        .map(number -> {
                            final BigInteger bigInteger = new BigInteger(3000, SECURE_RANDOM);
                            log.info("BigInteger is {}", bigInteger);
                            return bigInteger;
                        })
                        .addAttributes(Attributes.inputBuffer(16, 32)))
                .buffer(16, OverflowStrategy.backpressure())
                .async()
                .via(Flow.of(BigInteger.class)
                        .map(number -> {
                            var prime = number.nextProbablePrime();
                            log.info("Prime is " + prime);
                            return prime;
                        }))
                .async()
                .via(Flow.of(BigInteger.class)
                        .grouped(10)
                        .map(list -> {
                            List<BigInteger> newList = new ArrayList<>(list);
                            newList.sort(Collections.reverseOrder());
                            return newList;
                        }))
                .toMat(Sink.foreach(log::info), Keep.right())
                .run(ACTOR_SYSTEM)
                .whenComplete((data, error) -> {
                    if(error == null) {
                        log.info("The application ran in: {}(ms)", System.currentTimeMillis() - startTime);
                        ACTOR_SYSTEM.terminate();
                    }
                });
    }

    /**
     *
     * 33. Other overflow strategies
     *
     * @return A CompletionStage<Done>
     */
    public CompletionStage<Done> buildBigPrimesBackPressurev2() {
        long startTime = System.currentTimeMillis();
        return Source.range(1, 100)
                .via(Flow.of(Integer.class)
                        .map(number -> {
                            final BigInteger bigInteger = new BigInteger(3000, SECURE_RANDOM);
                            log.info("BigInteger is {}", bigInteger);
                            return bigInteger;
                        })
                        .addAttributes(Attributes.inputBuffer(16, 32)))
                .buffer(16, OverflowStrategy.dropHead()) //GPS
                .async()
                .via(Flow.of(BigInteger.class)
                        .map(number -> {
                            var prime = number.nextProbablePrime();
                            log.info("Prime is " + prime);
                            return prime;
                        }))
                .async()
                .via(Flow.of(BigInteger.class)
                        .grouped(10)
                        .map(list -> {
                            List<BigInteger> newList = new ArrayList<>(list);
                            newList.sort(Collections.reverseOrder());
                            return newList;
                        }))
                .toMat(Sink.foreach(log::info), Keep.right())
                .run(ACTOR_SYSTEM)
                .whenComplete((data, error) -> {
                    if(error == null) {
                        log.info("The application ran in: {}(ms)", System.currentTimeMillis() - startTime);
                        ACTOR_SYSTEM.terminate();
                    }
                });
    }


}
