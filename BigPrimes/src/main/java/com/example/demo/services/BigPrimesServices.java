package com.example.demo.services;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Log4j2
@Service
public class BigPrimesServices {

    private static final ActorSystem ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    public NotUsed buildBigPrimes() {
        return Source.range(1, 10)
                .via(Flow.of(Integer.class)
                        .map(number -> new BigInteger(2000, new Random())))
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

}
