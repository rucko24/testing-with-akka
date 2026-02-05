package com.example.demo;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.concurrent.CompletionStage;

@Log4j2
@Service
public class ExploringMaterializedValuesService {

    private final ActorSystem<String> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    public NotUsed source() {
        return Source.range(1, 100)
                .map(item -> 1 + SECURE_RANDOM.nextInt(1000))
                .via(Flow.of(Integer.class)
                        .filter(x -> x > 200))
                .via(Flow.of(Integer.class)
                        .filter(x -> x % 2 == 0))
                .to(Sink.foreach(log::info))
                .run(actorSystem);

    }

    /**
     * Using toMat operator
     *
     * @return A CompletionStage<Integer>
     */
    public CompletionStage<Integer> sourcev2() {
        return Source.range(1, 100)
                .map(item -> 1 + SECURE_RANDOM.nextInt(1000))
                .via(Flow.of(Integer.class)
                        .filter(x -> x > 200))
                .via(Flow.of(Integer.class)
                        .filter(x -> x % 2 == 0))
                .toMat(Sink.fold(0, (counter, value) -> {
                    log.info(value);
                    return counter + 1;
                }), Keep.right())
                .run(actorSystem)
                .whenComplete((value, error) -> {
                   if(error == null) {
                       log.info("The graph's materialized value is {}", value);
                   } else {
                       log.info("Error {}", error.getMessage());
                   }
                });
    }


}
