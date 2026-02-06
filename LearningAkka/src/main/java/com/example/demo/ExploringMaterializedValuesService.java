package com.example.demo;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.jspecify.annotations.NonNull;
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
                .to(this.sinkForEach())
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
                    if (error == null) {
                        log.info("The graph's materialized value is {}", value);
                    } else {
                        log.info("Error {}", error.getMessage());
                    }
                });
    }

    public CompletionStage<Integer> sourceMatV3() {
        return Source.range(1, 100)
                .map(item -> 1 + SECURE_RANDOM.nextInt(1000))
                .via(Flow.of(Integer.class)
                        .filter(x -> x > 200))
                .viaMat(Flow.of(Integer.class)
                        .filter(x -> x % 2 == 0), Keep.right())
                .toMat(Sink.fold(0, (counter, value) -> {
                    log.info(value);
                    return counter + 1;
                }), Keep.right())
                .run(actorSystem)
                .whenComplete((value, error) -> {
                    if (error == null) {
                        log.info("The graph's materialized value is {}", value);
                    } else {
                        log.info("Error {}", error.getMessage());
                    }
                });
    }

    public CompletionStage<Done> sourceMatV4() {
        var source = Source.range(1, 100)
                .map(item -> 1 + SECURE_RANDOM.nextInt(1000));


        var result = source.via(Flow.of(Integer.class)
                        .filter(x -> x > 200))
                .viaMat(Flow.of(Integer.class)
                        .filter(x -> x % 2 == 0), Keep.right())
                .toMat(Sink.fold(0, (counter, value) -> {
                    log.info(value);
                    return counter + 1;
                }), Keep.right())
                .run(actorSystem)
                .whenComplete((value, error) -> {
                    if (error == null) {
                        log.info("The graph's materialized value is {}", value);
                    } else {
                        log.info("Error {}", error.getMessage());
                    }
                    actorSystem.terminate();
                });

        return source.toMat(Sink.ignore(), Keep.right())
                .run(actorSystem)
                .whenComplete((value, error) -> {
                    actorSystem.terminate();
                });

    }

    public CompletionStage<Integer> sourceReduce() {
        var source = Source.range(1, 100)
                .map(item -> 1 + SECURE_RANDOM.nextInt(1000));

        return source.via(Flow.of(Integer.class)
                        .filter(x -> x > 200))
                .viaMat(Flow.of(Integer.class)
                        .filter(x -> x % 2 == 0), Keep.right())
                .toMat(Sink.reduce((firstValue, secondValue) -> {
                    log.info(secondValue);
                    return firstValue + secondValue;
                }), Keep.right())
                .run(actorSystem)
                .whenComplete((value, error) -> {
                    if (error == null) {
                        log.info("The graph's materialized value is {}", value);
                    } else {
                        log.info("Error {}", error.getMessage());
                    }
                    actorSystem.terminate();
                });

//        return source.toMat(Sink.ignore(), Keep.right())
//                .run(actorSystem)
//                .whenComplete((value, error) -> {
//                    actorSystem.terminate();
//                });

    }

    /**
     * El mísmo source para crear el grafo, siendo una referencia distinta en ambas referencias result
     *
     * @return A CompletionStage<Done>
     */
    public CompletionStage<Integer> sourceTwoGraph() {
        var source = Source.range(1, 100)
                .map(item -> 1 + SECURE_RANDOM.nextInt(1000));

        Sink<Integer, CompletionStage<Integer>> sinWithSum = Sink.fold(0, (counter, value) -> {
            log.info(value);
            return counter + 1;
        });

        Sink<Integer, CompletionStage<Integer>> sink = Sink.reduce((firstValue, secondValue) -> {
            log.info(secondValue);
            return firstValue + secondValue;
        });

        var result = source.via(Flow.of(Integer.class)
                        .filter(x -> x > 200))
                .viaMat(Flow.of(Integer.class)
                        .log("input-filter")
                        .filter(x -> x % 2 == 0)
                        .log("output-filter"), Keep.right())
                .toMat(sink, Keep.right())
                .run(actorSystem)
                .whenComplete((value, error) -> {
                    if (error == null) {
                        log.info("The graph's materialized value is {}", value);
                    } else {
                        log.info("Error {}", error.getMessage());
                    }
//                    actorSystem.terminate();
                });

        return source.toMat(sinWithSum, Keep.right())
                .run(actorSystem)
                .whenComplete((value, error) -> {
//                    actorSystem.terminate();

                });

    }

    private @NonNull <T> Sink<T, CompletionStage<Done>> sinkForEach() {
        return Sink.foreach(log::info);
    }

}
