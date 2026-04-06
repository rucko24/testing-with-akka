package com.example.demo;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Log4j2
public class AdvancedBackpressure {

    public static void main(String[] args) {

        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorSystem");

        Source<Integer, NotUsed> source = Source.fromIterator(() -> Stream.iterate(1, i -> i + 1).iterator())
                .throttle(5, Duration.ofSeconds(1));

        Flow<Integer, List<Integer>, NotUsed> conflateWithSeedFlow = Flow.of(Integer.class)
                .conflateWithSeed((seed) -> {
                    List<Integer> list = new ArrayList<>();
                    list.add(seed);
                    return list;
                }, (list, aggregate) -> {
                    list.add(aggregate);
                    return list;
                });

        Flow<Integer, Integer, NotUsed> conflateFlow = Flow.of(Integer.class)
                .conflate((aggregate, next) -> aggregate + next);

        Flow<Integer, String, NotUsed> flow = Flow.<Integer>create()
                .map(x -> {
                    log.info("Flowing {}", x);
                    return x.toString();
                }).throttle(1, Duration.ofSeconds(1));

        source.via(conflateFlow)
                .via(flow)
                .to(Sink.foreach(x -> log.info("Sinking {}", x)))
                .run(actorSystem);

    }
}
