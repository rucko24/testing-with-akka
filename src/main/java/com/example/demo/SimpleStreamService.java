package com.example.demo;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.stream.DelayOverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

@Log4j2
@Service
public class SimpleStreamService {

    public NotUsed simpleStreamWithAnActor() {
//        Source.range(1, 10);
        return Source.range(1, 10, 2)
                .via(this.getMap())
                .to(this.getForeach())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed simpleStreamWithAnActorWithListSources() {
//        Source.range(1, 10);
        return Source.from(List.of("A", "B", "C", "D"))
                .via(Flow.of(String.class)
                        .map(e -> "The next value is: " + e))
                .to(getForeach())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed simpleStreamSourceRepeat() {
        return Source.repeat(3.141592654)
                .via(Flow.of(Double.class)
                        .map(value -> "The next value is: " + value))
                .to(Sink.foreach(log::info))
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed sourceCycle() {
        return Source.cycle(() -> List.of("Paula", "Bibi", "Carlos", "Daniel")
                        .iterator())
                .delay(Duration.ofSeconds(1), DelayOverflowStrategy.backpressure())
                .via(Flow.of(String.class))
                .to(Sink.foreach(log::info))
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed sourceInfiniteRangeSource() {
        return Source.fromIterator(() -> Stream.iterate(0, seed -> 1 + seed)
                        .iterator())
                .via(Flow.of(Integer.class))
                .throttle(1, Duration.ofSeconds(1))
                .take(3)
                .to(Sink.foreach(log::info))
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    private @NonNull Flow<Integer, String, NotUsed> getMap() {
        return Flow.of(Integer.class)
                .map(value -> "The next value is: " + value);
    }

    private @NonNull Sink<String, CompletionStage<Done>> getForeach() {
        return Sink.foreach(log::info);
    }

}
