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
                .to(this.sinkForEach())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed simpleStreamWithAnActorWithListSources() {
//        Source.range(1, 10);
        return Source.from(List.of("A", "B", "C", "D"))
                .via(Flow.of(String.class)
                        .map(e -> "The next value is: " + e))
                .to(this.sinkForEach())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed simpleStreamSourceRepeat() {
        return Source.repeat(3.141592654)
                .via(Flow.of(Double.class)
                        .map(value -> "The next value is: " + value))
                .to(this.sinkForEach())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed sourceCycle() {
        return Source.cycle(() -> List.of("Paula", "Bibi", "Carlos", "Daniel")
                        .iterator())
                .delay(Duration.ofSeconds(1), DelayOverflowStrategy.backpressure())
                .via(Flow.of(String.class))
                .to(this.sinkForEach())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed sourceInfiniteRangeSource() {
        return Source.fromIterator(() -> Stream.iterate(0, seed -> 1 + seed)
                        .iterator())
                .via(Flow.of(Integer.class))
                .throttle(1, Duration.ofSeconds(1))
                .take(3)
                .to(this.sinkForEach())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed sourceInfiniteRangeSourceIgnore() {
        return Source.fromIterator(() -> Stream.iterate(0, seed -> 1 + seed)
                        .iterator())
                .via(Flow.of(Integer.class))
                .throttle(1, Duration.ofSeconds(1))
                .take(3)
                .to(this.sinkIgnore())
                .run(ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public NotUsed sourceRunWith() {
        return this.sinkForEach()
                .runWith(Source.from(List.of("Paula", "Bibi", "Carlos", "Daniel"))
                                .throttle(1, Duration.ofMillis(500))
                                .via(Flow.of(String.class)
                                        .map(item -> "Name is: " + item))
                        , ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    public CompletionStage<Done> sourceRunForEach() {
        return Source.from(List.of("Paula", "Bibi", "Carlos", "Daniel"))
                .throttle(1, Duration.ofMillis(500))
                .via(Flow.of(String.class)
                        .map(item -> "Name is: " + item))
                .runForeach(log::info, ActorSystem.create(Behaviors.empty(), "actor-system"));
    }

    private @NonNull Flow<Integer, String, NotUsed> getMap() {
        return Flow.of(Integer.class)
                .map(value -> "The next value is: " + value);
    }

    private @NonNull <T> Sink<T, CompletionStage<Done>> sinkForEach() {
        return Sink.foreach(log::info);
    }

    private @NonNull <T> Sink<T, CompletionStage<Done>> sinkIgnore() {
        return Sink.ignore();
    }

}
