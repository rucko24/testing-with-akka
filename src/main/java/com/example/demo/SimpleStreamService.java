package com.example.demo;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionStage;

@Log4j2
@Service
public class SimpleStreamService {

    public NotUsed simpleStreamWithAnActor() {
        return Source.range(1, 10)
                .via(this.getMap())
                .to(this.getForeach())
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
