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

import java.util.List;
import java.util.concurrent.CompletionStage;

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
        Source<String, NotUsed> source = Source.from(List.of("A","B","C","D"));

        Flow<Integer, String, NotUsed> flow = Flow.of(Integer.class)
                .map(e -> "The next value is: " + e);

        Flow<String, String, NotUsed> flow2 = Flow.of(String.class)
                .map(e -> "The next value is: " + e);


        return source.via(flow2)
                .to(getForeach())
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
