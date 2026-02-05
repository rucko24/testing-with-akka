package com.example.demo;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionStage;

@Log4j2
@Service
public class ExploringFlowService {

    private final ActorSystem<String> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");

    public NotUsed sourceFilter() {
        return Source.range(1, 200)
                .via(Flow.of(Integer.class)
                        .filter(integer -> integer % 17 == 0))
                .to(this.sinkForeach())
                .run(this.actorSystem);
    }

    private <T> Sink<T, CompletionStage<Done>> sinkForeach() {
        return Sink.foreach(log::info);
    }

}
