package com.example.demo;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Using some flows
 */
@Log4j2
@Service
public class CombiningFlowService {

    private final ActorSystem<String> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");

    public NotUsed source() {
        return Source.from(List.of("Paula", "Bibi", "Carlos", "Daniel"))
                .via(Flow.of(String.class)
                        .map(name -> name.split(" ").length))
                .to(this.sinkForeach())
                .run(this.actorSystem);
    }


    private <T> Sink<T, CompletionStage<Done>> sinkForeach() {
        return Sink.foreach(log::info);
    }

}
