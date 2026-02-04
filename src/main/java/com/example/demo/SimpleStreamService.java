package com.example.demo;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionStage;

@Log4j2
@Service
public class SimpleStreamService {

    public RunnableGraph<NotUsed> source() {
        Source<Integer, NotUsed> source = Source.range(1, 10);

        Flow<Integer, String, NotUsed> flow = Flow.of(Integer.class)
                .map(value -> "The next value is: " + value);

        Sink<String, CompletionStage<Done>> sink = Sink.foreach(log::info);

        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty);

        return source.via(flow).to(sink);
    }

}
