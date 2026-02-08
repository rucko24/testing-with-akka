package com.example.demo;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionStage;

/**
 * 43. Using Balance for parallelilsm
 */
@Log4j2
@Service
public class ExploringConcurrencyService {


    private static final ActorSystem<Behaviors> ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    /**
     * 43. Using Balance for parallelilsm
     * <p>
     * Using async() with Flow.of()
     *
     */
    public CompletionStage<Done> balanceParallelilms() {

        Long start = System.currentTimeMillis();

        var grafo = RunnableGraph.fromGraph(
                GraphDSL.create(Sink.foreach(log::info), (builder, out) -> {

                    var sourceShape = builder.add(Source.range(1, 10));

                    UniformFanOutShape<Integer, Integer> balance = builder.add(Balance.create(4, true));
                    UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(4));

                    builder.from(sourceShape)
                            .viaFanOut(balance);

                    for (int i = 0; i < 4; i++) {
                        builder.from(balance)
                                .via(builder.add(Flow.of(Integer.class)
                                                .map(item -> {
                                                    log.info("Starting flow {}", item);
                                                    Thread.sleep(3000);
                                                    log.info("Finishing flow {}", item);
                                                    return item;
                                                }).async()
                                        )
                                )
                                .toFanIn(merge);
                    }

                    builder.from(merge)
                            .to(out);

                    return ClosedShape.getInstance();
                })

        ).run(ACTOR_SYSTEM);

        return grafo.whenComplete((date, error) -> {
            if (error == null) {
                log.info("Success {}");
                Long end = System.currentTimeMillis();
                log.info("Time taken {} " + (end - start), "(ms)");
                ACTOR_SYSTEM.terminate();
            } else {
                log.info("Failure {}");
                Long end = System.currentTimeMillis();
                log.info("Time taken {} " + (end - start), "(ms)");
                ACTOR_SYSTEM.terminate();
            }
        });

    }

}
