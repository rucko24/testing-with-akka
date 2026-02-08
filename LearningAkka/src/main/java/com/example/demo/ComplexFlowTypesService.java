package com.example.demo;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionStage;

@Log4j2
@Service
public class ComplexFlowTypesService {

    private final ActorSystem<Object> ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    /**
     * <p>41. Introducing fan-in and fan-out shapes</p>
     * <p>42. Broadcast and merge</p>
     */
    public CompletionStage<Done> buildFlowTypesComplex() {

        var sink = Sink.foreach(log::info);

        return RunnableGraph.fromGraph(GraphDSL.create(sink, (builder, out) -> {

                    var sourceShape = builder.add(Source.range(1, 10));

                    var flow1Shape = builder.add(Flow.of(Integer.class)
                            .map(number -> {
                                log.info("Flow 1 is processing " + number);
                                return (number * 2);
                            }));

                    var flow2Shape = builder.add(Flow.of(Integer.class)
                            .map(number -> {
                                log.info("Flow 2 is processing " + number);
                                return (number + 2);
                            }));

                    UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));

                    UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));

                    builder.from(sourceShape)
                            .viaFanOut(broadcast);

                    builder.from(broadcast.out(0))
                            .via(flow1Shape);

                    builder.from(broadcast.out(1))
                            .via(flow2Shape);

                    builder.from(flow1Shape)
                            .toInlet(merge.in(0));

                    builder.from(flow2Shape)
                            .toInlet(merge.in(1));


                    builder.from(merge)
                            .to(out);


                    return ClosedShape.getInstance();
                })
        ).run(ACTOR_SYSTEM);

    }

    public CompletionStage<Done> buildFlowTypesComplexV2() {

        var sink = Sink.foreach(log::info);

        return RunnableGraph.fromGraph(GraphDSL.create(sink, (builder, out) -> {

                    var sourceShape = builder.add(Source.range(1, 10));

                    var flow1Shape = builder.add(Flow.of(Integer.class)
                            .map(number -> {
                                log.info("Flow 1 is processing " + number);
                                return (number * 2);
                            }));

                    var flow2Shape = builder.add(Flow.of(Integer.class)
                            .map(number -> {
                                log.info("Flow 2 is processing " + number);
                                return (number + 2);
                            }));

                    UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));

                    UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));

                    builder.from(sourceShape)
                            .viaFanOut(broadcast)
                            .via(flow1Shape);

                    builder.from(broadcast)
                            .via(flow2Shape);

                    builder.from(flow1Shape)
                            .viaFanIn(merge)
                            .to(out);

                    builder.from(flow2Shape)
                            .viaFanIn(merge);

                    return ClosedShape.getInstance();
                })
        ).run(ACTOR_SYSTEM);

    }

}
