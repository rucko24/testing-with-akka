package com.example.demo;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FlowShape;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Partition;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

@Log4j2
@Service
public class UniFormFanShapesServices {

    private static final ActorSystem<String> ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();


    /**
     *
     * 45. Uniform fan-in and fan-out shapes
     *
     * Using partition Operator
     *
     * @return CompletionStage<Done>
     */
    public CompletionStage<Done> buildRandomNumbers() {

        return RunnableGraph.fromGraph(
                GraphDSL.create(Sink.foreach(log::info), (builder, out) -> {

                    SourceShape<Integer> sourcesShape = builder.add(
                            Source.repeat(1)
                                    .throttle(1, Duration.ofSeconds(1))
                                    .map(item -> 1 + SECURE_RANDOM.nextInt(10))
                    );

                    FlowShape<Integer, Integer> flowShape = builder.add(Flow.of(Integer.class)
                            .map(item -> {
                                log.info("Flowing {}", item);
                                return item;
                            }));

                    UniformFanOutShape<Integer, Integer> partitions = builder.add(
                            Partition.create(2, item -> (item == 0 || item == 10) ? 0 : 1));

                    UniformFanInShape<Integer, Integer> merge = builder.add(
                            Merge.create(2));

                    builder.from(sourcesShape)
                            .viaFanOut(partitions);


                    builder.from(partitions.out(0))
                            .via(flowShape)
                            .viaFanIn(merge)
                            .to(out);

                    builder.from(partitions.out(1))
                            .viaFanIn(merge);

                    return ClosedShape.getInstance();
                })
        ).run(ACTOR_SYSTEM);

//        completable.whenComplete((date, error) -> {
//            if(error == null) {
//                log.info("Success {}");
//            } else {
//                log.info("Failure");
//            }
//        })

    }

}
