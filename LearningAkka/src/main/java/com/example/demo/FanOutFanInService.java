package com.example.demo;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.japi.tuple.Tuple3;
import akka.stream.ClosedShape;
import akka.stream.FanInShape3;
import akka.stream.FanOutShape3;
import akka.stream.FlowShape;
import akka.stream.SourceShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.UnzipWith;
import akka.stream.javadsl.ZipWith;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionStage;

/**
 *
 * @author rubn
 */
@Log4j2
@Service
public class FanOutFanInService {

    private static final ActorSystem<String> ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    /**
     * FanOut to FanIn
     *
     * 46. Non uniform fan-in and fan-out shapes
     *
     * Estructuras muy flexibles, se puede hacer mucho
     *
     * @return CompletionStage<Done>
     */
    public CompletionStage<Done> buildFanOutFanIn() {

        return RunnableGraph.fromGraph(
                GraphDSL.create(Sink.foreach(log::info), (builder, out) -> {

                    SourceShape<Integer> sourcesShape = builder.add(Source.range(1, 10));

                    FlowShape<Integer, Integer> intergerFlowShape = builder.add(Flow.of(Integer.class)
                            .map(item -> {
                                log.info("Integer flow {}", item);
                                return item;
                            }));

                    FlowShape<Boolean, Boolean> booleanflowShape = builder.add(Flow.of(Boolean.class)
                            .map(item -> {
                                log.info("Boolean flow {}", item);
                                return item;
                            }));

                    FlowShape<String, String> stringflowShape = builder.add(Flow.of(String.class)
                            .map(item -> {
                                log.info("String flow {}", item);
                                return item;
                            }));

                    FanOutShape3<Integer, Integer, Boolean, String> fanOut =
                            builder.add(UnzipWith.create3(input -> new Tuple3<>(input, input % 2 == 0, "It`s " + input)));

                    FanInShape3<Integer, Boolean, String, String> fanIn =
                            builder.add(
                                    ZipWith.create3((a, b, c) -> {
                                        final StringBuilder stringBuilder = new StringBuilder();
                                        stringBuilder.append("The number was " + a);
                                        stringBuilder.append(", which is " + (b ? "even" : "odd"));
                                        stringBuilder.append(", and the String was " + c);
                                        return stringBuilder.toString();
                                    })
                            );

                    builder.from(sourcesShape)
                            .toInlet(fanOut.in());

                    builder.from(fanOut.out0())
                            .via(intergerFlowShape)
                            .toInlet(fanIn.in0());

                    builder.from(fanOut.out1())
                            .via(booleanflowShape)
                            .toInlet(fanIn.in1());

                    builder.from(fanOut.out2())
                            .via(stringflowShape)
                            .toInlet(fanIn.in2());

                    builder.from(fanIn.out())
                            .to(out);

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
