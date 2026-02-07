package com.example.demo.monitorinspeed;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FlowShape;
import akka.stream.SourceShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.log4j.Log4j2;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
@Service
public class VehicleSpeedService {

    private static final ActorSystem ACTOR_SYSTEM = ActorSystem.create(Behaviors.empty(), "actor-system");

    private final Map<Integer, VehiclePositionMessage> vehiclePositions = new ConcurrentHashMap<>();


    public CompletionStage<VehicleSpeed> buildVehicleTracking() {


        for (int i = 0; i <= 8; i++) {
            vehiclePositions.put(i, new VehiclePositionMessage(1, new Date(), 0, 0));
        }

        //Source repeat some value every 10 seconds
        return Source.repeat("go")
                .throttle(1, Duration.ofSeconds(10))
                //Flow 1 - transform into the ids of each van (ie 1..8) with mapConcat
                .via(this.vehicleIds())
                //Flow 2 - get position for each van as a VPMS with a call to the lookup method (create a new
                //utility functions each time), Note that this process isn`t instant so should be run in paralell
                .async()
                .via(this.vehiclePosition(vehiclePositions))
                .async()
                //Flow 3-use previos position from the map to calculate the current speed of each vehicle
                //position in the map with the newest position and pass the current speed downstream
                .via(this.vehicleSpeed(vehiclePositions))
                //flow 4 - filter to only keep those values with a speed > 95
                .filter(speed -> speed.getSpeed() > 95)
                //Sink - as soon as 1 value is received return it as a materialized value, and terminate the stream
                .toMat(Sink.head(), Keep.right())
                .run(ACTOR_SYSTEM)
                .whenComplete((data, throwable) -> {
                    if (throwable != null) {
                        log.info("Something went wrong {}", throwable.getMessage());
                    } else {
                        log.info("Vehicle id: {} is was going at speed: {}", data.getVehicleId(), data.getSpeed());
                    }
                    ACTOR_SYSTEM.terminate();
                });
    }

    private Flow<String, Integer, NotUsed> vehicleIds() {
        return Flow.of(String.class)
                .mapConcat(item -> List.of(1, 2, 3, 4, 5, 6, 7, 8));
    }

    /**
     * 37. Why do we need the GraphDSL?
     */
    public CompletionStage<Object> buildGraphDSL() {

        for (int i = 0; i <= 8; i++) {
            vehiclePositions.put(i, new VehiclePositionMessage(1, new Date(), 0, 0));
        }

        CompletionStage<Object> runnableGraph = RunnableGraph.fromGraph(
                        GraphDSL.create(Sink.head(), (builder, out) -> {

                            SourceShape<String> sourceShape = builder.add(Source.repeat("go")
                                    .throttle(1, Duration.ofSeconds(1)));

                            FlowShape<String, Integer> vehicleIdsShape = builder.add(this.vehicleIds());

                            FlowShape<Integer, VehiclePositionMessage> vehiclePositionsShape = builder.add(this.vehiclePosition(vehiclePositions));

                            FlowShape<VehiclePositionMessage, VehicleSpeed> vehicleSpeedShape = builder.add(this.vehicleSpeed(vehiclePositions));

                            FlowShape<VehicleSpeed, VehicleSpeed> filterSpeedShape = builder.add(Flow.of(VehicleSpeed.class)
                                    .filter(vehicleSpeed -> vehicleSpeed.getSpeed() > 95));

                            //No hace falta hacer este paso
                            //builder.add(sink);

                            builder.from(sourceShape)
                                    .to(out);

                            builder.from(vehicleIdsShape)
                                    .to(out);

                            builder.from(vehiclePositionsShape)
                                    .to(out);

                            builder.from(vehicleSpeedShape)
                                    .to(out);

                            builder.from(filterSpeedShape)
                                    .to(out);

                            return ClosedShape.getInstance();
                        })
                )
                .run(ACTOR_SYSTEM);

        return runnableGraph.whenComplete((data, throwable) -> {
            if (throwable != null) {
                log.info("Something went wrong {}", throwable.getMessage());
            } else {
                //log.info("Vehicle id: {} is was going at speed: {}", data.getVehicleId(), data.getSpeed());
            }
            ACTOR_SYSTEM.terminate();
        });

    }

    private @NonNull Flow<VehiclePositionMessage, VehicleSpeed, NotUsed> vehicleSpeed(Map<Integer, VehiclePositionMessage> vehiclePositions) {
        return Flow.of(VehiclePositionMessage.class)
                .map(vehiclePositionsMessage -> {
                    var cachedPositionMessage = vehiclePositions.get(vehiclePositionsMessage.getVehicleId());
                    var speed = UtilityFunctions.calculateSpeed(vehiclePositionsMessage, cachedPositionMessage);
                    log.info("Vehicle id: {} is travelling at: {}", vehiclePositionsMessage.getVehicleId(), speed.getSpeed());
                    vehiclePositions.put(vehiclePositionsMessage.getVehicleId(), vehiclePositionsMessage);
                    return speed;
                });
    }

    private @NonNull Flow<Integer, VehiclePositionMessage, NotUsed> vehiclePosition(Map<Integer, VehiclePositionMessage> vehiclePositions) {
        return Flow.of(Integer.class)
                .mapAsyncUnordered(1, vehicleId -> {
                    log.info("Requesting position for vehicle {}", vehicleId);
                    return CompletableFuture.supplyAsync(() -> {
                        var cachedVehicleId = vehiclePositions.get(vehicleId).getVehicleId();
                        return UtilityFunctions.getVehiclePosition(cachedVehicleId);
                    });
                });
    }

}
