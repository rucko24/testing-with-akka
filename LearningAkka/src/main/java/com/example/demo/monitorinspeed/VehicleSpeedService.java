package com.example.demo.monitorinspeed;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
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

    public CompletionStage<VehicleSpeed> buildVehicleTracking() {

        final Map<Integer, VehiclePositionMessage> vehiclePositions = new ConcurrentHashMap<>();

        for (int i = 0; i <= 8; i++) {
            vehiclePositions.put(i, new VehiclePositionMessage(1, new Date(), 0, 0));
        }

        //Source repeat some value every 10 seconds
        return Source.repeat("go")
                .throttle(1, Duration.ofSeconds(10))
                //Flow 1 - transform into the ids of each van (ie 1..8) with mapConcat
                .via(Flow.of(String.class)
                        .mapConcat(item -> List.of(1, 2, 3, 4, 5, 6, 7, 8)))
                //Flow 2 - get position for each van as a VPMS with a call to the lookup method (create a new
                //utility functions each time), Note that this process isn`t instant so should be run in paralell
                .async()
                .via(vehiclePosition(vehiclePositions))
                .async()
                //Flow 3-use previos position from the map to calculate the current speed of each vehicle
                //position in the map with the newest position and pass the current speed downstream
                .via(vehicleSpeed(vehiclePositions))
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

    private @NonNull Flow<VehiclePositionMessage, VehicleSpeed, NotUsed> vehicleSpeed(Map<Integer, VehiclePositionMessage> vehiclePositions) {
        return Flow.of(VehiclePositionMessage.class)
                .map(vehiclePositionsMessage -> {
                    var cachedPositionMessage = vehiclePositions.get(vehiclePositionsMessage.getVehicleId());
                    var speed = UtilityFunctions.calculateSpeed(vehiclePositionsMessage, cachedPositionMessage);
                    log.info("Vehicle id: {} is travelling at: {}", vehiclePositionsMessage.getVehicleId(), speed);
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
