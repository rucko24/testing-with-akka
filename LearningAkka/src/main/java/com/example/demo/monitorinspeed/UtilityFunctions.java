package com.example.demo.monitorinspeed;

import lombok.experimental.UtilityClass;

import java.util.Date;
import java.util.Random;

@UtilityClass
public class UtilityFunctions {

    public static VehiclePositionMessage getVehiclePosition(int vehicleId) {
        //simulate some time to get a response from the vehicle
        Random r = new Random();
        try {
            Thread.sleep(1000 * r.nextInt(5));
        } catch (InterruptedException e) {
        }
        return new VehiclePositionMessage(vehicleId, new Date(), r.nextInt(100), r.nextInt(100));
    }

    public static VehicleSpeed calculateSpeed(VehiclePositionMessage position1, VehiclePositionMessage position2) {
        double longDistance = Math.abs(position1.getLongPosition() - position2.getLongPosition());
        double latDistance = Math.abs(position1.getLatPosition() - position2.getLatPosition());
        double distanceTravelled = Math.pow((Math.pow(longDistance, 2) + Math.pow(latDistance, 2)), 0.5);
        long time = Math.max(1, Math.abs(position1.getCurrentDateTime().getTime() - position2.getCurrentDateTime().getTime()) / 1000);
        double speed = distanceTravelled * 10 / time;
        if (position2.getLongPosition() == 0 && position2.getLatPosition() == 0) speed = 0;
        if (speed > 120) speed = 50;
        return new VehicleSpeed(position1.getVehicleId(), speed);
    }
}
