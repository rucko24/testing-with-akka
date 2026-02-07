package com.example.demo.monitorinspeed;

public class VehicleSpeed {

    private int vehicleId;
    private double speed;

    public VehicleSpeed(int vehicleId, double speed) {
        this.vehicleId = vehicleId;
        this.speed= speed;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public double getSpeed() {
        return speed;
    }
}
