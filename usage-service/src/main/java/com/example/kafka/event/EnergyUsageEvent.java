package com.example.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;

public record EnergyUsageEvent(
        Long deviceId,
        double energyConsumed,
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp
) {}
