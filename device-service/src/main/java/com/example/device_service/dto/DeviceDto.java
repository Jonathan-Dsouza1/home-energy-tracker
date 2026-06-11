package com.example.device_service.dto;

import com.example.device_service.model.DeviceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public record DeviceDto (
        Long id,
        String name,
        DeviceType type,
        String location,
        Long userId
) {}
