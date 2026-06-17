package com.example.insight_service.dto;

import java.util.List;

public record UsageDto(
        Long userId,
        List<DeviceDto> devices
) {}
