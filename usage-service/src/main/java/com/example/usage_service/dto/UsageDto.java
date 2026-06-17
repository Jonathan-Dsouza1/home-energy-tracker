package com.example.usage_service.dto;

import java.util.List;

public record UsageDto(
        Long userId,
        List<DeviceDto> devices
) {}
