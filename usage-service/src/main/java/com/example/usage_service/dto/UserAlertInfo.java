package com.example.usage_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

public record UserAlertInfo (
        Double threshold,
        String email
){}
