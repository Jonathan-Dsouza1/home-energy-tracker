package com.example.insight_service.client;

import com.example.insight_service.dto.UsageDto;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Component
@RequiredArgsConstructor
public class UsageClient {

    private final RestTemplate restTemplate;

    @Value("${usage.service,url}")
    private final String baseUrl;

    public UsageDto getXDaysUsageForUser(Long userId, int days) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl)
                .path("/{userId}")
                .queryParam("days", days)
                .buildAndExpand(userId)
                .toUriString();

        ResponseEntity<UsageDto> response = restTemplate.getForEntity(url, UsageDto.class);
        return response.getBody();
    }
}
