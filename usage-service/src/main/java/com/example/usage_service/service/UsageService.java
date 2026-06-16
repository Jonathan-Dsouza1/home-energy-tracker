package com.example.usage_service.service;

import com.example.kafka.event.AlertingEvent;
import com.example.kafka.event.EnergyUsageEvent;
import com.example.usage_service.client.DeviceClient;
import com.example.usage_service.client.UserClient;
import com.example.usage_service.dto.DeviceDto;
import com.example.usage_service.dto.UserAlertInfo;
import com.example.usage_service.dto.UserDto;
import com.example.usage_service.model.DeviceEnergy;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
public class UsageService {

    private static final String ENERGY_USAGE_MEASUREMENT = "energy-usage";

    private InfluxDBClient influxDBClient;
    private DeviceClient deviceClient;
    private UserClient userClient;

    @Value("${influx.bucket}")
    private String influxBucket;

    @Value("${influx.org}")
    private String influxOrg;

    private final KafkaTemplate<String, AlertingEvent> kafkaTemplate;

    public UsageService(InfluxDBClient influxDBClient,
                        DeviceClient deviceClient,
                        UserClient userClient,
                        KafkaTemplate<String, AlertingEvent> kafkaTemplate) {
        this.influxDBClient = influxDBClient;
        this.deviceClient = deviceClient;
        this.userClient = userClient;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "energy-usage", groupId = "usage-service")
    public void energyUsageEvent(EnergyUsageEvent energyUsageEvent) {
//        log.info("Received energy usage event: {}", energyUsageEvent);
        Point point = Point.measurement("energy-usage")
                .addTag("deviceId", String.valueOf(energyUsageEvent.deviceId()))
                .addField("energyConsumed", energyUsageEvent.energyConsumed())
                .time(energyUsageEvent.timestamp(), WritePrecision.MS);
        influxDBClient.getWriteApiBlocking().writePoint(influxBucket, influxOrg, point);
    }

    @Scheduled(cron = "*/10 * * * * *")
    public void aggregateDeviceEnergyUsage() {
        final Instant now = Instant.now();
        final List<DeviceEnergy> deviceEnergies = getDeviceEnergies(now.minusSeconds(3600), now);

        log.info("Aggregated device energies over the past hour: {}", deviceEnergies);

        addUserIds(deviceEnergies);

        final Map<Long, List<DeviceEnergy>> userDeviceEnergyMap = deviceEnergies.stream()
                .filter(deviceEnergy -> deviceEnergy.getUserId() != null)
                .collect(Collectors.groupingBy(DeviceEnergy::getUserId));

        log.info("User-Device Energy Map: {}", userDeviceEnergyMap);

        getAlertingUsers(userDeviceEnergyMap.keySet())
                .forEach((userId, alertInfo) ->
                        sendThresholdAlertIfNeeded(userId, userDeviceEnergyMap.get(userId), alertInfo));
    }

    private List<DeviceEnergy> getDeviceEnergies(final Instant start, final Instant stop) {
        String fluxQuery = String.format("""
                from(bucket: "%s")
                    |> range(start: time(v: "%s"), stop: time(v: "%s"))
                    |> filter(fn: (r) => r["_measurement"] == "%s")
                    |> filter(fn: (r) => r["_field"] == "energyConsumed")
                    |> group(columns: ["deviceId"])
                    |> sum(column: "_value")
                """, influxBucket, start, stop, "energy-usage");

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery, influxOrg);

        return tables.stream()
                .flatMap(table -> table.getRecords().stream())
                .map(this::toDeviceEnergy)
                .toList();
    }

    private DeviceEnergy toDeviceEnergy(final FluxRecord record) {
        final Object value = record.getValueByKey("_value");

        return DeviceEnergy.builder()
                .deviceId(Long.valueOf((String) record.getValueByKey("deviceId")))
                .energyConsumed(value instanceof Number number ? number.doubleValue() : 0.0)
                .build();
    }

    private void addUserIds(final List<DeviceEnergy> deviceEnergies) {
        for (DeviceEnergy deviceEnergy : deviceEnergies) {
            try {
                final DeviceDto deviceResponse = deviceClient.getDeviceById(deviceEnergy.getDeviceId());
                if (deviceResponse == null || deviceResponse.id() == null) {
                    log.warn("Device not found for ID: {}", deviceEnergy.getDeviceId());
                    continue;
                }
                deviceEnergy.setUserId(deviceResponse.userId());
            } catch (Exception e) {
                log.warn("Failed to fetch device for ID: {}", deviceEnergy.getDeviceId());
            }
        }
    }

    private Map<Long, UserAlertInfo> getAlertingUsers(final Set<Long> userIds) {
        return userIds.stream()
                .flatMap(this::getAlertingUser)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Stream<Map.Entry<Long, UserAlertInfo>> getAlertingUser(final Long userId) {
        try {
            UserDto user = userClient.getUserById(userId);
            if (user == null || user.id() == null || !user.alerting()) {
                log.warn("User not found or alerting disabled for ID: {}", userId);
                return Stream.empty();
            }

            return Stream.of(Map.entry(userId,
                    new UserAlertInfo(user.energyAlertingThreshold(), user.email())));
        } catch (Exception e) {
            log.warn("Failed to fetch user for ID: {}", userId);
            return Stream.empty();
        }
    }

    private void sendThresholdAlertIfNeeded(final Long userId,
                                            final List<DeviceEnergy> devices,
                                            final UserAlertInfo alertInfo) {
        final double totalConsumption = devices.stream()
                .mapToDouble(DeviceEnergy::getEnergyConsumed)
                .sum();

        if (totalConsumption > alertInfo.threshold()) {
            log.info("ALERT: User ID {} has exceeded the energy threshold! " +
                            "Total Consumption: {},Threshold: {}",
                    userId, totalConsumption, alertInfo.threshold());

            final AlertingEvent alertingEvent = AlertingEvent.builder()
                    .userId(userId)
                    .message("Energy consumption threshold exceeded")
                    .threshold(alertInfo.threshold())
                    .energyConsumed(totalConsumption)
                    .email(alertInfo.email())
                    .build();

            kafkaTemplate.send("energy-alerts", alertingEvent);
        } else {
            log.info("User ID {} is within the energy threshold. " +
                            "Total Consumption: {}, Threshold: {}",
                    userId, totalConsumption, alertInfo.threshold());
        }
    }
}
