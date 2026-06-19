package com.example.usage_service.service;

import com.example.kafka.event.AlertingEvent;
import com.example.kafka.event.EnergyUsageEvent;
import com.example.usage_service.client.DeviceClient;
import com.example.usage_service.client.UserClient;
import com.example.usage_service.dto.DeviceDto;
import com.example.usage_service.dto.UsageDto;
import com.example.usage_service.dto.UserAlertInfo;
import com.example.usage_service.dto.UserDto;
import com.example.usage_service.model.Device;
import com.example.usage_service.model.DeviceEnergy;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class UsageService {

    private static final String ENERGY_USAGE_MEASUREMENT = "energy-usage";

    private final InfluxDBClient influxDBClient;
    private final DeviceClient deviceClient;
    private final UserClient userClient;
    private final KafkaTemplate<String, AlertingEvent> kafkaTemplate;

    @Value("${influx.bucket}")
    private String influxBucket;

    @Value("${influx.org}")
    private String influxOrg;

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

    public UsageDto getXDaysUsageForUser(Long userId, int days) {
        log.info("Getting usage for userId {} over past {} days", userId, days);
        final List<DeviceDto> devicesDto = deviceClient.getAllDevicesForUser(userId);

        final List<Device> devices = new ArrayList<>();
        for (DeviceDto deviceDto : devicesDto) {
            devices.add(Device.builder()
                    .id(deviceDto.id())
                    .name(deviceDto.name())
                    .type(deviceDto.type())
                    .location(deviceDto.location())
                    .userId(deviceDto.userId())
                    .build());
        }

        if (devices == null || devices.isEmpty()) {
            return UsageDto.builder()
                    .userId(userId)
                    .devices(null)
                    .build();
        }

        // build a set of device ids to filter on Flux query
        List<String> deviceIdStrings = devices.stream()
                .map(Device::getId)
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .toList();

        final Instant now = Instant.now();
        final Instant start = now.minusSeconds((long) days * 24 * 3600);

        // build device filter "r[\"deviceId\"] == \"1\" or r[\"deviceId\"] == \"2\""
        final String deviceFilter = deviceIdStrings.stream()
                .map(idStr -> String.format("r[\"deviceId\"] == \"%s\"", idStr))
                .collect(Collectors.joining(" or "));

        String fluxQuery = String.format("""
                from(bucket: "%s")
                  |> range(start: time(v: "%s"), stop: time(v: "%s"))
                  |> filter(fn: (r) => r["_measurement"] == "energy-usage")
                  |> filter(fn: (r) => r["_field"] == "energyConsumed")
                  |> filter(fn: (r) => %s)
                  |> group(columns: ["deviceId"])
                  |> sum(column: "_value")
                """, influxBucket, start.toString(), now.toString(), deviceFilter);

        final Map<Long, Double> aggregatedMap = new HashMap<>();

        try {
            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> tables = queryApi.query(fluxQuery, influxOrg);

            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    Object deviceIdObj = record.getValueByKey("deviceId");
                    String deviceIdStr = deviceIdObj == null ? null : deviceIdObj.toString();
                    if (deviceIdStr == null) continue;

                    Double energyConsumed = record.getValueByKey("_value") instanceof Number
                            ? ((Number) record.getValueByKey("_value")).doubleValue()
                            : 0.0;

                    try {
                        Long deviceId = Long.valueOf(deviceIdStr);
                        aggregatedMap.put(deviceId, aggregatedMap.getOrDefault(deviceId, 0.0) + energyConsumed);
                    } catch (NumberFormatException nfe) {
                        log.warn("Failed to parse deviceId from flux record: {}", deviceIdStr);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to query InfluxDB for user {} usage over {} days: {}", userId, days, e.getMessage());
            // set aggregatedConsumption to 0.0 on error
            devices.forEach(d -> d.setEnergyConsumed(0.0));
            return UsageDto.builder()
                    .userId(userId)
                    .devices(null)
                    .build();
        }

        // populate aggregated energy consumed per device
        for (Device device : devices) {
            if (device == null || device.getId() == null) continue;
            device.setEnergyConsumed(aggregatedMap.getOrDefault(device.getId(), 0.0));
        }

        log.info("Aggregated energy consumption for userId {}: {}", userId, aggregatedMap);

        final List<DeviceDto> resultDevices = devices.stream()
                .map(d -> DeviceDto.builder()
                        .id(d.getId())
                        .name(d.getName())
                        .type(d.getType())
                        .location(d.getLocation())
                        .userId(d.getUserId())
                        .energyConsumed(d.getEnergyConsumed())
                        .build())
                .toList();

        return UsageDto.builder()
                .userId(userId)
                .devices(resultDevices)
                .build();
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
