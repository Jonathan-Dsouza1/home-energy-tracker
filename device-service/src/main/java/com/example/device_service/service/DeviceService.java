package com.example.device_service.service;

import com.example.device_service.dto.DeviceDto;
import com.example.device_service.entity.Device;
import com.example.device_service.exception.DeviceNotFoundException;
import com.example.device_service.repository.DeviceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DeviceService {

    private final DeviceRepository deviceRepository;

    public DeviceDto getDeviceById(Long id) {
        Device device = deviceRepository.findById(id)
                .orElseThrow(() -> new DeviceNotFoundException("Device not found with id: " + id));
        return maptoDto(device);
    }

    public DeviceDto createDevice(DeviceDto deviceDto) {
        Device device = new Device();
        device.setName(deviceDto.getName());
        device.setType(deviceDto.getType());
        device.setLocation(deviceDto.getLocation());
        device.setUserId(deviceDto.getUserId());

        Device savedDevice = deviceRepository.save(device);
        return maptoDto(savedDevice);
    }

    public DeviceDto updateDevice(Long id, DeviceDto input) {
        Device before = deviceRepository.findById(id)
                .orElseThrow(() -> new DeviceNotFoundException("Device not found with id: " + id));

        before.setName(input.getName());
        before.setType(input.getType());
        before.setLocation(input.getLocation());
        before.setUserId(input.getUserId());

        Device after = deviceRepository.save(before);
        return maptoDto(after);
    }

    public void deleteDevice(Long id) {
        if (!deviceRepository.existsById(id)){
            throw new DeviceNotFoundException("Device not found with id: " + id);
        }
        deviceRepository.deleteById(id);
    }

    private DeviceDto maptoDto(Device device){
        DeviceDto dto = new DeviceDto();
        dto.setId(device.getId());
        dto.setName(device.getName());
        dto.setType(device.getType());
        dto.setLocation(device.getLocation());
        dto.setUserId(device.getUserId());
        return dto;
    }
}
