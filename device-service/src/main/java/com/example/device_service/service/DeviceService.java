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
        return mapToDto(device);
    }

    public DeviceDto createDevice(DeviceDto deviceDto) {
        Device device = new Device();
        device.setName(deviceDto.name());
        device.setType(deviceDto.type());
        device.setLocation(deviceDto.location());
        device.setUserId(deviceDto.userId());

        Device savedDevice = deviceRepository.save(device);
        return mapToDto(savedDevice);
    }

    public DeviceDto updateDevice(Long id, DeviceDto input) {
        Device before = deviceRepository.findById(id)
                .orElseThrow(() -> new DeviceNotFoundException("Device not found with id: " + id));

        before.setName(input.name());
        before.setType(input.type());
        before.setLocation(input.location());
        before.setUserId(input.userId());

        Device after = deviceRepository.save(before);
        return mapToDto(after);
    }

    public void deleteDevice(Long id) {
        if (!deviceRepository.existsById(id)){
            throw new DeviceNotFoundException("Device not found with id: " + id);
        }
        deviceRepository.deleteById(id);
    }

    private DeviceDto mapToDto(Device device){
        return new DeviceDto(
                device.getId(),
                device.getName(),
                device.getType(),
                device.getLocation(),
                device.getUserId()
        );
    }
}
