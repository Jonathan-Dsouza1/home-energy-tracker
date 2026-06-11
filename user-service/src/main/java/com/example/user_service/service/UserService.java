package com.example.user_service.service;

import com.example.user_service.dto.UserDto;
import com.example.user_service.entity.User;
import com.example.user_service.exception.UserNotFoundException;
import com.example.user_service.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;

    public UserDto getUserById(Long id) {
        return userRepository.findById(id)
                .map(this::toDto)
                .orElseThrow(() -> new UserNotFoundException("User not found with id: " + id));
    }

    public UserDto createUser(UserDto input){
        final User createdUser = User.builder()
                .name(input.name())
                .surname(input.surname())
                .email(input.email())
                .address(input.address())
                .alerting(input.alerting())
                .energyAlertingThreshold(input.energyAlertingThreshold())
                .build();

        final User saved = userRepository.save(createdUser);

        return toDto(saved);
    }

    public void updateUser(Long id, UserDto userDto){
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException("User not found with id: " + id));

        user.setName(userDto.name());
        user.setSurname(userDto.surname());
        user.setEmail(userDto.email());
        user.setAddress(userDto.address());
        user.setAlerting(userDto.alerting());
        user.setEnergyAlertingThreshold(userDto.energyAlertingThreshold());

        userRepository.save(user);
    }

    public void deleteUser(Long id){
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException("User not found with id: " + id));

        userRepository.delete(user);
    }

    private UserDto toDto(User user){
        return new UserDto(
                user.getId(),
                user.getName(),
                user.getSurname(),
                user.getEmail(),
                user.getAddress(),
                user.isAlerting(),
                user.getEnergyAlertingThreshold()
        );
    }
}
