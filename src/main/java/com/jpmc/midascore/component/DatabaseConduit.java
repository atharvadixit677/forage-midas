package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;

    public DatabaseConduit(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    // "REQUIRES_NEW" forces this to save immediately, even if the Test is still running
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }
}