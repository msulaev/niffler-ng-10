package guru.qa.niffler.data.repository;

import guru.qa.niffler.data.entity.userdata.UserEntity;

import java.util.Optional;
import java.util.UUID;

public interface UserdataUserRepository {
    UserEntity create(UserEntity user);

    Optional<UserEntity> findById(UUID id);

    Optional<UserEntity> findByIdWithFriendships(UUID id);

    void createInvitation(UserEntity requester, UserEntity addressee);

    void createFriendship(UserEntity user1, UserEntity user2);
}
