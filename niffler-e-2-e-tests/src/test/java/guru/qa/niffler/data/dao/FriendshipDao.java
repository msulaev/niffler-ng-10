package guru.qa.niffler.data.dao;

import guru.qa.niffler.data.entity.userdata.FriendshipEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface FriendshipDao {
    void create(FriendshipEntity friendship);

    Optional<FriendshipEntity> findById(UUID requesterId, UUID addresseeId);

    List<FriendshipEntity> findByRequester(UUID requesterId);

    List<FriendshipEntity> findByAddressee(UUID addresseeId);

    void delete(UUID requesterId, UUID addresseeId);
}

