package guru.qa.niffler.data.repository.impl;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.dao.FriendshipDao;
import guru.qa.niffler.data.dao.UserDataUserDao;
import guru.qa.niffler.data.entity.userdata.FriendshipEntity;
import guru.qa.niffler.data.entity.userdata.FriendshipStatus;
import guru.qa.niffler.data.entity.userdata.UserEntity;
import guru.qa.niffler.data.extractor.UserWithFriendshipsExtractor;
import guru.qa.niffler.data.impl.FriendshipDaoJdbc;
import guru.qa.niffler.data.impl.UserDataUserDaoJdbc;
import guru.qa.niffler.data.repository.UserdataUserRepository;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static guru.qa.niffler.data.tpl.Connections.holder;

public class UserdataUserRepositoryJdbc implements UserdataUserRepository {

    private static final Config CFG = Config.getInstance();
    
    private final UserDataUserDao userDataUserDao = new UserDataUserDaoJdbc();
    private final FriendshipDao friendshipDao = new FriendshipDaoJdbc();

    @Override
    public UserEntity create(UserEntity user) {
        return userDataUserDao.createUser(user);
    }

    @Override
    public Optional<UserEntity> findById(UUID id) {
        return userDataUserDao.findById(id);
    }

    @Override
    public Optional<UserEntity> findByIdWithFriendships(UUID id) {
        try (PreparedStatement ps = holder(CFG.userdataJdbcUrl()).connection().prepareStatement(
                "SELECT u.id, u.username, u.currency, u.firstname, u.surname, u.full_name, u.photo, u.photo_small, " +
                        "f.requester_id, f.addressee_id, f.created_date, f.status, " +
                        "req.id as req_id, req.username as req_username, req.currency as req_currency, " +
                        "addr.id as addr_id, addr.username as addr_username, addr.currency as addr_currency " +
                        "FROM \"user\" u " +
                        "LEFT JOIN friendship f ON (u.id = f.requester_id OR u.id = f.addressee_id) " +
                        "LEFT JOIN \"user\" req ON f.requester_id = req.id " +
                        "LEFT JOIN \"user\" addr ON f.addressee_id = addr.id " +
                        "WHERE u.id = ?"
        )) {
            ps.setObject(1, id);
            
            ps.execute();
            
            try (ResultSet rs = ps.getResultSet()) {
                return new UserWithFriendshipsExtractor().extractData(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createInvitation(UserEntity requester, UserEntity addressee) {
        FriendshipEntity invitation = new FriendshipEntity();
        invitation.setRequester(requester);
        invitation.setAddressee(addressee);
        invitation.setCreatedDate(new Date());
        invitation.setStatus(FriendshipStatus.PENDING);
        
        friendshipDao.create(invitation);
    }

    @Override
    public void createFriendship(UserEntity user1, UserEntity user2) {
        FriendshipEntity friendship1 = new FriendshipEntity();
        friendship1.setRequester(user1);
        friendship1.setAddressee(user2);
        friendship1.setCreatedDate(new Date());
        friendship1.setStatus(FriendshipStatus.ACCEPTED);
        
        FriendshipEntity friendship2 = new FriendshipEntity();
        friendship2.setRequester(user2);
        friendship2.setAddressee(user1);
        friendship2.setCreatedDate(new Date());
        friendship2.setStatus(FriendshipStatus.ACCEPTED);
        
        friendshipDao.create(friendship1);
        friendshipDao.create(friendship2);
    }
}
