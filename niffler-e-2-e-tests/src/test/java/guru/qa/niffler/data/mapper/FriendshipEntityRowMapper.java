package guru.qa.niffler.data.mapper;

import guru.qa.niffler.data.entity.userdata.FriendshipEntity;
import guru.qa.niffler.data.entity.userdata.FriendshipStatus;
import guru.qa.niffler.data.entity.userdata.UserEntity;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class FriendshipEntityRowMapper implements RowMapper<FriendshipEntity> {

    public static final FriendshipEntityRowMapper instance = new FriendshipEntityRowMapper();

    private FriendshipEntityRowMapper() {
    }

    @Override
    public FriendshipEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        FriendshipEntity friendship = new FriendshipEntity();
        
        UserEntity requester = new UserEntity();
        requester.setId(rs.getObject("requester_id", UUID.class));
        
        UserEntity addressee = new UserEntity();
        addressee.setId(rs.getObject("addressee_id", UUID.class));
        
        friendship.setRequester(requester);
        friendship.setAddressee(addressee);
        friendship.setCreatedDate(rs.getDate("created_date"));
        friendship.setStatus(FriendshipStatus.valueOf(rs.getString("status")));
        
        return friendship;
    }
}

