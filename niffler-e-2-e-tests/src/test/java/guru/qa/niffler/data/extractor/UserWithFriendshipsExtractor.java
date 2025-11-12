package guru.qa.niffler.data.extractor;

import guru.qa.niffler.data.entity.userdata.FriendshipEntity;
import guru.qa.niffler.data.entity.userdata.FriendshipStatus;
import guru.qa.niffler.data.entity.userdata.UserEntity;
import guru.qa.niffler.model.CurrencyValues;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class UserWithFriendshipsExtractor implements ResultSetExtractor<Optional<UserEntity>> {

    @Override
    public Optional<UserEntity> extractData(ResultSet rs) throws SQLException, DataAccessException {
        Map<UUID, UserEntity> userMap = new HashMap<>();
        UserEntity mainUser = null;
        
        while (rs.next()) {
            UUID mainUserId = rs.getObject("id", UUID.class);
            
            if (mainUser == null) {
                mainUser = new UserEntity();
                mainUser.setId(mainUserId);
                mainUser.setUsername(rs.getString("username"));
                mainUser.setCurrency(CurrencyValues.valueOf(rs.getString("currency")));
                mainUser.setFirstname(rs.getString("firstname"));
                mainUser.setSurname(rs.getString("surname"));
                mainUser.setFullname(rs.getString("full_name"));
                mainUser.setPhoto(rs.getBytes("photo"));
                mainUser.setPhotoSmall(rs.getBytes("photo_small"));
                mainUser.setFriendshipRequests(new ArrayList<>());
                mainUser.setFriendshipAddressees(new ArrayList<>());
            }
            
            UUID requesterId = rs.getObject("requester_id", UUID.class);
            if (requesterId == null) {
                continue;
            }
            
            UUID addresseeId = rs.getObject("addressee_id", UUID.class);
            
            UserEntity requester = userMap.get(requesterId);
            if (requester == null) {
                if (requesterId.equals(mainUserId)) {
                    requester = mainUser;
                } else {
                    requester = new UserEntity();
                    requester.setId(requesterId);
                    requester.setUsername(rs.getString("req_username"));
                    String reqCurrency = rs.getString("req_currency");
                    if (reqCurrency != null) {
                        requester.setCurrency(CurrencyValues.valueOf(reqCurrency));
                    }
                }
                userMap.put(requesterId, requester);
            }
            
            UserEntity addressee = userMap.get(addresseeId);
            if (addressee == null) {
                if (addresseeId.equals(mainUserId)) {
                    addressee = mainUser;
                } else {
                    addressee = new UserEntity();
                    addressee.setId(addresseeId);
                    addressee.setUsername(rs.getString("addr_username"));
                    String addrCurrency = rs.getString("addr_currency");
                    if (addrCurrency != null) {
                        addressee.setCurrency(CurrencyValues.valueOf(addrCurrency));
                    }
                }
                userMap.put(addresseeId, addressee);
            }
            
            FriendshipEntity friendship = new FriendshipEntity();
            friendship.setRequester(requester);
            friendship.setAddressee(addressee);
            friendship.setCreatedDate(rs.getDate("created_date"));
            friendship.setStatus(FriendshipStatus.valueOf(rs.getString("status")));
            
            if (requesterId.equals(mainUserId)) {
                mainUser.getFriendshipRequests().add(friendship);
            } else {
                mainUser.getFriendshipAddressees().add(friendship);
            }
        }
        
        return Optional.ofNullable(mainUser);
    }
}

