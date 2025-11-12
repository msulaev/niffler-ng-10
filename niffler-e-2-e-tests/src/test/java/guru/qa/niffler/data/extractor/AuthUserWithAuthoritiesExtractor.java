package guru.qa.niffler.data.extractor;

import guru.qa.niffler.data.entity.auth.AuthUserEntity;
import guru.qa.niffler.data.entity.auth.Authority;
import guru.qa.niffler.data.entity.auth.AuthorityEntity;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class AuthUserWithAuthoritiesExtractor implements ResultSetExtractor<Optional<AuthUserEntity>> {

    @Override
    public Optional<AuthUserEntity> extractData(ResultSet rs) throws SQLException, DataAccessException {
        Map<UUID, AuthUserEntity> userMap = new HashMap<>();
        
        while (rs.next()) {
            UUID userId = rs.getObject("id", UUID.class);
            
            AuthUserEntity user = userMap.get(userId);
            if (user == null) {
                user = new AuthUserEntity();
                user.setId(userId);
                user.setUsername(rs.getString("username"));
                user.setPassword(rs.getString("password"));
                user.setEnabled(rs.getBoolean("enabled"));
                user.setAccountNonExpired(rs.getBoolean("account_non_expired"));
                user.setAccountNonLocked(rs.getBoolean("account_non_locked"));
                user.setCredentialsNonExpired(rs.getBoolean("credentials_non_expired"));
                user.setAuthorities(new ArrayList<>());
                userMap.put(userId, user);
            }
            
            AuthorityEntity authority = new AuthorityEntity();
            authority.setId(rs.getObject("authority_id", UUID.class));
            authority.setAuthority(Authority.valueOf(rs.getString("authority")));
            authority.setUser(user);
            
            user.getAuthorities().add(authority);
        }
        
        return userMap.values().stream().findFirst();
    }
}

