package guru.qa.niffler.test.db;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.entity.auth.AuthUserEntity;
import guru.qa.niffler.data.entity.auth.Authority;
import guru.qa.niffler.data.entity.auth.AuthorityEntity;
import guru.qa.niffler.data.entity.userdata.UserEntity;
import guru.qa.niffler.data.impl.AuthAuthorityDaoJdbc;
import guru.qa.niffler.data.impl.AuthAuthorityDaoSpringJdbc;
import guru.qa.niffler.data.impl.AuthUserDaoJdbc;
import guru.qa.niffler.data.impl.AuthUserDaoSpringJdbc;
import guru.qa.niffler.data.impl.UserDataUserDaoJdbc;
import guru.qa.niffler.data.impl.UserDataDaoSpringJdbc;
import guru.qa.niffler.data.tpl.ChainedTransactionTemplate;
import guru.qa.niffler.data.tpl.Connections;
import guru.qa.niffler.data.tpl.JdbcTransactionTemplate;
import guru.qa.niffler.model.CurrencyValues;
import guru.qa.niffler.utils.RandomDataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class JdbcTransactionInvariantsTest {

    private static final Config CFG = Config.getInstance();

    @AfterEach
    void cleanup() {
        Connections.closeAllConnections();
    }

    @Test
    void jdbcWithoutTransaction_Success() {
        String testUsername = RandomDataUtils.randomUsername();
        
        Connection connection = Connections.holder(CFG.authJdbcUrl()).connection();
        try {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO \"user\" (username, password, enabled, account_non_expired, account_non_locked, credentials_non_expired) " +
                            "VALUES (?, ?, ?, ?, ?, ?)",
                    PreparedStatement.RETURN_GENERATED_KEYS
            );
            ps.setString(1, testUsername);
            ps.setString(2, "password");
            ps.setBoolean(3, true);
            ps.setBoolean(4, true);
            ps.setBoolean(5, true);
            ps.setBoolean(6, true);
            ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    void jdbcWithTransaction_Rollback() {
        String testUsername = RandomDataUtils.randomUsername();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.authJdbcUrl());

        assertThrows(RuntimeException.class, () ->
                txTemplate.execute(() -> {
                    AuthUserEntity authUser = new AuthUserEntity();
                    authUser.setUsername(testUsername);
                    authUser.setPassword("password");
                    authUser.setEnabled(true);
                    authUser.setAccountNonExpired(true);
                    authUser.setAccountNonLocked(true);
                    authUser.setCredentialsNonExpired(true);
                    
                    new AuthUserDaoJdbc().create(authUser);
                    
                    throw new RuntimeException("Simulated failure");
                })
        );
    }

    @Test
    void jdbcWithTransaction_Success() {
        String testUsername = RandomDataUtils.randomUsername();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.authJdbcUrl());

        AuthUserEntity createdUser = txTemplate.execute(() -> {
            AuthUserEntity authUser = new AuthUserEntity();
            authUser.setUsername(testUsername);
            authUser.setPassword("password");
            authUser.setEnabled(true);
            authUser.setAccountNonExpired(true);
            authUser.setAccountNonLocked(true);
            authUser.setCredentialsNonExpired(true);
            
            return new AuthUserDaoJdbc().create(authUser);
        });

        assertNotNull(createdUser.getId());
        assertEquals(testUsername, createdUser.getUsername());
    }

    @Test
    void springJdbcWithoutTransaction_NoRollback() {
        String testUsername = RandomDataUtils.randomUsername();

        AuthUserEntity authUser = new AuthUserEntity();
        authUser.setUsername(testUsername);
        authUser.setPassword("password");
        authUser.setEnabled(true);
        authUser.setAccountNonExpired(true);
        authUser.setAccountNonLocked(true);
        authUser.setCredentialsNonExpired(true);

        AuthUserEntity createdUser = new AuthUserDaoSpringJdbc().create(authUser);
        assertNotNull(createdUser.getId());

        Optional<AuthUserEntity> foundUser = new AuthUserDaoSpringJdbc().findById(createdUser.getId());
        assertTrue(foundUser.isPresent(), "User should be committed immediately");
    }

    @Test
    void springJdbcWithTransaction_Rollback() {
        String testUsername = RandomDataUtils.randomUsername();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.authJdbcUrl());

        assertThrows(RuntimeException.class, () ->
                txTemplate.execute(() -> {
                    AuthUserEntity authUser = new AuthUserEntity();
                    authUser.setUsername(testUsername);
                    authUser.setPassword("password");
                    authUser.setEnabled(true);
                    authUser.setAccountNonExpired(true);
                    authUser.setAccountNonLocked(true);
                    authUser.setCredentialsNonExpired(true);

                    new AuthUserDaoJdbc().create(authUser);

                    throw new RuntimeException("Simulated failure");
                })
        );
    }

    @Test
    void jdbcWithoutTransaction_MultiDb_PartialSuccess() {
        String testUsername = RandomDataUtils.randomUsername();

        AuthUserEntity authUser = new AuthUserEntity();
        authUser.setUsername(testUsername);
        authUser.setPassword("password");
        authUser.setEnabled(true);
        authUser.setAccountNonExpired(true);
        authUser.setAccountNonLocked(true);
        authUser.setCredentialsNonExpired(true);

        AuthUserEntity createdAuthUser = new AuthUserDaoJdbc().create(authUser);
        assertNotNull(createdAuthUser.getId());

        Optional<AuthUserEntity> authUserFound = new AuthUserDaoJdbc().findById(createdAuthUser.getId());
        assertTrue(authUserFound.isPresent(), "Auth user should be committed");
    }

    @Test
    void jdbcWithSeparateTransactions_MultiDb() {
        String testUsername = RandomDataUtils.randomUsername();
        JdbcTransactionTemplate authTx = new JdbcTransactionTemplate(CFG.authJdbcUrl());
        JdbcTransactionTemplate userdataTx = new JdbcTransactionTemplate(CFG.userdataJdbcUrl());

        AuthUserEntity createdAuthUser = authTx.execute(() -> {
            AuthUserEntity authUser = new AuthUserEntity();
            authUser.setUsername(testUsername);
            authUser.setPassword("password");
            authUser.setEnabled(true);
            authUser.setAccountNonExpired(true);
            authUser.setAccountNonLocked(true);
            authUser.setCredentialsNonExpired(true);

            return new AuthUserDaoJdbc().create(authUser);
        });

        assertNotNull(createdAuthUser.getId());

        UserEntity createdUdUser = userdataTx.execute(() -> {
            UserEntity udUser = new UserEntity();
            udUser.setUsername(testUsername);
            udUser.setCurrency(CurrencyValues.RUB);
            udUser.setFirstname(RandomDataUtils.randomName());
            udUser.setSurname(RandomDataUtils.randomSurname());
            udUser.setFullname(udUser.getFirstname() + " " + udUser.getSurname());

            return new UserDataUserDaoJdbc().createUser(udUser);
        });

        assertNotNull(createdUdUser.getId());

        authTx.execute(() -> {
            Optional<AuthUserEntity> foundAuth = new AuthUserDaoJdbc().findById(createdAuthUser.getId());
            assertTrue(foundAuth.isPresent());
            return null;
        });

        userdataTx.execute(() -> {
            Optional<UserEntity> foundUd = new UserDataUserDaoJdbc().findById(createdUdUser.getId());
            assertTrue(foundUd.isPresent());
            return null;
        });
    }

    @Test
    void springJdbcWithoutTransaction_MultiDb() {
        String testUsername = RandomDataUtils.randomUsername();

        AuthUserEntity authUser = new AuthUserEntity();
        authUser.setUsername(testUsername);
        authUser.setPassword("password");
        authUser.setEnabled(true);
        authUser.setAccountNonExpired(true);
        authUser.setAccountNonLocked(true);
        authUser.setCredentialsNonExpired(true);

        AuthUserEntity createdAuthUser = new AuthUserDaoSpringJdbc().create(authUser);
        
        AuthorityEntity readAuth = new AuthorityEntity();
        readAuth.setUserId(createdAuthUser.getId());
        readAuth.setAuthority(Authority.read);

        AuthorityEntity writeAuth = new AuthorityEntity();
        writeAuth.setUserId(createdAuthUser.getId());
        writeAuth.setAuthority(Authority.write);

        new AuthAuthorityDaoSpringJdbc().create(readAuth, writeAuth);

        UserEntity udUser = new UserEntity();
        udUser.setUsername(testUsername);
        udUser.setCurrency(CurrencyValues.RUB);
        udUser.setFirstname(RandomDataUtils.randomName());
        udUser.setSurname(RandomDataUtils.randomSurname());
        udUser.setFullname(udUser.getFirstname() + " " + udUser.getSurname());

        UserEntity createdUdUser = new UserDataDaoSpringJdbc().createUser(udUser);

        assertNotNull(createdAuthUser.getId());
        assertNotNull(createdUdUser.getId());

        Optional<AuthUserEntity> foundAuth = new AuthUserDaoSpringJdbc().findById(createdAuthUser.getId());
        Optional<UserEntity> foundUd = new UserDataDaoSpringJdbc().findById(createdUdUser.getId());

        assertTrue(foundAuth.isPresent());
        assertTrue(foundUd.isPresent());
    }

    @Test
    void chainedTransactionManager_MultiDb_Rollback() {
        String testUsername = RandomDataUtils.randomUsername();
        ChainedTransactionTemplate chainedTx = new ChainedTransactionTemplate(
                CFG.authJdbcUrl(),
                CFG.userdataJdbcUrl()
        );

        assertThrows(RuntimeException.class, () ->
                chainedTx.execute(
                        () -> {
                            AuthUserEntity authUser = new AuthUserEntity();
                            authUser.setUsername(testUsername);
                            authUser.setPassword("password");
                            authUser.setEnabled(true);
                            authUser.setAccountNonExpired(true);
                            authUser.setAccountNonLocked(true);
                            authUser.setCredentialsNonExpired(true);

                            AuthUserEntity created = new AuthUserDaoJdbc().create(authUser);
                            
                            AuthorityEntity readAuth = new AuthorityEntity();
                            readAuth.setUserId(created.getId());
                            readAuth.setAuthority(Authority.read);

                            AuthorityEntity writeAuth = new AuthorityEntity();
                            writeAuth.setUserId(created.getId());
                            writeAuth.setAuthority(Authority.write);

                            new AuthAuthorityDaoJdbc().create(readAuth, writeAuth);
                            
                            return created;
                        },
                        () -> {
                            UserEntity udUser = new UserEntity();
                            udUser.setUsername(testUsername);
                            udUser.setCurrency(CurrencyValues.RUB);
                            udUser.setFirstname(RandomDataUtils.randomName());
                            udUser.setSurname(RandomDataUtils.randomSurname());
                            udUser.setFullname(udUser.getFirstname() + " " + udUser.getSurname());

                            new UserDataUserDaoJdbc().createUser(udUser);

                            throw new RuntimeException("Simulated failure in userdata");
                        }
                )
        );
    }

    @Test
    void chainedTransactionManager_MultiDb_Success() {
        String testUsername = RandomDataUtils.randomUsername();
        ChainedTransactionTemplate chainedTx = new ChainedTransactionTemplate(
                CFG.authJdbcUrl(),
                CFG.userdataJdbcUrl()
        );

        UserEntity result = chainedTx.execute(
                () -> {
                    AuthUserEntity authUser = new AuthUserEntity();
                    authUser.setUsername(testUsername);
                    authUser.setPassword("password");
                    authUser.setEnabled(true);
                    authUser.setAccountNonExpired(true);
                    authUser.setAccountNonLocked(true);
                    authUser.setCredentialsNonExpired(true);

                    AuthUserEntity created = new AuthUserDaoJdbc().create(authUser);

                    AuthorityEntity readAuth = new AuthorityEntity();
                    readAuth.setUserId(created.getId());
                    readAuth.setAuthority(Authority.read);

                    AuthorityEntity writeAuth = new AuthorityEntity();
                    writeAuth.setUserId(created.getId());
                    writeAuth.setAuthority(Authority.write);

                    new AuthAuthorityDaoJdbc().create(readAuth, writeAuth);

                    return null;
                },
                () -> {
                    UserEntity udUser = new UserEntity();
                    udUser.setUsername(testUsername);
                    udUser.setCurrency(CurrencyValues.RUB);
                    udUser.setFirstname(RandomDataUtils.randomName());
                    udUser.setSurname(RandomDataUtils.randomSurname());
                    udUser.setFullname(udUser.getFirstname() + " " + udUser.getSurname());

                    return new UserDataUserDaoJdbc().createUser(udUser);
                }
        );

        assertNotNull(result);
        assertNotNull(result.getId());
        assertEquals(testUsername, result.getUsername());
    }
}

