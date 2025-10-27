package guru.qa.niffler.test;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.entity.auth.AuthUserEntity;
import guru.qa.niffler.data.entity.auth.Authority;
import guru.qa.niffler.data.entity.auth.AuthorityEntity;
import guru.qa.niffler.data.impl.AuthAuthorityDaoJdbc;
import guru.qa.niffler.data.impl.AuthUserDaoJdbc;
import guru.qa.niffler.data.tpl.JdbcTransactionTemplate;
import guru.qa.niffler.data.tpl.XaTransactionTemplate;
import guru.qa.niffler.model.CategoryJson;
import guru.qa.niffler.model.CurrencyValues;
import guru.qa.niffler.model.SpendJson;
import guru.qa.niffler.model.UserJson;
import guru.qa.niffler.service.SpendDbClient;
import guru.qa.niffler.service.UserDbClient;
import guru.qa.niffler.utils.RandomDataUtils;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static guru.qa.niffler.data.tpl.Connections.holder;
import static org.junit.jupiter.api.Assertions.*;

public class JdbcTest {

    private static final Config CFG = Config.getInstance();

    @Test
    void createSpend() {
        SpendDbClient spendDbClient = new SpendDbClient();
        SpendJson spend = spendDbClient.createSpend(
                new SpendJson(
                        null,
                        new Date(),
                        new CategoryJson(
                                null,
                                RandomDataUtils.randomCategoryName(),
                                RandomDataUtils.randomUsername(),
                                false
                        ),
                        CurrencyValues.EUR,
                        100.00,
                        RandomDataUtils.randomSentence(3),
                        RandomDataUtils.randomUsername()
                )
        );
        assertNotNull(spend);
        assertNotNull(spend.id());
    }

    @Test
    void createAuthUser() {
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.authJdbcUrl());
        
        AuthUserEntity createdUser = txTemplate.execute(() -> {
            AuthUserEntity user = new AuthUserEntity();
            user.setUsername(RandomDataUtils.randomUsername());
            user.setPassword(RandomDataUtils.randomSentence(3));
            user.setEnabled(true);
            user.setAccountNonExpired(true);
            user.setAccountNonLocked(true);
            user.setCredentialsNonExpired(true);
            return new AuthUserDaoJdbc().create(user);
        });

        assertNotNull(createdUser.getId());
        
        Optional<AuthUserEntity> foundUser = txTemplate.execute(() -> {
            return new AuthUserDaoJdbc().findById(createdUser.getId());
        });

        assertTrue(foundUser.isPresent());
    }

    @Test
    void createAuthorities() {
        String testUsername = RandomDataUtils.randomUsername();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.authJdbcUrl());
        
        UUID userId = txTemplate.execute(() -> {
            AuthUserEntity user = new AuthUserEntity();
            user.setUsername(testUsername);
            user.setPassword(RandomDataUtils.randomSentence(3));
            user.setEnabled(true);
            user.setAccountNonExpired(true);
            user.setAccountNonLocked(true);
            user.setCredentialsNonExpired(true);
            return new AuthUserDaoJdbc().create(user).getId();
        });

        txTemplate.execute(() -> {
            AuthorityEntity readAuth = new AuthorityEntity();
            readAuth.setUserId(userId);
            readAuth.setAuthority(Authority.read);

            AuthorityEntity writeAuth = new AuthorityEntity();
            writeAuth.setUserId(userId);
            writeAuth.setAuthority(Authority.write);

            new AuthAuthorityDaoJdbc().create(readAuth, writeAuth);
            return null;
        });

        Optional<AuthUserEntity> userWithAuthorities = txTemplate.execute(() -> {
            return new AuthUserDaoJdbc().findById(userId);
        });

        assertTrue(userWithAuthorities.isPresent());
        assertEquals(testUsername, userWithAuthorities.get().getUsername());
    }

    @Test
    void xaTransactionRollback() {
        String testUsername = RandomDataUtils.randomUsername();
        XaTransactionTemplate xaTxTemplate = new XaTransactionTemplate(
            CFG.authJdbcUrl(), 
            CFG.userdataJdbcUrl()
        );
        
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
            xaTxTemplate.execute(
                () -> {
                    AuthUserEntity authUser = new AuthUserEntity();
                    authUser.setUsername(testUsername);
                    authUser.setPassword(RandomDataUtils.randomSentence(3));
                    authUser.setEnabled(true);
                    authUser.setAccountNonExpired(true);
                    authUser.setAccountNonLocked(true);
                    authUser.setCredentialsNonExpired(true);
                    return new AuthUserDaoJdbc().create(authUser);
                },
                () -> {
                    throw new RuntimeException();
                }
            )
        );
        
        assertNotNull(exception);
    }

    @Test
    void transactionIsolationLevel() {
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.authJdbcUrl());
        
        txTemplate.execute(() -> {
            try {
                Connection con = holder(CFG.authJdbcUrl()).connection();
                assertEquals(Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation());
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, Connection.TRANSACTION_REPEATABLE_READ);
    }

    @Test
    void xaTransactionIsolationLevel() {
        XaTransactionTemplate xaTxTemplate = new XaTransactionTemplate(
            CFG.authJdbcUrl(), 
            CFG.userdataJdbcUrl()
        );
        
        xaTxTemplate.execute(
            () -> {
                try {
                    Connection con = holder(CFG.authJdbcUrl()).connection();
                    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                    assertEquals(Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            () -> {
                try {
                    Connection con = holder(CFG.userdataJdbcUrl()).connection();
                    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                    assertEquals(Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    @Test
    void createUserInBothDatabases() {
        String username = RandomDataUtils.randomUsername();
        
        UserJson user = new UserDbClient().createUser(
            new UserJson(
                null,
                username,
                RandomDataUtils.randomName(),
                RandomDataUtils.randomSurname(),
                RandomDataUtils.randomName() + " " + RandomDataUtils.randomSurname(),
                CurrencyValues.RUB,
                null,
                null,
                null
            )
        );

        assertNotNull(user);
        assertNotNull(user.id());
        assertEquals(username, user.username());
    }

}
