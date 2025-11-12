package guru.qa.niffler.test.db;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.dao.FriendshipDao;
import guru.qa.niffler.data.entity.auth.AuthUserEntity;
import guru.qa.niffler.data.entity.auth.Authority;
import guru.qa.niffler.data.entity.auth.AuthorityEntity;
import guru.qa.niffler.data.entity.spend.CategoryEntity;
import guru.qa.niffler.data.entity.spend.SpendEntity;
import guru.qa.niffler.data.entity.userdata.FriendshipEntity;
import guru.qa.niffler.data.entity.userdata.FriendshipStatus;
import guru.qa.niffler.data.entity.userdata.UserEntity;
import guru.qa.niffler.data.impl.AuthAuthorityDaoJdbc;
import guru.qa.niffler.data.impl.AuthUserDaoJdbc;
import guru.qa.niffler.data.impl.CategoryDaoJdbc;
import guru.qa.niffler.data.impl.FriendshipDaoJdbc;
import guru.qa.niffler.data.impl.SpendDaoJdbc;
import guru.qa.niffler.data.impl.UserDataUserDaoJdbc;
import guru.qa.niffler.data.repository.AuthUserRepository;
import guru.qa.niffler.data.repository.UserdataUserRepository;
import guru.qa.niffler.data.repository.impl.AuthUserRepositoryJdbc;
import guru.qa.niffler.data.repository.impl.UserdataUserRepositoryJdbc;
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
        
        Optional<AuthUserEntity> foundUser = txTemplate.execute(() -> new AuthUserDaoJdbc().findById(createdUser.getId()));

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
            readAuth.setId(userId);
            readAuth.setAuthority(Authority.read);

            AuthorityEntity writeAuth = new AuthorityEntity();
            writeAuth.setId(userId);
            writeAuth.setAuthority(Authority.write);

            new AuthAuthorityDaoJdbc().create(readAuth, writeAuth);
            return null;
        });

        Optional<AuthUserEntity> userWithAuthorities = txTemplate.execute(() -> new AuthUserDaoJdbc().findById(userId));

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

    @Test
    void testFriendshipDaoCreate() {
        FriendshipDao friendshipDao = new FriendshipDaoJdbc();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.userdataJdbcUrl());

        UserEntity user1 = new UserEntity();
        user1.setId(UUID.randomUUID());
        user1.setUsername(RandomDataUtils.randomUsername());
        user1.setCurrency(CurrencyValues.RUB);

        UserEntity user2 = new UserEntity();
        user2.setId(UUID.randomUUID());
        user2.setUsername(RandomDataUtils.randomUsername());
        user2.setCurrency(CurrencyValues.RUB);

        txTemplate.execute(() -> {
            new UserDataUserDaoJdbc().createUser(user1);
            new UserDataUserDaoJdbc().createUser(user2);

            FriendshipEntity friendship = new FriendshipEntity();
            friendship.setRequester(user1);
            friendship.setAddressee(user2);
            friendship.setCreatedDate(new Date());
            friendship.setStatus(FriendshipStatus.PENDING);

            friendshipDao.create(friendship);

            Optional<FriendshipEntity> found = friendshipDao.findById(user1.getId(), user2.getId());
            assertTrue(found.isPresent());
            assertEquals(FriendshipStatus.PENDING, found.get().getStatus());

            return null;
        });
    }

    @Test
    void testCreateUserViaRepository() {
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.userdataJdbcUrl());

        UserEntity user = txTemplate.execute(() -> {
            UserEntity newUser = new UserEntity();
            newUser.setUsername(RandomDataUtils.randomUsername());
            newUser.setCurrency(CurrencyValues.RUB);
            newUser.setFirstname(RandomDataUtils.randomName());
            newUser.setSurname(RandomDataUtils.randomSurname());

            return repository.create(newUser);
        });

        assertNotNull(user);
        assertNotNull(user.getId());

        txTemplate.execute(() -> {
            FriendshipDao friendshipDao = new FriendshipDaoJdbc();
            assertEquals(0, friendshipDao.findByRequester(user.getId()).size());
            assertEquals(0, friendshipDao.findByAddressee(user.getId()).size());
            return null;
        });
    }

    @Test
    void testCreateInvitation() {
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.userdataJdbcUrl());

        txTemplate.execute(() -> {
            UserEntity userA = new UserEntity();
            userA.setUsername(RandomDataUtils.randomUsername());
            userA.setCurrency(CurrencyValues.RUB);
            userA = repository.create(userA);

            UserEntity userB = new UserEntity();
            userB.setUsername(RandomDataUtils.randomUsername());
            userB.setCurrency(CurrencyValues.EUR);
            userB = repository.create(userB);

            repository.createInvitation(userA, userB);

            FriendshipDao friendshipDao = new FriendshipDaoJdbc();
            Optional<FriendshipEntity> invitation = friendshipDao.findById(userA.getId(), userB.getId());
            assertTrue(invitation.isPresent());
            assertEquals(FriendshipStatus.PENDING, invitation.get().getStatus());

            Optional<FriendshipEntity> reverse = friendshipDao.findById(userB.getId(), userA.getId());
            assertFalse(reverse.isPresent());

            return null;
        });
    }

    @Test
    void testCreateFriendship() {
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.userdataJdbcUrl());

        txTemplate.execute(() -> {
            UserEntity userA = new UserEntity();
            userA.setUsername(RandomDataUtils.randomUsername());
            userA.setCurrency(CurrencyValues.RUB);
            userA = repository.create(userA);

            UserEntity userB = new UserEntity();
            userB.setUsername(RandomDataUtils.randomUsername());
            userB.setCurrency(CurrencyValues.EUR);
            userB = repository.create(userB);

            repository.createFriendship(userA, userB);

            FriendshipDao friendshipDao = new FriendshipDaoJdbc();

            Optional<FriendshipEntity> friendship1 = friendshipDao.findById(userA.getId(), userB.getId());
            assertTrue(friendship1.isPresent());
            assertEquals(FriendshipStatus.ACCEPTED, friendship1.get().getStatus());

            Optional<FriendshipEntity> friendship2 = friendshipDao.findById(userB.getId(), userA.getId());
            assertTrue(friendship2.isPresent());
            assertEquals(FriendshipStatus.ACCEPTED, friendship2.get().getStatus());

            return null;
        });
    }

    @Test
    void testFindAuthUserWithAuthorities() {
        AuthUserRepository repository = new AuthUserRepositoryJdbc();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.authJdbcUrl());

        UUID userId = txTemplate.execute(() -> {
            AuthUserEntity user = new AuthUserEntity();
            user.setUsername(RandomDataUtils.randomUsername());
            user.setPassword("password");
            user.setEnabled(true);
            user.setAccountNonExpired(true);
            user.setAccountNonLocked(true);
            user.setCredentialsNonExpired(true);

            AuthorityEntity authority1 = new AuthorityEntity();
            authority1.setAuthority(Authority.read);
            authority1.setUser(user);

            AuthorityEntity authority2 = new AuthorityEntity();
            authority2.setAuthority(Authority.write);
            authority2.setUser(user);

            user.setAuthorities(java.util.Arrays.asList(authority1, authority2));

            return repository.create(user).getId();
        });

        Optional<AuthUserEntity> foundUser = txTemplate.execute(() -> repository.findById(userId));

        assertTrue(foundUser.isPresent());
        assertEquals(2, foundUser.get().getAuthorities().size());
        
        foundUser.get().getAuthorities().forEach(authority -> {
            assertNotNull(authority.getUser());
            assertEquals(userId, authority.getUser().getId());
        });
    }

    @Test
    void testFindUserWithFriendships() {
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.userdataJdbcUrl());

        UUID mainUserId = txTemplate.execute(() -> {
            UserEntity mainUser = new UserEntity();
            mainUser.setUsername(RandomDataUtils.randomUsername());
            mainUser.setCurrency(CurrencyValues.RUB);
            mainUser = repository.create(mainUser);

            UserEntity friend = new UserEntity();
            friend.setUsername(RandomDataUtils.randomUsername());
            friend.setCurrency(CurrencyValues.EUR);
            friend = repository.create(friend);

            UserEntity requester = new UserEntity();
            requester.setUsername(RandomDataUtils.randomUsername());
            requester.setCurrency(CurrencyValues.USD);
            requester = repository.create(requester);

            repository.createFriendship(mainUser, friend);

            repository.createInvitation(requester, mainUser);

            return mainUser.getId();
        });

        Optional<UserEntity> foundUser = txTemplate.execute(() -> repository.findByIdWithFriendships(mainUserId));

        assertTrue(foundUser.isPresent());
        UserEntity user = foundUser.get();

        assertNotNull(user.getFriendshipRequests());
        assertTrue(user.getFriendshipRequests().size() >= 1);

        assertNotNull(user.getFriendshipAddressees());
        assertTrue(user.getFriendshipAddressees().size() >= 1);
    }

    @Test
    void testFindSpendWithCategory() {
        JdbcTransactionTemplate txTemplate = new JdbcTransactionTemplate(CFG.spendJdbcUrl());

        UUID spendId = txTemplate.execute(() -> {
            CategoryDaoJdbc categoryDao = new CategoryDaoJdbc();
            CategoryEntity category = new CategoryEntity();
            category.setName(RandomDataUtils.randomCategoryName());
            category.setUsername(RandomDataUtils.randomUsername());
            category.setArchived(false);
            category = categoryDao.create(category);

            SpendDaoJdbc spendDao = new SpendDaoJdbc();
            SpendEntity spend = new SpendEntity();
            spend.setUsername(category.getUsername());
            spend.setCurrency(CurrencyValues.RUB);
            spend.setSpendDate(new Date());
            spend.setAmount(100.0);
            spend.setDescription("Test spend");
            spend.setCategory(category);
            
            return spendDao.create(spend).getId();
        });

        Optional<SpendEntity> foundSpend = txTemplate.execute(() -> {
            SpendDaoJdbc spendDao = new SpendDaoJdbc();
            return spendDao.findByIdWithCategory(spendId);
        });

        assertTrue(foundSpend.isPresent());
        SpendEntity spend = foundSpend.get();
        
        assertNotNull(spend.getCategory());
        assertNotNull(spend.getCategory().getName());
        assertNotNull(spend.getCategory().getUsername());
        assertFalse(spend.getCategory().isArchived());
    }

    @Test
    void testFriendshipDaoValidation() {
        FriendshipDao friendshipDao = new FriendshipDaoJdbc();
        FriendshipEntity friendship = new FriendshipEntity();

        UserEntity user1 = new UserEntity();
        user1.setId(UUID.randomUUID());
        UserEntity user2 = new UserEntity();
        user2.setId(UUID.randomUUID());

        friendship.setRequester(user1);
        friendship.setAddressee(user2);
        friendship.setCreatedDate(new Date());
        friendship.setStatus(null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            friendshipDao.create(friendship);
        });
        assertEquals("Friendship status cannot be null", exception.getMessage());

        friendship.setStatus(FriendshipStatus.PENDING);
        friendship.setRequester(null);
        exception = assertThrows(IllegalArgumentException.class, () -> {
            friendshipDao.create(friendship);
        });
        assertEquals("Friendship requester cannot be null", exception.getMessage());

        friendship.setRequester(user1);
        friendship.setAddressee(null);
        exception = assertThrows(IllegalArgumentException.class, () -> {
            friendshipDao.create(friendship);
        });
        assertEquals("Friendship addressee cannot be null", exception.getMessage());

        friendship.setAddressee(user2);
        friendship.setCreatedDate(null);
        exception = assertThrows(IllegalArgumentException.class, () -> {
            friendshipDao.create(friendship);
        });
        assertEquals("Friendship created date cannot be null", exception.getMessage());
    }

}
