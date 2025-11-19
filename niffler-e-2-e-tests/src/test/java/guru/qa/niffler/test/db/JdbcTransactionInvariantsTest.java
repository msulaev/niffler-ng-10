package guru.qa.niffler.test.db;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.entity.auth.AuthUserEntity;
import guru.qa.niffler.data.entity.auth.Authority;
import guru.qa.niffler.data.entity.auth.AuthorityEntity;
import guru.qa.niffler.data.entity.spend.CategoryEntity;
import guru.qa.niffler.data.entity.spend.SpendEntity;
import guru.qa.niffler.data.entity.userdata.FriendshipStatus;
import guru.qa.niffler.data.entity.userdata.UserEntity;
import guru.qa.niffler.data.impl.AuthAuthorityDaoJdbc;
import guru.qa.niffler.data.impl.AuthAuthorityDaoSpringJdbc;
import guru.qa.niffler.data.impl.AuthUserDaoJdbc;
import guru.qa.niffler.data.impl.AuthUserDaoSpringJdbc;
import guru.qa.niffler.data.impl.UserDataUserDaoJdbc;
import guru.qa.niffler.data.impl.UserDataDaoSpringJdbc;
import guru.qa.niffler.data.repository.AuthUserRepository;
import guru.qa.niffler.data.repository.SpendRepository;
import guru.qa.niffler.data.repository.UserdataUserRepository;
import guru.qa.niffler.data.repository.impl.AuthUserRepositoryJdbc;
import guru.qa.niffler.data.repository.impl.SpendRepositoryJdbc;
import guru.qa.niffler.data.repository.impl.UserdataUserRepositoryJdbc;
import guru.qa.niffler.data.tpl.ChainedTransactionTemplate;
import guru.qa.niffler.data.tpl.Connections;
import guru.qa.niffler.data.tpl.JdbcTransactionTemplate;
import guru.qa.niffler.model.CurrencyValues;
import guru.qa.niffler.utils.RandomDataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static guru.qa.niffler.data.tpl.Connections.holder;
import static org.junit.jupiter.api.Assertions.*;

public class JdbcTransactionInvariantsTest {

    private static final Config CFG = Config.getInstance();

    @AfterEach
    void cleanup() {
        Connections.closeAllConnections();
    }

    @Test
    void jdbcWithoutTransactionSuccess() {
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
    void jdbcWithTransactionRollback() {
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
    void jdbcWithTransactionSuccess() {
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
    void springJdbcWithoutTransactionNoRollback() {
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
        assertTrue(foundUser.isPresent());
    }

    @Test
    void springJdbcWithTransactionRollback() {
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
    void jdbcWithoutTransactionMultiDbPartialSuccess() {
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
        assertTrue(authUserFound.isPresent());
    }

    @Test
    void jdbcWithSeparateTransactionsMultiDb() {
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
    void springJdbcWithoutTransactionMultiDb() {
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
        readAuth.setId(createdAuthUser.getId());
        readAuth.setAuthority(Authority.read);

        AuthorityEntity writeAuth = new AuthorityEntity();
        writeAuth.setId(createdAuthUser.getId());
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
    void chainedTransactionManagerMultiDbRollback() {
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
                            readAuth.setId(created.getId());
                            readAuth.setAuthority(Authority.read);

                            AuthorityEntity writeAuth = new AuthorityEntity();
                            writeAuth.setId(created.getId());
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
    void chainedTransactionManagerMultiDbSuccess() {
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
                    readAuth.setId(created.getId());
                    readAuth.setAuthority(Authority.read);

                    AuthorityEntity writeAuth = new AuthorityEntity();
                    writeAuth.setId(created.getId());
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

    @Test
    void repositoryAuthUserCreateAndFindById() {
        String testUsername = RandomDataUtils.randomUsername();
        AuthUserRepository repository = new AuthUserRepositoryJdbc();

        AuthUserEntity authUser = new AuthUserEntity();
        authUser.setUsername(testUsername);
        authUser.setPassword("password");
        authUser.setEnabled(true);
        authUser.setAccountNonExpired(true);
        authUser.setAccountNonLocked(true);
        authUser.setCredentialsNonExpired(true);

        AuthorityEntity readAuth = new AuthorityEntity();
        readAuth.setAuthority(Authority.read);

        AuthorityEntity writeAuth = new AuthorityEntity();
        writeAuth.setAuthority(Authority.write);

        List<AuthorityEntity> authorities = new ArrayList<>();
        authorities.add(readAuth);
        authorities.add(writeAuth);
        authUser.setAuthorities(authorities);

        AuthUserEntity created = repository.create(authUser);
        assertNotNull(created.getId());
        assertEquals(testUsername, created.getUsername());

        Optional<AuthUserEntity> found = repository.findById(created.getId());
        assertTrue(found.isPresent());
        assertEquals(testUsername, found.get().getUsername());
        assertEquals(2, found.get().getAuthorities().size());
        assertTrue(found.get().getAuthorities().stream()
                .anyMatch(a -> a.getAuthority() == Authority.read));
        assertTrue(found.get().getAuthorities().stream()
                .anyMatch(a -> a.getAuthority() == Authority.write));
    }

    @Test
    void repositoryAuthUserFindByUsername() {
        String testUsername = RandomDataUtils.randomUsername();
        AuthUserRepository repository = new AuthUserRepositoryJdbc();

        AuthUserEntity authUser = new AuthUserEntity();
        authUser.setUsername(testUsername);
        authUser.setPassword("password");
        authUser.setEnabled(true);
        authUser.setAccountNonExpired(true);
        authUser.setAccountNonLocked(true);
        authUser.setCredentialsNonExpired(true);

        AuthorityEntity readAuth = new AuthorityEntity();
        readAuth.setAuthority(Authority.read);

        List<AuthorityEntity> authorities = new ArrayList<>();
        authorities.add(readAuth);
        authUser.setAuthorities(authorities);

        repository.create(authUser);

        Optional<AuthUserEntity> found = repository.findByUsername(testUsername);
        assertTrue(found.isPresent());
        assertEquals(testUsername, found.get().getUsername());
        assertEquals(1, found.get().getAuthorities().size());
        assertEquals(Authority.read, found.get().getAuthorities().get(0).getAuthority());
    }

    @Test
    void repositoryUserdataUserCreateAndFindById() {
        String testUsername = RandomDataUtils.randomUsername();
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();

        UserEntity user = new UserEntity();
        user.setUsername(testUsername);
        user.setCurrency(CurrencyValues.RUB);
        user.setFirstname(RandomDataUtils.randomName());
        user.setSurname(RandomDataUtils.randomSurname());
        user.setFullname(user.getFirstname() + " " + user.getSurname());

        UserEntity created = repository.create(user);
        assertNotNull(created.getId());
        assertEquals(testUsername, created.getUsername());

        Optional<UserEntity> found = repository.findById(created.getId());
        assertTrue(found.isPresent());
        assertEquals(testUsername, found.get().getUsername());
    }

    @Test
    void repositoryUserdataUserAddInvitation() {
        String username1 = RandomDataUtils.randomUsername();
        String username2 = RandomDataUtils.randomUsername();
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();

        UserEntity requester = new UserEntity();
        requester.setUsername(username1);
        requester.setCurrency(CurrencyValues.USD);
        requester.setFirstname(RandomDataUtils.randomName());
        requester.setSurname(RandomDataUtils.randomSurname());
        requester.setFullname(requester.getFirstname() + " " + requester.getSurname());

        UserEntity addressee = new UserEntity();
        addressee.setUsername(username2);
        addressee.setCurrency(CurrencyValues.EUR);
        addressee.setFirstname(RandomDataUtils.randomName());
        addressee.setSurname(RandomDataUtils.randomSurname());
        addressee.setFullname(addressee.getFirstname() + " " + addressee.getSurname());

        requester = repository.create(requester);
        addressee = repository.create(addressee);

        repository.createInvitation(requester, addressee);

        Optional<UserEntity> foundRequester = repository.findByIdWithFriendships(requester.getId());
        assertTrue(foundRequester.isPresent());
        assertEquals(1, foundRequester.get().getFriendshipRequests().size());
        assertEquals(FriendshipStatus.PENDING, foundRequester.get().getFriendshipRequests().get(0).getStatus());
        assertEquals(addressee.getId(), foundRequester.get().getFriendshipRequests().get(0).getAddressee().getId());

        Optional<UserEntity> foundAddressee = repository.findByIdWithFriendships(addressee.getId());
        assertTrue(foundAddressee.isPresent());
        assertEquals(1, foundAddressee.get().getFriendshipAddressees().size());
        assertEquals(FriendshipStatus.PENDING, foundAddressee.get().getFriendshipAddressees().get(0).getStatus());
        assertEquals(requester.getId(), foundAddressee.get().getFriendshipAddressees().get(0).getRequester().getId());
    }

    @Test
    void repositoryUserdataUserAddFriend() {
        String username1 = RandomDataUtils.randomUsername();
        String username2 = RandomDataUtils.randomUsername();
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();

        UserEntity user1 = new UserEntity();
        user1.setUsername(username1);
        user1.setCurrency(CurrencyValues.USD);
        user1.setFirstname(RandomDataUtils.randomName());
        user1.setSurname(RandomDataUtils.randomSurname());
        user1.setFullname(user1.getFirstname() + " " + user1.getSurname());

        UserEntity user2 = new UserEntity();
        user2.setUsername(username2);
        user2.setCurrency(CurrencyValues.EUR);
        user2.setFirstname(RandomDataUtils.randomName());
        user2.setSurname(RandomDataUtils.randomSurname());
        user2.setFullname(user2.getFirstname() + " " + user2.getSurname());

        user1 = repository.create(user1);
        user2 = repository.create(user2);

        repository.createFriendship(user1, user2);

        Optional<UserEntity> foundUser1 = repository.findByIdWithFriendships(user1.getId());
        Optional<UserEntity> foundUser2 = repository.findByIdWithFriendships(user2.getId());

        assertTrue(foundUser1.isPresent());
        assertTrue(foundUser2.isPresent());

        assertEquals(1, foundUser1.get().getFriendshipRequests().size());
        assertEquals(FriendshipStatus.ACCEPTED, foundUser1.get().getFriendshipRequests().get(0).getStatus());
        assertEquals(user2.getId(), foundUser1.get().getFriendshipRequests().get(0).getAddressee().getId());

        assertEquals(1, foundUser2.get().getFriendshipAddressees().size());
        assertEquals(FriendshipStatus.ACCEPTED, foundUser2.get().getFriendshipAddressees().get(0).getStatus());
        assertEquals(user1.getId(), foundUser2.get().getFriendshipAddressees().get(0).getRequester().getId());
    }

    @Test
    void repositoryUserdataUserFindByIdWithFriendsMultipleFriendships() {
        String username1 = RandomDataUtils.randomUsername();
        String username2 = RandomDataUtils.randomUsername();
        String username3 = RandomDataUtils.randomUsername();
        UserdataUserRepository repository = new UserdataUserRepositoryJdbc();

        UserEntity user1 = new UserEntity();
        user1.setUsername(username1);
        user1.setCurrency(CurrencyValues.USD);
        user1.setFirstname(RandomDataUtils.randomName());
        user1.setSurname(RandomDataUtils.randomSurname());
        user1.setFullname(user1.getFirstname() + " " + user1.getSurname());

        UserEntity user2 = new UserEntity();
        user2.setUsername(username2);
        user2.setCurrency(CurrencyValues.EUR);
        user2.setFirstname(RandomDataUtils.randomName());
        user2.setSurname(RandomDataUtils.randomSurname());
        user2.setFullname(user2.getFirstname() + " " + user2.getSurname());

        UserEntity user3 = new UserEntity();
        user3.setUsername(username3);
        user3.setCurrency(CurrencyValues.RUB);
        user3.setFirstname(RandomDataUtils.randomName());
        user3.setSurname(RandomDataUtils.randomSurname());
        user3.setFullname(user3.getFirstname() + " " + user3.getSurname());

        user1 = repository.create(user1);
        user2 = repository.create(user2);
        user3 = repository.create(user3);

        repository.createInvitation(user1, user2);
        repository.createFriendship(user1, user3);

        Optional<UserEntity> foundUser1 = repository.findByIdWithFriendships(user1.getId());
        assertTrue(foundUser1.isPresent());

        assertEquals(2, foundUser1.get().getFriendshipRequests().size());

        long pendingCount = foundUser1.get().getFriendshipRequests().stream()
                .filter(f -> f.getStatus() == FriendshipStatus.PENDING)
                .count();
        long acceptedCount = foundUser1.get().getFriendshipRequests().stream()
                .filter(f -> f.getStatus() == FriendshipStatus.ACCEPTED)
                .count();

        assertEquals(1, pendingCount);
        assertEquals(1, acceptedCount);
    }

    @Test
    void repositorySpendCreateAndFindByIdWithCategory() {
        String testUsername = RandomDataUtils.randomUsername();
        SpendRepository spendRepository = new SpendRepositoryJdbc();

        CategoryEntity category = new CategoryEntity();
        category.setName("Test Category");
        category.setUsername(testUsername);
        category.setArchived(false);

        try (PreparedStatement ps = holder(CFG.spendJdbcUrl()).connection().prepareStatement(
                "INSERT INTO category (name, username, archived) VALUES (?, ?, ?)",
                PreparedStatement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, category.getName());
            ps.setString(2, category.getUsername());
            ps.setBoolean(3, category.isArchived());
            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    category.setId(rs.getObject("id", java.util.UUID.class));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        SpendEntity spend = new SpendEntity();
        spend.setUsername(testUsername);
        spend.setCurrency(CurrencyValues.USD);
        spend.setSpendDate(new Date());
        spend.setAmount(100.50);
        spend.setDescription("Test spend");
        spend.setCategory(category);

        SpendEntity created = spendRepository.create(spend);
        assertNotNull(created.getId());

        Optional<SpendEntity> foundSimple = spendRepository.findById(created.getId());
        assertTrue(foundSimple.isPresent());
        assertEquals(testUsername, foundSimple.get().getUsername());
        Optional<SpendEntity> foundWithCategory = spendRepository.findByIdWithCategory(created.getId());
        assertTrue(foundWithCategory.isPresent());
        assertEquals(testUsername, foundWithCategory.get().getUsername());
        assertNotNull(foundWithCategory.get().getCategory());
        assertEquals(category.getId(), foundWithCategory.get().getCategory().getId());
        assertEquals("Test Category", foundWithCategory.get().getCategory().getName());
        assertEquals(testUsername, foundWithCategory.get().getCategory().getUsername());
        assertFalse(foundWithCategory.get().getCategory().isArchived());
    }
}

