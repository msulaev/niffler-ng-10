package guru.qa.niffler.test.db;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.entity.auth.AuthUserEntity;
import guru.qa.niffler.data.entity.auth.Authority;
import guru.qa.niffler.data.entity.auth.AuthorityEntity;
import guru.qa.niffler.data.entity.userdata.UserEntity;
import guru.qa.niffler.data.impl.AuthAuthorityDaoJdbc;
import guru.qa.niffler.data.impl.AuthUserDaoJdbc;
import guru.qa.niffler.data.impl.UserDataUserDaoJdbc;
import guru.qa.niffler.data.tpl.ChainedTransactionTemplate;
import guru.qa.niffler.model.CurrencyValues;
import guru.qa.niffler.utils.RandomDataUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ChainedTransactionTest {

    private static final Config CFG = Config.getInstance();

    @Test
    void chainedTransactionSuccess() {
        String testUsername = RandomDataUtils.randomUsername();
        ChainedTransactionTemplate chainedTx = new ChainedTransactionTemplate(
                CFG.authJdbcUrl(),
                CFG.userdataJdbcUrl()
        );

        UserEntity createdUser = chainedTx.execute(
                () -> {
                    AuthUserEntity authUser = new AuthUserEntity();
                    authUser.setUsername(testUsername);
                    authUser.setPassword("password");
                    authUser.setEnabled(true);
                    authUser.setAccountNonExpired(true);
                    authUser.setAccountNonLocked(true);
                    authUser.setCredentialsNonExpired(true);

                    AuthUserEntity createdAuthUser = new AuthUserDaoJdbc().create(authUser);

                    AuthorityEntity readAuth = new AuthorityEntity();
                    readAuth.setUserId(createdAuthUser.getId());
                    readAuth.setAuthority(Authority.read);

                    AuthorityEntity writeAuth = new AuthorityEntity();
                    writeAuth.setUserId(createdAuthUser.getId());
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

        assertNotNull(createdUser);
        assertNotNull(createdUser.getId());
        assertEquals(testUsername, createdUser.getUsername());
    }

}

