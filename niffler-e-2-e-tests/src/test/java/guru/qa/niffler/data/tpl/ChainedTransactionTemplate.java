package guru.qa.niffler.data.tpl;

import org.springframework.data.transaction.ChainedTransactionManager;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class ChainedTransactionTemplate {

    private final JdbcConnectionHolders holders;
    private final ChainedTransactionManager chainedTransactionManager;
    private final AtomicBoolean closeAfterAction = new AtomicBoolean(true);

    public ChainedTransactionTemplate(String... jdbcUrls) {
        this.holders = Connections.holders(jdbcUrls);

        PlatformTransactionManager[] txManagers = new PlatformTransactionManager[jdbcUrls.length];
        for (int i = 0; i < jdbcUrls.length; i++) {
            txManagers[i] = new JdbcTransactionManager(DataSources.dataSource(jdbcUrls[i]));
        }

        this.chainedTransactionManager = new ChainedTransactionManager(txManagers);
    }

    public ChainedTransactionTemplate holdConnectionAfterAction() {
        this.closeAfterAction.set(false);
        return this;
    }

    public <T> T execute(Supplier<T>... actions) {
        TransactionStatus status = chainedTransactionManager.getTransaction(new DefaultTransactionDefinition());
        try {
            T result = null;
            for (Supplier<T> action : actions) {
                result = action.get();
            }
            chainedTransactionManager.commit(status);
            return result;
        } catch (Exception e) {
            chainedTransactionManager.rollback(status);
            throw new RuntimeException(e);
        } finally {
            if (closeAfterAction.get()) {
                holders.close();
            }
        }
    }
}

