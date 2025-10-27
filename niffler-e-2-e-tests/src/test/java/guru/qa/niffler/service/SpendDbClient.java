package guru.qa.niffler.service;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.dao.CategoryDao;
import guru.qa.niffler.data.dao.SpendDao;
import guru.qa.niffler.data.impl.CategoryDaoJdbc;
import guru.qa.niffler.data.impl.SpendDaoJdbc;
import guru.qa.niffler.data.entity.spend.CategoryEntity;
import guru.qa.niffler.data.entity.spend.SpendEntity;
import guru.qa.niffler.data.tpl.JdbcTransactionTemplate;
import guru.qa.niffler.model.CategoryJson;
import guru.qa.niffler.model.SpendJson;

import java.util.Optional;

public class SpendDbClient implements SpendClient {

    private static final Config CFG = Config.getInstance();

    private final CategoryDao categoryDao = new CategoryDaoJdbc();
    private final SpendDao spendDao = new SpendDaoJdbc();

    private final JdbcTransactionTemplate jdbcTxTemplate = new JdbcTransactionTemplate(
            CFG.spendJdbcUrl()
    );

    @Override
    public SpendJson createSpend(SpendJson spend) {
        return jdbcTxTemplate.execute(() -> {
                    SpendEntity spendEntity = SpendEntity.fromJson(spend);
                    if (spendEntity.getCategory().getId() == null) {
                        CategoryEntity categoryEntity = categoryDao.create(spendEntity.getCategory());
                        spendEntity.setCategory(categoryEntity);
                    }
                    return SpendJson.fromEntity(
                            spendDao.create(spendEntity)
                    );
                }
        );
    }

    @Override
    public CategoryJson createCategory(CategoryJson category) {
        return jdbcTxTemplate.execute(() -> {
                    return CategoryJson.fromEntity(
                            categoryDao.create(
                                    CategoryEntity.fromJson(category)
                            )
                    );
                }
        );
    }

    @Override
    public Optional<CategoryJson> findCategoryByNameAndUsername(String categoryName, String username) {
        return jdbcTxTemplate.execute(() -> {
                    return categoryDao
                            .findCategoryByUsernameAndCategoryName(username, categoryName)
                            .map(CategoryJson::fromEntity);
                }
        );
    }

    public CategoryJson updateCategory(CategoryJson category) {
        return jdbcTxTemplate.execute(() -> {
                    return CategoryJson.fromEntity(
                            categoryDao.update(
                                    CategoryEntity.fromJson(category)
                            )
                    );
                }
        );
    }

    public void deleteCategory(CategoryJson category) {
        jdbcTxTemplate.execute(() -> {
                    categoryDao.deleteCategory(
                            CategoryEntity.fromJson(category)
                    );
                    return null;
                }
        );
    }

    public void deleteSpend(SpendJson spend) {
        jdbcTxTemplate.execute(() -> {
                    spendDao.deleteSpend(
                            SpendEntity.fromJson(spend)
                    );
                    return null;
                }
        );
    }
}
