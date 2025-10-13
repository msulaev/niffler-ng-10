package guru.qa.niffler.service;

import guru.qa.niffler.data.dao.CategoryDao;
import guru.qa.niffler.data.dao.SpendDao;
import guru.qa.niffler.data.dao.impl.CategoryDaoJdbc;
import guru.qa.niffler.data.dao.impl.SpendDaoJdbc;
import guru.qa.niffler.data.entity.spend.CategoryEntity;
import guru.qa.niffler.data.entity.spend.SpendEntity;
import guru.qa.niffler.model.CategoryJson;
import guru.qa.niffler.model.SpendJson;

import java.util.Optional;

public class SpendDbClient implements SpendClient {

    private final SpendDao spendDao = new SpendDaoJdbc();
    private final CategoryDao categoryDao = new CategoryDaoJdbc();

    @Override
    public SpendJson createSpend(SpendJson spend) {
        SpendEntity spendEntity = SpendEntity.fromJson(spend);
        if (spendEntity.getCategory().getId() == null) {
            CategoryEntity categoryEntity = categoryDao
                    .findCategoryByUsernameAndCategoryName(
                            spend.username(),
                            spend.category().name()
                    )
                    .orElseGet(() -> categoryDao.create(spendEntity.getCategory()));
            spendEntity.setCategory(categoryEntity);
        }
        return SpendJson.fromEntity(
                spendDao.create(spendEntity)
        );
    }

    @Override
    public CategoryJson createCategory(CategoryJson category) {
        return CategoryJson.fromEntity(
                categoryDao.create(
                        CategoryEntity.fromJson(category)
                )
        );
    }

    @Override
    public Optional<CategoryJson> findCategoryByNameAndUsername(String categoryName, String username) {
        return categoryDao.findCategoryByUsernameAndCategoryName(username, categoryName)
                .map(CategoryJson::fromEntity);
    }

    public CategoryJson updateCategory(CategoryJson category) {
        return CategoryJson.fromEntity(
                categoryDao.update(
                        CategoryEntity.fromJson(category)
                )
        );
    }

    public void deleteCategory(CategoryJson category) {
        categoryDao.deleteCategory(
                CategoryEntity.fromJson(category)
        );
    }

    public void deleteSpend(SpendJson spend) {
        spendDao.deleteSpend(
                SpendEntity.fromJson(spend)
        );
    }
}
