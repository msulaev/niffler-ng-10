package guru.qa.niffler.data.extractor;

import guru.qa.niffler.data.entity.spend.CategoryEntity;
import guru.qa.niffler.data.entity.spend.SpendEntity;
import guru.qa.niffler.model.CurrencyValues;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

public class SpendWithCategoryExtractor implements ResultSetExtractor<Optional<SpendEntity>> {

    public static final SpendWithCategoryExtractor instance = new SpendWithCategoryExtractor();

    private SpendWithCategoryExtractor() {
    }

    @Override
    public Optional<SpendEntity> extractData(ResultSet rs) throws SQLException, DataAccessException {
        if (rs.next()) {
            SpendEntity spend = new SpendEntity();
            spend.setId(rs.getObject("id", UUID.class));
            spend.setUsername(rs.getString("username"));
            spend.setSpendDate(rs.getDate("spend_date"));
            spend.setCurrency(CurrencyValues.valueOf(rs.getString("currency")));
            spend.setAmount(rs.getDouble("amount"));
            spend.setDescription(rs.getString("description"));
            
            CategoryEntity category = new CategoryEntity();
            category.setId(rs.getObject("cat_id", UUID.class));
            category.setName(rs.getString("cat_name"));
            category.setUsername(rs.getString("cat_username"));
            category.setArchived(rs.getBoolean("archived"));
            
            spend.setCategory(category);
            
            return Optional.of(spend);
        }
        return Optional.empty();
    }
}

