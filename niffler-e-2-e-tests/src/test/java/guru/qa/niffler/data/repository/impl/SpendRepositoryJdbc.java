package guru.qa.niffler.data.repository.impl;

import guru.qa.niffler.config.Config;
import guru.qa.niffler.data.entity.spend.CategoryEntity;
import guru.qa.niffler.data.entity.spend.SpendEntity;
import guru.qa.niffler.data.mapper.SpendEntityRowMapper;
import guru.qa.niffler.data.repository.SpendRepository;
import guru.qa.niffler.model.CurrencyValues;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.UUID;

import static guru.qa.niffler.data.tpl.Connections.holder;

public class SpendRepositoryJdbc implements SpendRepository {

    private static final Config CFG = Config.getInstance();
    private static final String URL = CFG.spendJdbcUrl();

    @Override
    public SpendEntity create(SpendEntity spend) {
        try (PreparedStatement ps = holder(URL).connection().prepareStatement(
                "INSERT INTO spend (username, currency, spend_date, amount, description, category_id) " +
                        "VALUES (?, ?, ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, spend.getUsername());
            ps.setString(2, spend.getCurrency().name());
            ps.setDate(3, new java.sql.Date(spend.getSpendDate().getTime()));
            ps.setDouble(4, spend.getAmount());
            ps.setString(5, spend.getDescription());
            ps.setObject(6, spend.getCategory().getId());

            ps.executeUpdate();

            final UUID generatedKey;
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    generatedKey = rs.getObject("id", UUID.class);
                } else {
                    throw new SQLException("Can't find id in ResultSet");
                }
            }
            spend.setId(generatedKey);
            return spend;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<SpendEntity> findById(UUID id) {
        try (PreparedStatement ps = holder(URL).connection().prepareStatement(
                "SELECT * FROM spend WHERE id = ?")) {
            ps.setObject(1, id);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.ofNullable(SpendEntityRowMapper.instance.mapRow(rs, 1));
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<SpendEntity> findByIdWithCategory(UUID id) {
        try (PreparedStatement ps = holder(URL).connection().prepareStatement(
                "SELECT s.id as spend_id, s.username as spend_username, s.currency as spend_currency, " +
                        "s.spend_date, s.amount, s.description, " +
                        "c.id as category_id, c.name as category_name, c.username as category_username, c.archived " +
                        "FROM spend s " +
                        "JOIN category c ON s.category_id = c.id " +
                        "WHERE s.id = ?"
        )) {
            ps.setObject(1, id);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    SpendEntity spend = new SpendEntity();
                    spend.setId(rs.getObject("spend_id", UUID.class));
                    spend.setUsername(rs.getString("spend_username"));
                    spend.setCurrency(CurrencyValues.valueOf(rs.getString("spend_currency")));
                    spend.setSpendDate(rs.getDate("spend_date"));
                    spend.setAmount(rs.getDouble("amount"));
                    spend.setDescription(rs.getString("description"));

                    CategoryEntity category = new CategoryEntity();
                    category.setId(rs.getObject("category_id", UUID.class));
                    category.setName(rs.getString("category_name"));
                    category.setUsername(rs.getString("category_username"));
                    category.setArchived(rs.getBoolean("archived"));

                    spend.setCategory(category);

                    return Optional.of(spend);
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

