package org.flywaydb.core.schema;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Execute the following statements to prepare the local PostgreSQL database
 * for the tests:
 *
 *      CREATE DATABASE flyway;
 *      CREATE USER flyway WITH ENCRYPTED PASSWORD 'flyway';
 *      GRANT ALL PRIVILEGES ON DATABASE flyway TO flyway;
 *      CREATE SCHEMA flyway_schema;
 *      GRANT ALL ON SCHEMA flyway_schema TO flyway;
 *
 * Execute the following statements to clean the database to rerun the tests:
 *
 *      DROP TABLE flyway_schema.schema_names;
 *      DROP TABLE flyway_schema.flyway_schema_history;
 *      DROP SCHEMA flyway_schema;
 *      CREATE SCHEMA flyway_schema;
 *      GRANT ALL ON SCHEMA flyway_schema TO flyway;
 *
 */
public final class CurrentSchemaTest {

    private static final String SCHEMA_NAME = "flyway_schema";

    @Test
    public void migrateFailsWithNPE() {
        DriverDataSource dataSource = createDataSourceWithCurrentSchemaSet();
        FluentConfiguration configuration = createDefaultConfiguration(dataSource);
        Flyway flyway = new Flyway(configuration);
        flyway.migrate();
    }

    @Test
    public void migrateSucceedsWhenSchemaIsSetInFlyway() {
        DriverDataSource dataSource = createDataSourceWithCurrentSchemaSet();
        FluentConfiguration configuration = createDefaultConfiguration(dataSource);
        configuration.schemas(SCHEMA_NAME);
        Flyway flyway = new Flyway(configuration);
        flyway.migrate();

        String currentSchemaName = new JdbcTemplate(dataSource).queryForObject("SELECT name FROM schema_names", String.class).toString();
        assert SCHEMA_NAME.equals(currentSchemaName);
    }

    private FluentConfiguration createDefaultConfiguration(DriverDataSource dataSource) {
        return new FluentConfiguration()
            .dataSource(dataSource)
            .locations("org.flywaydb.core.schema");
    }

    private DriverDataSource createDataSourceWithCurrentSchemaSet() {
        return new DriverDataSource(
            Thread.currentThread().getContextClassLoader(),
            "org.postgresql.Driver",
            "jdbc:postgresql://localhost:5432/flyway?currentSchema=" + SCHEMA_NAME,
            "flyway",
            "flyway"
        );
    }

}
