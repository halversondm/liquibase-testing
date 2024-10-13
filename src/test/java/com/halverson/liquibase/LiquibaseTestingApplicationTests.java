package com.halverson.liquibase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LiquibaseTestingApplicationTests {

    private static Connection connection;

    @BeforeAll
    static void beforeAll() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "");
    }

    @AfterAll
    static void afterAll() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @Order(1)
    void runLiquibase() throws Exception {
        log.info("Running Liquibase...");
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
        Scope.child(Scope.Attr.resourceAccessor, new ClassLoaderResourceAccessor(), () -> {
            CommandScope update = new CommandScope("update");
            update.addArgumentValue("changelogFile", "db/changelog/root.xml");
            update.addArgumentValue("database", database);
            update.execute();
        });

        log.info("Running Liquibase...DONE");
    }

    @Test
    @Order(2)
    void testDataViaSchema() throws IOException, ProcessingException, SQLException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonSchemaNode = objectMapper.readTree(new ClassPathResource("data_schema.json").getContentAsByteArray());
        // see if this can be tuned to not ignore unknown keys
        JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
        JsonSchema jsonSchema = factory.getJsonSchema(jsonSchemaNode);

        boolean needToFail = false;

        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT ID, DATA FROM TABLE_WITH_CLOB");

            while (resultSet.next()) {
                Integer id = resultSet.getInt("ID");
                Clob clob = resultSet.getClob("DATA");
                JsonNode jsonNode = objectMapper.readTree(clob.getAsciiStream());
                ProcessingReport report = jsonSchema.validate(jsonNode);
                if (report.isSuccess()) {
                    log.info("ID # {} JSON is valid according to the schema.", id);
                } else {
                    log.info("ID # {} JSON is not valid according to the schema: {}", id, report);
                    needToFail = true;
                }
            }
        }

        if (needToFail) {
            fail("Test Data via Schema has failed.  See logs for specific error.");
        }

    }

}