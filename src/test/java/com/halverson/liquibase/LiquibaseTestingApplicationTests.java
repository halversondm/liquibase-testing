package com.halverson.liquibase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@ActiveProfiles("test")
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ImportTestcontainers
class LiquibaseTestingApplicationTests {

    @Autowired
    DataSource dataSource;

    @Test
    @Order(1)
    void runLiquibase() throws Exception {
        log.info("Running Liquibase...");
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

        try (Statement statement = dataSource.getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT id, data FROM table_with_clob");

            while (resultSet.next()) {
                Integer id = resultSet.getInt("id");
                String data = resultSet.getString("data");
                JsonNode jsonNode = objectMapper.readTree(data.getBytes());
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