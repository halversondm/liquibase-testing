package com.halverson.liquibase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@ActiveProfiles("postgres")
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ImportTestcontainers
class LiquibaseWithPostgresTests {

    @Autowired
    DataSource dataSource;

    @Test
    @Order(1)
    void runLiquibase() throws Exception {
        log.info("Running Liquibase...");
    }

    @Test
    @Order(2)
    void testDataViaSchema() throws Exception {
        CommonValidationAgainstDatabase.validateAgainstSchema(dataSource);
    }

}