package com.halverson.liquibase;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;

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
    void runLiquibase() {
        log.info("Running Liquibase...");
    }

    @Test
    @Order(2)
    void testDataViaSchema() throws Exception {
        CommonValidationAgainstDatabase.validateAgainstSchema(dataSource);
    }

}