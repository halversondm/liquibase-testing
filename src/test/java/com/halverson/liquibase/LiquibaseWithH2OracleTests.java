package com.halverson.liquibase;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;

@SpringBootTest
@ActiveProfiles("h2-oracle")
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LiquibaseWithH2OracleTests {

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
