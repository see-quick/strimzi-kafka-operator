package io.strimzi.systemtest.smokeparalell.classconcurrent_methodconcurrent;

import io.strimzi.systemtest.annotations.ParallelTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class Example {

    private static final Logger LOGGER = LogManager.getLogger(Example.class);

    @Test
    void test1() throws InterruptedException {
        Thread.sleep(5000);
        LOGGER.info("Hello 1");
    }

    @Test
    void test2() throws InterruptedException {
        Thread.sleep(5000);
        LOGGER.info("Hello 2");
    }

    @Test
    void test3() throws InterruptedException {
        Thread.sleep(5000);
        LOGGER.info("Hello 3");
    }

    @BeforeAll
    static void setUp() {
        LOGGER.info("Preparing paralell run...");
    }
}
