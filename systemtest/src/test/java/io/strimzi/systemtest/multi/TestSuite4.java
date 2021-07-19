package io.strimzi.systemtest.multi;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.test.annotations.IsolatedSuite;

@IsolatedSuite
public class TestSuite4 extends AbstractST {

    @ParallelTest
    void test1() throws InterruptedException {
        Thread.sleep(2000);
    }

    @ParallelTest
    void test2() throws InterruptedException {
        Thread.sleep(2000);
    }
}
