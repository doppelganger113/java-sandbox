package org.example;

import org.example.workshop.EmittersSwarm;
import org.example.workshop.ThreadQueueWorkshop;
import org.junit.jupiter.api.RepeatedTest;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class VirtualWorkshopTest {

    @RepeatedTest(10)
    public void givenTwoJobTypes_whenManyInvocationPerThread_thenHandleOneInvocationPerJobWhenFree() {
        final AtomicInteger deliveryCounter = new AtomicInteger(0);
        final AtomicInteger processingCounter = new AtomicInteger(0);

        ThreadQueueWorkshop<String> workshop = ThreadQueueWorkshop.<String>newBuilder()
                .registerWorker("delivery", new TestWorker("delivery", deliveryCounter))
                .registerWorker("processing", new TestWorker("processing", processingCounter))
                .build();

        EmittersSwarm swarm = EmittersSwarm.getBuilder()
                .setWorkshop(workshop)
                .waitOnEmission(20)
                .build();
        try (workshop; swarm) {
            swarm.startAndAwait();
        }

        assertEquals(5, deliveryCounter.get());
        assertEquals(5, processingCounter.get());
    }
}
