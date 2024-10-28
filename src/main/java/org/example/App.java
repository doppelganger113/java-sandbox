package org.example;

import org.example.workshop.*;

public class App {

    public static Workshop<String> threadQueueWorkshop() {
        return ThreadQueueWorkshop.<String>newBuilder()
                .registerWorker("delivery", new SlowWorker<>("delivery-worker"))
                .registerWorker("processing", new SlowWorker<>("processing-worker"))
                .build();
    }

    public static Workshop<String> virtualWorkshop() throws Throwable {
        return VirtualWorkshop.<String>getBuilder()
                .registerWorker("delivery", new SlowWorker<>("delivery-worker"))
                .registerWorker("processing", new SlowWorker<>("processing-worker"))
                .build();
    }

    public static void main(String[] args) throws Throwable {
        Workshop<String> workshop = virtualWorkshop();

        EmittersSwarm swarm = EmittersSwarm.getBuilder()
                .setWorkshop(workshop)
                .build();

        try (swarm; workshop) {
            swarm.startAndAwait();
        }

        System.out.println("Done.");
    }
}
