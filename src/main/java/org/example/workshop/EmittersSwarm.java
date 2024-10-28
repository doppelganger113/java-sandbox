package org.example.workshop;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EmittersSwarm implements AutoCloseable {

    public static class Builder {
        private Workshop<String> workshop;
        private int numOfEmitters = 5;
        private int numOfEmissions = 10;
        private int waitMls = 1_000;

        public Builder() {
        }

        public Builder setWorkshop(Workshop<String> workshop) {
            this.workshop = workshop;
            return this;
        }

        public Builder numOfEmitters(int numOfEmitters) {
            this.numOfEmitters = numOfEmitters;
            return this;
        }

        public Builder numOfEmissions(int numOfEmissions) {
            this.numOfEmissions = numOfEmissions;
            return this;
        }

        public Builder waitOnEmission(int waitMls) {
            this.waitMls = waitMls;
            return this;
        }

        public EmittersSwarm build() {
            if (workshop == null) {
                throw new IllegalStateException("You must specify a workshop");
            }

            return new EmittersSwarm(workshop, numOfEmitters, numOfEmissions, waitMls);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Workshop<String> workshop;
    private final ExecutorService executor;
    private final int numOfEmitters;
    private final int numOfEmissions;
    private final int waitMls;

    private EmittersSwarm(
            Workshop<String> workshop,
            int numOfEmitters,
            int numOfEmissions,
            int waitMls
    ) {
        this.workshop = workshop;
        this.numOfEmitters = numOfEmitters;
        this.numOfEmissions = numOfEmissions;
        this.waitMls = waitMls;
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void startAndAwait() {
        System.out.println("[EmittersSwarm] starting...");
        workshop.run();

        String[] workerNames = workshop.getWorkerNames().toArray(new String[0]);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < numOfEmitters; i++) {
            final int emitterId = i;
            System.out.println("[Emitter](" + emitterId + ") created");

            tasks.add(() -> {
                for (int j = 0; j < numOfEmissions; j++) {
                    try {
                        String workerName = workerNames[emitterId % workerNames.length];

                        boolean success = workshop.trySendJob(workerName, "[Work](" + emitterId + ") " + j);
                        String color = success ? Color.GREEN : Color.RED;
                        System.out.println(
                                "[Emitter](" + emitterId + ") --" + workerName + "--> " + j + " " + color + success + Color.RESET
                        );
                        Thread.sleep(waitMls);
                    } catch (InterruptedException e) {
                        System.out.println(
                                "[Emitter] (" + emitterId + ")" + e.getMessage() + " " + Arrays.toString(e.getStackTrace())
                        );
                    }
                }
                System.out.println("[Emitter](" + emitterId + ") closed.");

                return null;
            });
        }

        System.out.println("[EmittersSwarm] waiting to finish...");
        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("[EmittersSwarm] done.");
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
