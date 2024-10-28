package org.example.workshop;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class VirtualWorkshop<T> implements Workshop<T> {

    public static class Builder<T> {
        private final HashMap<String, Worker<T>> workerMap = new HashMap<>();
        private final HashMap<String, Semaphore> semaphoreMap = new HashMap<>();

        public Builder<T> registerWorker(String name, Worker<T> worker) {
            workerMap.put(name, worker);
            return this;
        }

        public VirtualWorkshop<T> build() {
            workerMap.forEach((name, worker) -> {
                semaphoreMap.put(name, new Semaphore(1));
            });
            return new VirtualWorkshop<>(workerMap, semaphoreMap);
        }
    }

    public static <T> Builder<T> getBuilder() {
        return new Builder<>();
    }

    private final HashMap<String, Worker<T>> workerMap;
    private final HashMap<String, Semaphore> semaphoreMap;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private VirtualWorkshop(
            HashMap<String, Worker<T>> workerMap,
            HashMap<String, Semaphore> semaphoreMap
    ) {
        this.workerMap = workerMap;
        this.semaphoreMap = semaphoreMap;
    }

    public Set<String> getWorkerNames() {
        return workerMap.keySet();
    }

    @Override
    public void run() {
        // We have nothing to start :)
    }

    public boolean trySendJob(String name, T job) {
        Semaphore semaphore = semaphoreMap.get(name);
        if (semaphore == null) {
            throw new IllegalStateException("Semaphore for " + name + " does not exist");
        }
        Worker<T> worker = workerMap.get(name);
        if (worker == null) {
            throw new IllegalStateException("Worker for " + name + " does not exist");
        }
        boolean permit = semaphore.tryAcquire();
        if (!permit) return false;

        executor.submit(() -> {
            try {
                worker.work(job);
            } catch (Throwable e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            } finally {
                semaphore.release();
            }
        });

        return true;
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
