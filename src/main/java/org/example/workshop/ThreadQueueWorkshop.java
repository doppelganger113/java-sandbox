package org.example.workshop;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Workshop contains thread pool with thread per each registered worker. It receives notifications of jobs, but
 * does not queue them up, only handling latest events as soon as it finishes processing.
 */
public class ThreadQueueWorkshop<T> implements Workshop<T> {

    private record Message<T>(T value, boolean shouldShutdown) {
    }

    private final Message<T> POISON_PILL = new Message<>(null, true);

    public static class Builder<T> {
        private final HashMap<String, Worker<T>> workerMap = new HashMap<>();
        private boolean shouldIgnoreInFlightMessage = false;

        public Builder<T> registerWorker(String name, Worker<T> worker) {
            workerMap.put(name, worker);
            return this;
        }

        public Builder<T> skipInFlightMessage() {
            shouldIgnoreInFlightMessage = true;
            return this;
        }

        public ThreadQueueWorkshop<T> build() {
            if (workerMap.isEmpty()) {
                throw new IllegalArgumentException("workerMap is empty");
            }

            ExecutorService executor = Executors.newFixedThreadPool(workerMap.size());
            HashMap<String, LinkedBlockingQueue<Message<T>>> jobQueueMap = new HashMap<>(workerMap.size());

            workerMap.forEach((key, worker) -> {
                LinkedBlockingQueue<Message<T>> queue = new LinkedBlockingQueue<>(1);
                jobQueueMap.put(key, queue);
            });

            return new ThreadQueueWorkshop<>(workerMap, executor, jobQueueMap, shouldIgnoreInFlightMessage);
        }
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    private final HashMap<String, Worker<T>> workerMap;
    private final HashMap<String, LinkedBlockingQueue<Message<T>>> jobQueueMap;
    private final ExecutorService executor;
    private final boolean shouldIgnoreInFlightMessage;
    private volatile boolean isShutdown = false;

    private ThreadQueueWorkshop(
            HashMap<String, Worker<T>> workerMap,
            ExecutorService executor,
            HashMap<String, LinkedBlockingQueue<Message<T>>> jobQueueMap,
            boolean shouldIgnoreInFlightMessage
    ) {
        this.workerMap = workerMap;
        this.jobQueueMap = jobQueueMap;
        this.executor = executor;
        this.shouldIgnoreInFlightMessage = shouldIgnoreInFlightMessage;
    }

    public Set<String> getWorkerNames() {
        return workerMap.keySet();
    }

    public void run() {
        System.out.println("[Workshop] starting...");

        workerMap.forEach((key, worker) -> {
            LinkedBlockingQueue<Message<T>> queue = jobQueueMap.get(key);
            if (queue == null) {
                throw new RuntimeException("No queue for key: " + key);
            }

            executor.submit(() -> {
                while (!isShutdown) {
                    try {
                        Message<T> message = queue.take();
                        if (message.shouldShutdown) {
                            break;
                        }
                        worker.work(message.value);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    } finally {
                        // When we don't want the stored in-flight messages in queue and want to use newer ones
                        // after finishing of processing
                        if (shouldIgnoreInFlightMessage) {
                            queue.clear();
                        }
                    }
                }
                System.out.println("Consumer closed");
            });
        });
    }

    public boolean trySendJob(String workerName, T work) {
        // In the shutdown state we want to prevent sending messages so we can send our dead pill without trouble
        if (isShutdown) {
            throw new IllegalStateException("Workshop is shutdown, cannot send job");
        }
        LinkedBlockingQueue<Message<T>> workerQueue = jobQueueMap.get(workerName);
        if (workerQueue == null) {
            throw new IllegalArgumentException("worker " + workerName + " not found");
        }
        return workerQueue.offer(new Message<>(work, false));
    }

    @Override
    public void close() {
        System.out.println("[Workshop] closing");
        isShutdown = true;
        try {
            jobQueueMap.forEach((key, queue) -> {
                try {
                    // With put, we wait if necessary to ensure we send this message
                    queue.offer(POISON_PILL);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            // We shut the executor down after sending the poison message so it gets to process it
            executor.close();
        }
        System.out.println("[Workshop] closed");
    }
}
