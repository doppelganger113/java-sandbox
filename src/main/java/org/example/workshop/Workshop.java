package org.example.workshop;

import java.util.Set;

public interface Workshop<T> extends AutoCloseable {
    /**
     * Starts consumer threads if any or any other requirement needed for operating.
     */
    void run();

    /**
     * Names of the registered workers
     */
    Set<String> getWorkerNames();

    /**
     * This method is thread safe
     *
     * @return true if sending of the job was successful
     */
    boolean trySendJob(String workerName, T work);
}
