package org.example.workshop;

public interface Worker<T> {
    void work(T value) throws Throwable;
}
