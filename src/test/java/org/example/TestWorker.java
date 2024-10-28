package org.example;

import org.example.workshop.Color;
import org.example.workshop.Worker;

import java.util.concurrent.atomic.AtomicInteger;

public record TestWorker(String name, AtomicInteger counter) implements Worker<String> {
    @Override
    public void work(String value) throws InterruptedException {
        System.out.println(Color.CYAN + "[Worker](" + name + ") consuming " + value + Color.RESET);
        counter.incrementAndGet();
        Thread.sleep(50);
    }
}
