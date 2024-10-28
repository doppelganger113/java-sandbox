package org.example.workshop;

public class SlowWorker<T> implements Worker<T> {

    private final String name;

    public SlowWorker(String name) {
        this.name = name;
    }

    @Override
    public void work(T value) throws InterruptedException {
        System.out.println(Color.CYAN + "[Worker](" + name + ") starting to work on " + value + Color.RESET);
        Thread.sleep(3_000);
    }
}
