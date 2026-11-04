import java.util.LinkedList;
import java.util.Queue;

public class BoundedBlockingQueueSync<T> {
    private final Queue<T> queue = new LinkedList<>();
    private final int capacity;

    public BoundedBlockingQueueSync(int capacity) { this.capacity = capacity; }

    public synchronized void enqueue(T item) throws InterruptedException {
        while (queue.size() == capacity) wait();
        queue.add(item);
        notifyAll();                 // must wake everyone: one monitor, mixed waiters
    }

    public synchronized T dequeue() throws InterruptedException {
        while (queue.isEmpty()) wait();
        T item = queue.poll();
        notifyAll();
        return item;
    }

    public synchronized int size() { return queue.size(); }

    public static void main(String[] args) {
        BoundedBlockingQueue<Integer> q = new BoundedBlockingQueue<>(2);

        Thread producer = new Thread(() -> {
            try { for (int i = 0; i < 6; i++) { q.enqueue(i); System.out.println("put " + i); } }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        Thread consumer = new Thread(() -> {
            try { for (int i = 0; i < 6; i++) { System.out.println("took " + q.dequeue()); Thread.sleep(200); } }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        producer.start();
        consumer.start();
    }

}
