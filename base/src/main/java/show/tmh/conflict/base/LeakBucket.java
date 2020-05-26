package show.tmh.conflict.base;

import java.util.concurrent.*;

/**
 * 漏桶限流
 *
 * @author yuhao
 * @date 2020/5/26 4:12 下午
 */
public class LeakBucket {

    static class LockObject {
        volatile boolean pass;
    }

    private int burst;
    private int rate;
    private long duration;
    private BlockingQueue<LockObject> queue;
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    public LeakBucket(int rate, long duration, int burst, TimeUnit unit) {
        assert rate > 0 && burst >= 0;
        this.burst = burst;
        this.rate = rate;
        this.duration = unit.toMillis(duration);

        if (burst == 0) {
            queue = new SynchronousQueue<LockObject>(true);
        } else {
            queue = new ArrayBlockingQueue<LockObject>(burst, true);
        }

        init();
    }

    public boolean acquire() throws InterruptedException {
        LockObject lockObject = new LockObject();
        boolean offer = queue.offer(lockObject);
        if (!offer) {
            return false;
        }
        synchronized (lockObject) {
            if (!lockObject.pass) {
                lockObject.wait();
            }
        }
        return true;
    }

    private void init() {
        executorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    LockObject lockObject = queue.take();
                    synchronized (lockObject) {
                        lockObject.pass = true;
                        lockObject.notify();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }, 0, duration / rate, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) throws InterruptedException {
        final LeakBucket leakBucket = new LeakBucket(3, 1, 3, TimeUnit.SECONDS);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executorService.submit(new Runnable() {
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        try {
                            boolean acquire = leakBucket.acquire();
                            if (acquire) {
                                System.out.println(System.currentTimeMillis() / 1000);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }


    }

}
