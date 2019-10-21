/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alipay.sofa.jraft.util.timer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.Platform;

/**
 * <h3>Implementation Details</h3>
 * <p>
 * {@link HashedWheelTimer} is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 * <p>
 *
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 * 同 netty的hash轮
 */
public class HashedWheelTimer implements Timer {

    private static final Logger                                      LOG                    = LoggerFactory
                                                                                                .getLogger(HashedWheelTimer.class);

    /**
     * 应该是允许执行的最大任务数
     */
    private static final int                                         INSTANCE_COUNT_LIMIT   = 256;
    /**
     * 计数器对象
     */
    private static final AtomicInteger                               instanceCounter        = new AtomicInteger();
    /**
     * 当任务实例过多时是否发出警告
     */
    private static final AtomicBoolean                               warnedTooManyInstances = new AtomicBoolean();

    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> workerStateUpdater     = AtomicIntegerFieldUpdater
                                                                                                .newUpdater(
                                                                                                    HashedWheelTimer.class,
                                                                                                    "workerState");

    /**
     * 该对象负责触发内部的 timeout 对象
     */
    private final Worker                                             worker                 = new Worker();
    /**
     * 执行 worker的线程
     */
    private final Thread                                             workerThread;

    public static final int                                          WORKER_STATE_INIT      = 0;
    public static final int                                          WORKER_STATE_STARTED   = 1;
    public static final int                                          WORKER_STATE_SHUTDOWN  = 2;
    @SuppressWarnings({ "unused", "FieldMayBeFinal" })
    private volatile int                                             workerState;                                                  // 0 - init, 1 - started, 2 - shut down

    private final long                                               tickDuration;
    /**
     * 存放 timeout的 hash轮
     */
    private final HashedWheelBucket[]                                wheel;
    private final int                                                mask;
    private final CountDownLatch                                     startTimeInitialized   = new CountDownLatch(1);
    /**
     * 存放超时对象
     */
    private final Queue<HashedWheelTimeout>                          timeouts               = new ConcurrentLinkedQueue<>();
    /**
     * 存放被关闭的对象
     */
    private final Queue<HashedWheelTimeout>                          cancelledTimeouts      = new ConcurrentLinkedQueue<>();
    private final AtomicLong                                         pendingTimeouts        = new AtomicLong(0);
    private final long                                               maxPendingTimeouts;

    private volatile long                                            startTime;

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     * 设置固定的 时间间隔(也就是该时间段内的任务都会存放在一个slot中)
     * @param tickDuration the duration between tick
     * @param unit         the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, -1);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory      a {@link ThreadFactory} that creates a
     *                           background {@link Thread} which is dedicated to
     *                           {@link TimerTask} execution.
     * @param tickDuration       the duration between tick
     * @param unit               the time unit of the {@code tickDuration}
     * @param ticksPerWheel      the size of the wheel
     * @param maxPendingTimeouts The maximum number of pending timeouts after which call to   代表最多等待时间 超过该时间执行的任务会抛出Rejected异常
     *                           {@code newTimeout} will result in
     *                           {@link RejectedExecutionException}
     *                           being thrown. No maximum pending timeouts limit is assumed if
     *                           this value is 0 or negative.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel,
                            long maxPendingTimeouts) {

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }

        // Normalize ticksPerWheel to power of two and initialize the wheel.
        // 根据时间间隔创建hash轮
        wheel = createWheel(ticksPerWheel);
        mask = wheel.length - 1;

        // Convert tickDuration to nanos.
        this.tickDuration = unit.toNanos(tickDuration);

        // Prevent overflow.
        if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                "tickDuration: %d (expected: 0 < tickDuration in nanos < %d", tickDuration, Long.MAX_VALUE
                                                                                            / wheel.length));
        }
        workerThread = threadFactory.newThread(worker);

        this.maxPendingTimeouts = maxPendingTimeouts;

        // 注意 instanceCounter 是全局变量  也就是 Hash轮对象本身不应该创建太多
        if (instanceCounter.incrementAndGet() > INSTANCE_COUNT_LIMIT
            && warnedTooManyInstances.compareAndSet(false, true)) {
            // 打印提示信息
            reportTooManyInstances();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            // This object is going to be GCed and it is assumed the ship has sailed to do a proper shutdown. If
            // we have not yet shutdown then we want to make sure we decrement the active instance count.
            if (workerStateUpdater.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                instanceCounter.decrementAndGet();
            }
        }
    }

    /**
     * 根据时间间隔构建hash轮
     * @param ticksPerWheel
     * @return
     */
    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException("ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }

        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        switch (workerStateUpdater.get(this)) {
            // 如果处在init 状态 开始执行runnable
            case WORKER_STATE_INIT:
                if (workerStateUpdater.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // Wait until the startTime is initialized by the worker.
        // 阻塞主线程 直到任务正常启动
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    /**
     * 停止定时任务 并返回一组还未处理完毕的任务
     * @return
     */
    @Override
    public Set<Timeout> stop() {
        // 定时任务线程内部不允许 关闭 定时器
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(HashedWheelTimer.class.getSimpleName() + ".stop() cannot be called from "
                                            + TimerTask.class.getSimpleName());
        }

        if (!workerStateUpdater.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            if (workerStateUpdater.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                instanceCounter.decrementAndGet();
            }

            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                // 打断定时线程 并等待一会儿
                workerThread.interrupt();
                try {
                    // 为了 给 worker线程一点时间 将任务 移动到 unprocessed队列中
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            instanceCounter.decrementAndGet();
        }
        // 返回无法处理的任务
        return worker.unprocessedTimeouts();
    }

    /**
     * 从外部添加一个定时任务 这里没有直接处理 添加到轮的复杂逻辑(相对而言) 而是先存放在一个队列中 通过worker 去队列中拉取任务并设置到 bucket中
     * @param task
     * @param delay
     * @param unit
     * @return
     */
    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();

        // 超过最大任务数 拒绝执行
        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts (" + pendingTimeoutsCount
                                                 + ") is greater than or equal to maximum allowed pending "
                                                 + "timeouts (" + maxPendingTimeouts + ")");
        }

        // 调用该方法 会被动启动线程
        start();

        // Add the timeout to the timeout queue which will be processed on the next tick.
        // During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

        // Guard against overflow.  超过Long 最大值了
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        // 任务先加入到队列中
        timeouts.add(timeout);
        return timeout;
    }

    /**
     * Returns the number of pending timeouts of this {@link Timer}.
     * 获取当前等待执行的任务总数
     */
    public long pendingTimeouts() {
        return pendingTimeouts.get();
    }

    private static void reportTooManyInstances() {
        String resourceType = HashedWheelTimer.class.getSimpleName();
        LOG.error("You are creating too many {} instances.  {} is a shared resource that must be "
                  + "reused across the JVM, so that only a few instances are created.", resourceType, resourceType);
    }

    /**
     * 封装了 执行定时任务的 逻辑
     */
    private final class Worker implements Runnable {
        /**
         * 内部存储一组还未执行的 定时任务
         */
        private final Set<Timeout> unprocessedTimeouts = new HashSet<>();

        /**
         * 代表当前嘀嗒了几次
         */
        private long               tick;

        @Override
        public void run() {
            // Initialize the startTime.
            startTime = System.nanoTime();
            if (startTime == 0) {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            // Notify the other threads waiting for the initialization at start().
            startTimeInitialized.countDown();

            do {
                // 沉睡 一个 嘀嗒的时长
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    // tick 代表当前是第几次嘀嗒 通过与掩码的位运算 获取数组下标
                    int idx = (int) (tick & mask);
                    // 清理被关闭的任务
                    processCancelledTasks();
                    // 获取对应的bucket对象
                    HashedWheelBucket bucket = wheel[idx];
                    // 将定时任务从 队列中移动到 bucket中
                    transferTimeoutsToBuckets();
                    // 开始执行在 deadline 时长内的全部任务
                    bucket.expireTimeouts(deadline);
                    // 增加一次嘀嗒声
                    tick++;
                }
            } while (workerStateUpdater.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // Fill the unprocessedTimeouts so we can return them from stop() method.
            // 代表定时器被停止了  这时会将未处理的任务都移动到 unprocessedTimeouts 中 这样 调用stop 时 就可以看到未处理的任务了
            for (HashedWheelBucket bucket : wheel) {
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            // 将还没来得及 放入 bucket中的任务也设置到 processed队列中
            for (;;) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            processCancelledTasks();
        }

        /**
         * 执行桶中的任务
         */
        private void transferTimeoutsToBuckets() {
            // transfer only max. 100000 timeouts per tick to prevent a thread to stale the workerThread when it just
            // adds new timeouts in a loop.
            for (int i = 0; i < 100000; i++) {
                // 定时任务一开始是加入到队列中的  每次达到对应的时间时 将任务转移到 对应的 bucket中
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                // 如果该任务已经被关闭了就不执行了
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    // Was cancelled in the meantime.
                    continue;
                }

                // 代表本应该在第几次嘀嗒时执行 因为同一个数组中可能存在着不同轮数的任务  deadline 应该是基于startTime 计算出来的
                long calculated = timeout.deadline / tickDuration;
                // 计算该任务距离执行还需要几轮 应该会在其他地方继续更新 并在为0 的时候才允许执行
                timeout.remainingRounds = (calculated - tick) / wheel.length;

                final long ticks = Math.max(calculated, tick); // Ensure we don't schedule for past.
                int stopIndex = (int) (ticks & mask);

                // 将任务拉到bucket 中
                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);
            }
        }

        /**
         * 处理被关闭的任务
         */
        private void processCancelledTasks() {
            for (;;) {
                // 看来cancelledTimeouts 存放的是被关闭的任务 同时 该任务还存在于bucket 中 在每次sleep tick的时长后 才会挨个将任务从bucket中移除出来
                // 这意味着 最合理的访问bucket的时机就是 sleep 结束的时候 所以需要额外的内存去存放 被关闭的任务
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                try {
                    timeout.remove();
                } catch (Throwable t) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("An exception was thrown while process a cancellation task", t);
                    }
                }
            }
        }

        /**
         * calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         * 等待一个 tick的时长
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         */
        private long waitForNextTick() {
            // 每个 tickDuration 刚好对应一个时间槽的长度 该算法的特点就是每当经过一个时间槽的时长就将数组中存放的任务挨个执行
            long deadline = tickDuration * (tick + 1);

            for (;;) {
                // 每次进入新的循环 需要重新计算一次 currentTime
                final long currentTime = System.nanoTime() - startTime;
                // 这里的 + 999999 应该是抵消 除法去尾法带来的影响    deadline - currentTime 代表还需要阻塞的时长
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                // 当出现 <= 0 就代表已经sleep 了足够长的时间
                if (sleepTimeMs <= 0) {
                    // 代表该线程被打断 了
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        // 代表等待的真实时长
                        return currentTime;
                    }
                }

                // Check if we run on windows, as if thats the case we will need
                // to round the sleepTime as workaround for a bug that only affect
                // the JVM if it runs on windows.
                //
                // See https://github.com/netty/netty/issues/356
                if (Platform.isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    if (workerStateUpdater.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        public Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    /**
     * 代表可超时的任务对象
     */
    private static final class HashedWheelTimeout implements Timeout {

        private static final int                                           ST_INIT       = 0;
        private static final int                                           ST_CANCELLED  = 1;
        private static final int                                           ST_EXPIRED    = 2;
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER = AtomicIntegerFieldUpdater
                                                                                             .newUpdater(
                                                                                                 HashedWheelTimeout.class,
                                                                                                 "state");

        private final HashedWheelTimer                                     timer;
        private final TimerTask                                            task;
        private final long                                                 deadline;

        @SuppressWarnings({ "unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
        private volatile int                                               state         = ST_INIT;

        // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
        // HashedWheelTimeout will be added to the correct HashedWheelBucket.
        long                                                               remainingRounds;

        // This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
        // As only the workerThread will act on it there is no need for synchronization / volatile.
        HashedWheelTimeout                                                 next;
        HashedWheelTimeout                                                 prev;

        // The bucket to which the timeout was added
        HashedWheelBucket                                                  bucket;

        /**
         * 初始化 定时任务对象
         * @param timer  执行任务的定时器
         * @param task   任务逻辑
         * @param deadline   执行时间
         */
        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean cancel() {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            // 关闭就是将本对象加入到 一个暂存的队列中 通过bucket 引用 在bucket中移除对应的任务对象
            timer.cancelledTimeouts.add(this);
            return true;
        }

        void remove() {
            // 不设置bucket的情况下 实际上在bucket遍历内部的任务时也会将被关闭的任务移除
            HashedWheelBucket bucket = this.bucket;
            if (bucket != null) {
                bucket.remove(this);
            } else {
                timer.pendingTimeouts.decrementAndGet();
            }
        }

        public boolean compareAndSetState(int expected, int state) {
            return STATE_UPDATER.compareAndSet(this, expected, state);
        }

        public int state() {
            return state;
        }

        @Override
        public boolean isCancelled() {
            return state() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state() == ST_EXPIRED;
        }

        /**
         * 代表 到了执行任务的时间
         */
        public void expire() {
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                // 执行任务  外部任务会被封装成 task 用户通过实现 run 方法执行定时任务
                task.run(this);
            } catch (Throwable t) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;

            StringBuilder buf = new StringBuilder(192).append(getClass().getSimpleName()).append('(')
                .append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining).append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining).append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(", task: ").append(task()).append(')').toString();
        }
    }

    /**
     * Bucket that stores HashedWheelTimeouts. These are stored in a linked-list like datastructure to allow easy
     * removal of HashedWheelTimeouts in the middle. Also the HashedWheelTimeout act as nodes themself and so no
     * extra object creation is needed.
     * hash桶
     */
    private static final class HashedWheelBucket {
        // Used for the linked-list datastructure  内部数据 按照链表结构连接 为啥要维护一个tail 节点???
        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;

        /**
         * Add {@link HashedWheelTimeout} to this bucket.
         * 将任务移动到bucket 中 标准链表操作
         */
        public void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }

        /**
         * Expire all {@link HashedWheelTimeout}s for the given {@code deadline}.
         * 执行等待时间在deadline 内的所有任务
         */
        public void expireTimeouts(long deadline) {
            // bucket中 timeout 以一个链表进行连接
            HashedWheelTimeout timeout = head;

            // process all timeouts
            while (timeout != null) {
                HashedWheelTimeout next = timeout.next;
                // 确保 剩余轮数为0 才可以执行
                if (timeout.remainingRounds <= 0) {
                    // 将任务从队列中移除
                    next = remove(timeout);
                    // 代表到达执行时间 执行任务
                    if (timeout.deadline <= deadline) {
                        timeout.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new IllegalStateException(String.format("timeout.deadline (%d) > deadline (%d)",
                            timeout.deadline, deadline));
                    }
                // 只有轮数还没到0的才允许被移除 到了0 即使设置关闭也会执行
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    // 每当能轮到某个桶 也就代表着这个bucket中的全部任务都小了一轮
                    timeout.remainingRounds--;
                }
                timeout = next;
            }
        }

        /**
         * 将timeout 从链表中移除
         * @param timeout
         * @return
         */
        public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            // 将对象从链表中移除
            if (timeout.prev != null) {
                timeout.prev.next = next;
            }
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    head = next;
                }
            } else if (timeout == tail) {
                // if the timeout is the tail modify the tail to be the prev node.
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            // 减少当前悬置的 定时任务
            timeout.timer.pendingTimeouts.decrementAndGet();
            return next;
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         * 将未处理的任务 转移到 set中
         */
        public void clearTimeouts(Set<Timeout> set) {
            for (;;) {
                HashedWheelTimeout timeout = pollTimeout();
                if (timeout == null) {
                    return;
                }
                // 超时或被关闭的任务就不需要被返回了
                if (timeout.isExpired() || timeout.isCancelled()) {
                    continue;
                }
                set.add(timeout);
            }
        }

        /**
         * 挨个 将链表中的所有任务取出
         * @return
         */
        private HashedWheelTimeout pollTimeout() {
            HashedWheelTimeout head = this.head;
            if (head == null) {
                return null;
            }
            HashedWheelTimeout next = head.next;
            if (next == null) {
                tail = this.head = null;
            } else {
                this.head = next;
                next.prev = null;
            }

            // null out prev and next to allow for GC.
            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
        }
    }
}
