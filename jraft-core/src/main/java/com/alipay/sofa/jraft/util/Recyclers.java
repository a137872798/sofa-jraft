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
package com.alipay.sofa.jraft.util;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Light-weight object pool based on a thread-local stack.
 * <p/>
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 * recycle netty中用于性能优化的类
 * @param <T> the type of the pooled object
 */
public abstract class Recyclers<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Recyclers.class);

    /**
     * id 生成器
     */
    private static final AtomicInteger idGenerator = new AtomicInteger(Integer.MIN_VALUE);

    private static final int OWN_THREAD_ID = idGenerator.getAndIncrement();
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024; // Use 4k instances as default.
    private static final int DEFAULT_MAX_CAPACITY_PER_THREAD;
    private static final int INITIAL_CAPACITY;

    static {
        // 每个线程最大存储数量
        int maxCapacityPerThread = SystemPropertyUtil.getInt("jraft.recyclers.maxCapacityPerThread", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD);
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        // 每条线程最多存放4096个对象
        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;
        if (LOG.isDebugEnabled()) {
            if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
                LOG.debug("-Djraft.recyclers.maxCapacityPerThread: disabled");
            } else {
                LOG.debug("-Djraft.recyclers.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
            }
        }

        // 取更小的值 该值只是初始化 可以进行扩容
        INITIAL_CAPACITY = Math.min(DEFAULT_MAX_CAPACITY_PER_THREAD, 256);
    }

    /**
     * 一个空的handle对象
     */
    public static final Handle NOOP_HANDLE = new Handle() {};

    /**
     * 每个回收对象中 每条线程最大存放的对象数量
     */
    private final int maxCapacityPerThread;

    /**
     * 基于本地线程存储的 栈对象
     */
    private final ThreadLocal<Stack<T>> threadLocal = new ThreadLocal<Stack<T>>() {

        @Override
        protected Stack<T> initialValue() {
            return new Stack<>(Recyclers.this, Thread.currentThread(), maxCapacityPerThread);
        }
    };

    protected Recyclers() {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD);
    }

    /**
     * 使用指定大小来初始化 recycler对象
     * @param maxCapacityPerThread
     */
    protected Recyclers(int maxCapacityPerThread) {
        this.maxCapacityPerThread = Math.max(0, maxCapacityPerThread);
    }

    /**
     * 允许在一个线程中多次调用get 和 recycle
     * @return
     */
    @SuppressWarnings("unchecked")
    public final T get() {
        // 存储大小为0 直接返回空handle对象
        if (maxCapacityPerThread == 0) {
            return newObject(NOOP_HANDLE);
        }
        // 这里会使用调用get() 方法的线程去初始化 threadLocal的值
        Stack<T> stack = threadLocal.get();
        // 弹出最后一个 handle 对象  (stack 中是一个 handle 数组)
        DefaultHandle handle = stack.pop();
        // 代表不仅本线程中没有 保留回收对象 且其他线程线程也没有
        if (handle == null) {
            // 初始化一个handle 对象
            handle = stack.newHandle();
            // 该方法由用户来实现  handle 只是确保用户可以获取到一些信息 一般来说直接返回需要被回收的对象就可以
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    /**
     * 调用该方法 将对象保存起来 避免重复创建开销较大的对象  又或者说避免那些会被反复创建的对象
     * 因为不断反复创建 会使得GC 更加频繁
     * 注意 回收的线程不一定是创建该对象的线程 一个对象可以在使用完后 切换到另一个线程进行保存 这也是为了方便吧
     * 如果一定要回到创建该对象的线程才能回收 对于用户来说是种麻烦
     * 这里需要用户自行保存 newObject(Handle handle) 时携带的 handle 对象 因为当回收对象时需要携带该对象
     * @param o  需要包保存的对象
     * @param handle  将包裹数据的 handle 传回来
     * @return
     */
    public final boolean recycle(T o, Handle handle) {
        // 该handle 是在 stack 对应的槽 长度限制为0 时返回的 所以不允许进行回收
        if (handle == NOOP_HANDLE) {
            return false;
        }

        DefaultHandle h = (DefaultHandle) handle;
        // 必须确保 该handle 是由本recycles返回的
        if (h.stack.parent != this) {
            return false;
        }
        // 必须确保 要回收的值就是handle 的值
        if (o != h.value) {
            throw new IllegalArgumentException("o does not belong to handle");
        }
        h.recycle();
        return true;
    }

    protected abstract T newObject(Handle handle);

    public final int threadLocalCapacity() {
        return threadLocal.get().elements.length;
    }

    public final int threadLocalSize() {
        return threadLocal.get().size;
    }

    public interface Handle {}

    /**
     * 默认的处理器对象  该对象内部维护了被重复利用的对象
     */
    static final class DefaultHandle implements Handle {

        // 当通过pop 返回handle 时 这2个变量的值会重置成0
        // 当调用recycle 时 这2个值会被设置
        private int lastRecycledId;
        private int recycleId;

        /**
         * 该对象是由哪个stack 创建的
         */
        private Stack<?> stack;
        /**
         * 代表可以循环利用的对象
         */
        private Object value;

        /**
         * 指定该对象所属的 stack (创建的对象 会被存到这个Stack中)
         * @param stack
         */
        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        /**
         * 回收对象
         */
        public void recycle() {
            // 获取调用该方法的线程 确保该线程就是生成stack 的线程后 本handle 对象存储到stack中
            Thread thread = Thread.currentThread();
            if (thread == stack.thread) {
                stack.push(this);
                return;
            }
            // we don't want to have a ref to the queue as the value in our weak map
            // so we null it out; to ensure there are no races with restoring it later
            // we impose a memory ordering here (no-op on x86)
            // 如果不是在本线程执行recycle  获取对应线程的容器 不存在则创建
            Map<Stack<?>, WeakOrderQueue> delayedRecycled = Recyclers.delayedRecycled.get();
            // 维护 创建stack 的线程 与 调用recycle 线程的映射关系  注意该 queue 是一个链表结构
            WeakOrderQueue queue = delayedRecycled.get(stack);
            if (queue == null) {
                // 注意将 调用recycle 的线程设置到 queue中 当下次相同线程 还想recycle时 会从delayedRecycled 找到该映射关系
                // 就不需要重复创建 queue 了
                delayedRecycled.put(stack, queue = new WeakOrderQueue(stack, thread));
            }
            // 在非创建 stack 的其它线程构建的 queue中添加可回收对象
            queue.add(this);
        }
    }

    /**
     * 针对某个 stack 如果生成的对象在其他线程调用了recycle 那么会被回收到 WeakOrderQueue
     */
    private static final ThreadLocal<Map<Stack<?>, WeakOrderQueue>> delayedRecycled = ThreadLocal.withInitial(WeakHashMap::new);

    // a queue that makes only moderate guarantees about visibility: items are seen in the correct order,
    // but we aren't absolutely guaranteed to ever see anything at all, thereby keeping the queue cheap to maintain
    // 当某个可回收对象被当前线程对应的 stack 创建时 却由另一个线程回收 那么 就会维护在该结构中
    private static final class WeakOrderQueue {
        private static final int LINK_CAPACITY = 16;

        // Let Link extend AtomicInteger for intrinsics. The Link itself will be used as writerIndex.

        @SuppressWarnings("serial")
        private static final class Link extends AtomicInteger {
            // 每个link 最多存放 16个  回收对象
            private final DefaultHandle[] elements = new DefaultHandle[LINK_CAPACITY];
            // 对应 elements 的指针
            private int readIndex;
            private Link next;
        }

        // chain of data items  保存链首 链尾 引用
        private Link head, tail;
        // pointer to another queue of delayed items for the same stack  WeakOrderQueue 之间以链表结构连接
        private WeakOrderQueue next;
        // 该队列对应的 线程
        private final WeakReference<Thread> owner;
        // 每个队列对应唯一的 id
        private final int id = idGenerator.getAndIncrement();

        /**
         *
         * @param stack 代表创建包裹 原对象的stack 内部包含创建对象的线程
         * @param thread 当前调用recycle 的线程(非 stack 线程 否则会直接加入到stack中)
         */
        WeakOrderQueue(Stack<?> stack, Thread thread) {
            // Link 通过一个链表结构进行连接
            head = tail = new Link();
            owner = new WeakReference<>(thread);
            synchronized (stackLock(stack)) {
                next = stack.head;
                stack.head = this;
            }
        }

        private Object stackLock(Stack<?> stack) {
            return stack;
        }

        /**
         * 往队列中添加可回收对象   因为基于 ThreadLocal不会有并发问题
         * @param handle
         */
        void add(DefaultHandle handle) {
            // 一旦设置id 就代表该对象已经被回收过了  注意这里只设置了 lastRecycledId
            handle.lastRecycledId = id;

            // 一个link 可以存放 16 个可回收对象 一旦超过该容量就创建新的link 并形成链表
            Link tail = this.tail;
            int writeIndex;
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                this.tail = tail = tail.next = new Link();
                writeIndex = tail.get();
            }
            tail.elements[writeIndex] = handle;
            // 注意 对象被queue 回收时 将stack引用置空了
            handle.stack = null;
            // we lazy set to ensure that setting stack to null appears before we unnull it in the owning thread;
            // this also means we guarantee visibility of an element in the queue if we see the index updated
            tail.lazySet(writeIndex + 1);
        }

        // 代表被获取的下标没有到达 写入的下标
        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        // transfer as many items as we can from this queue to the stack, returning true if any were transferred
        /**
         * 将本queue 中的 可回收对象全部转移到 stack中
         * @param dst
         * @return
         */
        @SuppressWarnings("rawtypes")
        boolean transfer(Stack<?> dst) {
            // 如果该对象中没有 存储 handle 返回false
            Link head = this.head;
            if (head == null) {
                return false;
            }

            // 代表head 已经读取完毕 切换到link 同样是惰性的
            if (head.readIndex == LINK_CAPACITY) {
                if (head.next == null) {
                    return false;
                }
                this.head = head = head.next;
            }

            final int srcStart = head.readIndex;
            int srcEnd = head.get();
            // 代表readIndex 等于写入的 下标 也就是没有元素可读
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }

            // 获取栈当前的尺寸大小 要确保不超过 stack的容量
            final int dstSize = dst.size;
            final int expectedCapacity = dstSize + srcSize;

            if (expectedCapacity > dst.elements.length) {
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                // 计算实际能写入的 数量
                srcEnd = Math.min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            // 开始转移对象
            if (srcStart != srcEnd) {
                // 一边从 head 读取 一边从 tail 写入  是为了避免竞争
                final DefaultHandle[] srcElems = head.elements;
                final DefaultHandle[] dstElems = dst.elements;
                int newDstSize = dstSize;
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle element = srcElems[i];
                    // 如果某个对象 添加到 stack 中 那么 (recycleId = lastRecycleId) != 0
                    // 如果被 queue 回收 那么 recycleId = 0  lastRecycleId != 0
                    if (element.recycleId == 0) {
                        // 注意这里的 recycleId 已经变成 queue 的id 了
                        element.recycleId = element.lastRecycledId;
                    // 代表该元素 已经被回收到stack 中了
                    } else if (element.recycleId != element.lastRecycledId) {
                        throw new IllegalStateException("recycled already");
                    }
                    // 将该handle 的 stack 还给它
                    element.stack = dst;
                    dstElems[newDstSize++] = element;
                    srcElems[i] = null;
                }
                // 更新长度
                dst.size = newDstSize;

                // 通过GC 回收无用的 Link
                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    this.head = head.next;
                }

                // 更新读取的指针
                head.readIndex = srcEnd;
                return true;
            } else {
                // The destination stack is full already.
                return false;
            }
        }
    }

    static final class Stack<T> {

        // we keep a queue of per-thread queues, which is appended to once only, each time a new thread other
        // than the stack owner recycles: when we run out of items in our stack we iterate this collection
        // to scavenge those that can be reused. this permits us to incur minimal thread synchronisation whilst
        // still recycling all items.
        // 该stack 对象由哪个 recyclers 创建
        final Recyclers<T> parent;
        /**
         * 存放该stack 的线程
         */
        final Thread thread;
        /**
         * 存储元素的槽  每个handle 看作一个存储对象的包装器 并且包含了 一些额外信息比如由哪个stack 存储
         */
        private DefaultHandle[] elements;
        /**
         * 槽的最大长度
         */
        private final int maxCapacity;
        /**
         * 当前存储元素的长度
         */
        private int size;

        /**
         * 该队列中 Thread 通过 WeakReference 引用   每个WeakOrderQueue 对应一个线程 它们形成一个链表结构
         * 当获取不到本线程的可回收对象时 会尝试从其他线程获取
         */
        private volatile WeakOrderQueue head;
        /**
         * 扫描 queue 的2个指针 都是不断增加 不会往回走
         * 直到扫描完尾部 没有找到对象 会重置指针 那么 下次才能获取到 前面指针的回收对象 为什么要这样设计???
         */
        private WeakOrderQueue cursor, prev;

        /**
         * 首先要明确 该对象对应 ThreadLocal.init() 方法中 也就是每条线程通过ThreadLocal获取对象时 都会生成一个对应
         * 于线程的 回收对象
         * @param parent 该栈对象是绑定在哪个回收对象上的
         * @param thread 持有stack 的线程
         * @param maxCapacity 该stack 允许存储的最大数量
         */
        Stack(Recyclers<T> parent, Thread thread, int maxCapacity) {
            this.parent = parent;
            this.thread = thread;
            this.maxCapacity = maxCapacity;
            // 初始化存储元素的槽  是一个handle对象  同时初始容量选择较小的值 也就意味着handle 对象可以扩容
            elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
        }

        /**
         * 进行扩容  当然不能超过 maxCapacity
         * @param expectedCapacity
         * @return
         */
        int increaseCapacity(int expectedCapacity) {
            int newCapacity = elements.length;
            int maxCapacity = this.maxCapacity;
            do {
                newCapacity <<= 1;
            } while (newCapacity < expectedCapacity && newCapacity < maxCapacity);

            newCapacity = Math.min(newCapacity, maxCapacity);
            if (newCapacity != elements.length) {
                elements = Arrays.copyOf(elements, newCapacity);
            }

            return newCapacity;
        }

        /**
         * 从槽中弹出handle 对象  默认先从本线程的 槽中获取
         * @return
         */
        DefaultHandle pop() {
            int size = this.size;
            // 代表本线程对应的槽没有对象可取
            if (size == 0) {
                // 尝试从其他线程 借用对象
                if (!scavenge()) {
                    // 借用失败 返回 null
                    return null;
                }
                // 成功情况 更新size
                size = this.size;
            }
            // 又减小size 是因为该对象马上要被弹出 数组元素减少
            size--;
            // 从后往前返回handle 对象  注意handle 本身没有从数组中移除
            DefaultHandle ret = elements[size];
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            // 当某个在 recycles 中的对象被弹出时 recycleId 和 lastRecycleId 会被重置
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            this.size = size;
            return ret;
        }

        /**
         * 尝试从其他线程获取被回收对象
         * @return
         */
        boolean scavenge() {
            // continue an existing scavenge, if any
            if (scavengeSome()) {
                return true;
            }

            // reset our scavenge cursor  扫描到末尾也没找到 就回到头部 如果指针已经在较后的位置了然后 前面的queue 有对象不就获取不到了吗
            // 设计如此还是疏忽???
            prev = null;
            cursor = head;
            return false;
        }

        /**
         * 从其他线程对应的 queue 中获取可回收元素
         * @return
         */
        boolean scavengeSome() {
            // 找到某个曾经回收过对象的线程对应的 queue
            WeakOrderQueue cursor = this.cursor;
            // 首次创建 stack 时 cursor为null
            if (cursor == null) {
                cursor = head;
                // 如果head 也是null 代表其他线程也没有回收对象
                if (cursor == null) {
                    return false;
                }
            }

            boolean success = false;
            // 首次调用时  prev 还是null
            WeakOrderQueue prev = this.prev;
            do {
                // 将目标queue 中存储的对象转移到 生成可回收对象的 stack中 (相当于是惰性转移)
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }

                // 当在 head 没有转移到 handle 时 顺着队列往下
                WeakOrderQueue next = cursor.next;
                // 配合weakReference 使用  (当线程runnable 执行完时 该对象并不会被回收)
                if (cursor.owner.get() == null) {
                    // If the thread associated with the queue is gone, unlink it, after
                    // performing a volatile read to confirm there is no data left to collect.
                    // We never unlink the first queue, as we don't want to synchronize on updating the head.
                    // 如果被回收的 线程对应的 queue 还有未转移的可回收对象  在本次回收后 本queue 将从链表中被移除
                    // 该对象和 ThreadLocal 的模式很像 也就是会有内存泄漏???
                    if (cursor.hasFinalData()) {
                        for (;;) {
                            // 将数据进行转移 如果失败 跳过本对象
                            if (cursor.transfer(this)) {
                                success = true;
                            } else {
                                break;
                            }
                        }
                    }
                    if (prev != null) {
                        prev.next = next;
                    }
                } else {
                    prev = cursor;
                }

                cursor = next;

            } while (cursor != null && !success);

            this.prev = prev;
            this.cursor = cursor;
            return success;
        }

        /**
         * 将handle 存储到槽中  只有确保 生成该stack的线程就是 归还对象的线程 才可以直接push
         * @param item
         */
        void push(DefaultHandle item) {
            // 等同于 &&  如果这2个值被设置了 就代表该对象已经被回收过一次了
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            // 标识该对象已经被回收
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            // 超过最大值 不再允许回收了 这里就是放弃掉了该对象 那么该对象会被GC自动回收吗???
            if (size >= maxCapacity) {
                // Hit the maximum capacity - drop the possibly youngest object.
                return;
            }
            // 进行扩容
            if (size == elements.length) {
                elements = Arrays.copyOf(elements, Math.min(size << 1, maxCapacity));
            }

            elements[size] = item;
            this.size = size + 1;
        }

        DefaultHandle newHandle() {
            return new DefaultHandle(this);
        }
    }
}
