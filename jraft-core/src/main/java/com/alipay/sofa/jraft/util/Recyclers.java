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

        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;
        if (LOG.isDebugEnabled()) {
            if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
                LOG.debug("-Djraft.recyclers.maxCapacityPerThread: disabled");
            } else {
                LOG.debug("-Djraft.recyclers.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
            }
        }

        INITIAL_CAPACITY = Math.min(DEFAULT_MAX_CAPACITY_PER_THREAD, 256);
    }

    /**
     * 一个空的handle对象
     */
    public static final Handle NOOP_HANDLE = new Handle() {};

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
        // 弹出最后一个 handle 对象
        DefaultHandle handle = stack.pop();
        // 一开始还没有设置对象
        if (handle == null) {
            // 初始化一个handle 对象
            handle = stack.newHandle();
            // 将用户需要循环利用的对象保存到 handle 中
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    /**
     * 调用该方法 将对象保存起来 避免重复创建开销较大的对象
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
        // 必须确保 该handle 是由本对象返回的
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
     * 默认的处理器对象
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
            // 如果不是在本线程执行recycle  获取对应线程的 容器 不存在则创建
            Map<Stack<?>, WeakOrderQueue> delayedRecycled = Recyclers.delayedRecycled.get();
            // 维护 创建stack 的线程 与 调用recycle 线程的映射关系
            WeakOrderQueue queue = delayedRecycled.get(stack);
            if (queue == null) {
                delayedRecycled.put(stack, queue = new WeakOrderQueue(stack, thread));
            }
            // 在非创建 stack 的其它线程构建的 queue中添加可回收对象
            queue.add(this);
        }
    }

    /**
     * 创建一个 WeakHashMap 来
     */
    private static final ThreadLocal<Map<Stack<?>, WeakOrderQueue>> delayedRecycled = ThreadLocal.withInitial(WeakHashMap::new);

    // a queue that makes only moderate guarantees about visibility: items are seen in the correct order,
    // but we aren't absolutely guaranteed to ever see anything at all, thereby keeping the queue cheap to maintain
    private static final class WeakOrderQueue {
        private static final int LINK_CAPACITY = 16;

        // Let Link extend AtomicInteger for intrinsics. The Link itself will be used as writerIndex.

        /**
         * Link 之间通过一个单向链表连结
         */
        @SuppressWarnings("serial")
        private static final class Link extends AtomicInteger {
            private final DefaultHandle[] elements = new DefaultHandle[LINK_CAPACITY];

            private int readIndex;
            private Link next;
        }

        // chain of data items
        private Link head, tail;
        // pointer to another queue of delayed items for the same stack
        private WeakOrderQueue next;
        private final WeakReference<Thread> owner;
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

        void add(DefaultHandle handle) {
            handle.lastRecycledId = id;

            Link tail = this.tail;
            int writeIndex;
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                this.tail = tail = tail.next = new Link();
                writeIndex = tail.get();
            }
            tail.elements[writeIndex] = handle;
            handle.stack = null;
            // we lazy set to ensure that setting stack to null appears before we unnull it in the owning thread;
            // this also means we guarantee visibility of an element in the queue if we see the index updated
            tail.lazySet(writeIndex + 1);
        }

        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        // transfer as many items as we can from this queue to the stack, returning true if any were transferred
        @SuppressWarnings("rawtypes")
        boolean transfer(Stack<?> dst) {

            Link head = this.head;
            if (head == null) {
                return false;
            }

            if (head.readIndex == LINK_CAPACITY) {
                if (head.next == null) {
                    return false;
                }
                this.head = head = head.next;
            }

            final int srcStart = head.readIndex;
            int srcEnd = head.get();
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }

            final int dstSize = dst.size;
            final int expectedCapacity = dstSize + srcSize;

            if (expectedCapacity > dst.elements.length) {
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                srcEnd = Math.min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            if (srcStart != srcEnd) {
                final DefaultHandle[] srcElems = head.elements;
                final DefaultHandle[] dstElems = dst.elements;
                int newDstSize = dstSize;
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle element = srcElems[i];
                    if (element.recycleId == 0) {
                        element.recycleId = element.lastRecycledId;
                    } else if (element.recycleId != element.lastRecycledId) {
                        throw new IllegalStateException("recycled already");
                    }
                    element.stack = dst;
                    dstElems[newDstSize++] = element;
                    srcElems[i] = null;
                }
                dst.size = newDstSize;

                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    this.head = head.next;
                }

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
         * 该队列中 Thread 通过 WeakReference 引用   每个WeakOrderQueue 对应一个线程 当获取不到本线程的可回收对象时 会尝试从其他线程获取
         */
        private volatile WeakOrderQueue head;
        /**
         * 前后队列
         */
        private WeakOrderQueue cursor, prev;

        /**
         *
         * @param parent 回收对象
         * @param thread 持有stack 的线程
         * @param maxCapacity 该stack 允许存储的最大数量
         */
        Stack(Recyclers<T> parent, Thread thread, int maxCapacity) {
            this.parent = parent;
            this.thread = thread;
            this.maxCapacity = maxCapacity;
            // 初始化存储元素的槽
            elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
        }

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
                if (!scavenge()) {
                    return null;
                }
                size = this.size;
            }
            size--;
            // 从后往前返回handle 对象  注意handle 本身没有从数组中移除
            DefaultHandle ret = elements[size];
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            // 该对象被回收时 会将 recycleId lastRecycledId 重置
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            this.size = size;
            return ret;
        }

        /**
         * 清理数据
         * @return
         */
        boolean scavenge() {
            // continue an existing scavenge, if any
            if (scavengeSome()) {
                return true;
            }

            // reset our scavenge cursor
            prev = null;
            cursor = head;
            return false;
        }

        /**
         * 进行清理
         * @return
         */
        boolean scavengeSome() {
            WeakOrderQueue cursor = this.cursor;
            if (cursor == null) {
                cursor = head;
                if (cursor == null) {
                    return false;
                }
            }

            boolean success = false;
            WeakOrderQueue prev = this.prev;
            do {
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }

                WeakOrderQueue next = cursor.next;
                if (cursor.owner.get() == null) {
                    // If the thread associated with the queue is gone, unlink it, after
                    // performing a volatile read to confirm there is no data left to collect.
                    // We never unlink the first queue, as we don't want to synchronize on updating the head.
                    if (cursor.hasFinalData()) {
                        for (;;) {
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
         * 将handle 存储到槽中
         * @param item
         */
        void push(DefaultHandle item) {
            // 等同于 &&  如果这2个值被设置了 就代表了 当前已经被回收了
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            // 设置回收id
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            // 超过最大值 不再允许回收了 其实可能会超过该值 因为扩容会扩大1倍
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
