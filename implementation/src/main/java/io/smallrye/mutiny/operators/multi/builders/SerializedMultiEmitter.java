package io.smallrye.mutiny.operators.multi.builders;

import static io.smallrye.mutiny.helpers.Subscriptions.TERMINATED;

import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.mutiny.subscription.MultiSubscriber;

/**
 * Serializes calls to onItem, onFailure and onCompletion and their Reactive Streams equivalent.
 *
 * @param <T> the type of item
 */
public class SerializedMultiEmitter<T> implements MultiEmitter<T>, MultiSubscriber<T>, ContextSupport {

    private final AtomicInteger wip = new AtomicInteger();
    private final BaseMultiEmitter<T> downstream;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Queue<T> queue = Queues.createMpscQueue();

    private volatile boolean done;

    SerializedMultiEmitter(BaseMultiEmitter<T> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription s) {
        // do nothing.
    }

    @Override
    public void onItem(T item) {
        if (downstream.isCancelled() || done) {
            return;
        }
        if (item == null) {
            onFailure(new NullPointerException("`onItem` called with `null`"));
            return;
        }
        if (wip.compareAndSet(0, 1)) {
            downstream.emit(item);
            if (wip.decrementAndGet() == 0) {
                return;
            }
        } else {
            Queue<T> q = queue;
            synchronized (q) {
                q.offer(item);
            }
            if (wip.getAndIncrement() != 0) {
                return;
            }
        }
        drainLoop();
    }

    @Override
    public void onFailure(Throwable failure) {
        if (downstream.isCancelled() || done) {
            Infrastructure.handleDroppedException(failure);
            return;
        }
        if (failure == null) {
            failure = new NullPointerException("failure cannot be `null`");
        }
        if (this.failure.compareAndSet(null, failure)) {
            done = true;
            drain();
        }
    }

    @Override
    public void onCompletion() {
        if (downstream.isCancelled() || done) {
            return;
        }
        done = true;
        drain();
    }

    void drain() {
        if (wip.getAndIncrement() == 0) {
            drainLoop();
        }
    }

    void drainLoop() {
        BaseMultiEmitter<T> emitter = this.downstream;
        Queue<T> q = queue;
        int missed = 1;
        for (;;) {

            for (;;) {
                if (emitter.isCancelled()) {
                    q.clear();
                    return;
                }

                if (this.failure.get() != null) {
                    q.clear();
                    emitter.fail(this.failure.getAndSet(TERMINATED));
                    return;
                }
                boolean isDone = done;
                T item = q.poll();
                boolean isEmpty = item == null;
                if (isDone && isEmpty) {
                    emitter.complete();
                    return;
                }

                if (isEmpty) {
                    break;
                }

                emitter.emit(item);
            }

            missed = wip.addAndGet(-missed);
            if (missed == 0) {
                break;
            }
        }
    }

    @Override
    public MultiEmitter<T> emit(T item) {
        if (item == null) {
            fail(new NullPointerException("`emit` called with `null`."));
        }
        onItem(item);
        return this;
    }

    @Override
    public void fail(Throwable failure) {
        if (failure == null) {
            fail(new NullPointerException("`fail` called with `null`."));
        }
        onFailure(failure);
    }

    @Override
    public void complete() {
        onCompletion();
    }

    @Override
    public MultiEmitter<T> onTermination(Runnable onTermination) {
        downstream.onTermination(onTermination);
        return this;
    }

    @Override
    public boolean isCancelled() {
        return downstream.isCancelled();
    }

    @Override
    public long requested() {
        return downstream.requested();
    }

    @Override
    public MultiEmitter<T> onRequest(LongConsumer consumer) {
        downstream.onRequest(consumer);
        return this;
    }

    @Override
    public MultiEmitter<T> onCancellation(Runnable onCancellation) {
        downstream.onCancellation(onCancellation);
        return this;
    }

    @Override
    public Context context() {
        if (downstream instanceof ContextSupport) {
            return ((ContextSupport) downstream).context();
        } else {
            return Context.empty();
        }
    }
}
