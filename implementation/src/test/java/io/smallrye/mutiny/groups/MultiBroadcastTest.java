package io.smallrye.mutiny.groups;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.MultiEmitterProcessor;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import junit5.support.InfrastructureResource;

@ResourceLock(value = InfrastructureResource.NAME, mode = ResourceAccessMode.READ)
public class MultiBroadcastTest {

    @Test
    public void testPublishToAllSubscribers() {
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();

        Multi<Integer> multi = processor.toMulti().broadcast().toAllSubscribers();

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);
        s1.assertItems(1, 2, 3).assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(1));
        s2.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(4).emit(5);
        // 5 is not received, because we use the lower number of requests.
        s1.assertItems(1, 2, 3, 4).assertNotTerminated();
        s2.assertItems(4).assertNotTerminated();

        processor.complete();

        s1.assertNotTerminated();
        s2.assertNotTerminated();

        // No one completed, because the are still ongoing items, and not enough requests
        s2.request(10);

        s1.assertCompleted();
        s2.assertCompleted();
    }

    @Test
    public void testPublishToAllSubscribersWithFailure() {
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();

        Multi<Integer> multi = processor.toMulti().broadcast().toAllSubscribers();

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);
        s1.assertItems(1, 2, 3).assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(1));
        s2.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(4).emit(5);
        // 5 is not received, because we use the lower number of requests.
        s1.assertItems(1, 2, 3, 4).assertNotTerminated();
        s2.assertItems(4).assertNotTerminated();

        processor.fail(new IOException("boom"));

        s1.assertFailedWith(IOException.class, "boom");
        s2.assertFailedWith(IOException.class, "boom");
    }

    @Test
    public void testPublishAfterDepartureOfASubscriber() {
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();

        Multi<Integer> multi = processor.toMulti().broadcast().toAllSubscribers();

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);
        s1.assertItems(1, 2, 3).assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(1));
        s2.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(4).emit(5);
        // 5 is not received, because we use the lower number of requests.
        s1.assertItems(1, 2, 3, 4).assertNotTerminated();
        s2.assertItems(4).assertNotTerminated();

        processor.complete();

        s1.assertNotTerminated();
        s2.assertNotTerminated();

        s2.cancel();

        s1.assertItems(1, 2, 3, 4, 5);
        s1.assertCompleted();
    }

    @Test
    public void testNoCancellationEvenWithoutSubscribers() {
        AtomicBoolean cancelled = new AtomicBoolean();
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();
        processor.onTermination(() -> cancelled.set(true));

        Multi<Integer> multi = processor.toMulti().broadcast().toAllSubscribers();

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);
        s1.assertItems(1, 2, 3).assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(1));
        s2.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(4).emit(5);
        s1.assertItems(1, 2, 3, 4).assertNotTerminated();
        s2.assertItems(4).assertNotTerminated();

        s2.cancel();
        assertThat(cancelled).isFalse();
        s1.cancel();
        assertThat(cancelled).isFalse();
        assertThat(processor.isCancelled()).isFalse();
        AssertSubscriber<Integer> s3 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        processor.emit(23);
        processor.complete();
        s3.assertItems(23).assertCompleted();
    }

    @Test
    public void testSubscriptionAfterUpstreamCompletion() {
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();
        processor.emit(1).emit(2).emit(3);
        processor.complete();

        processor.toMulti().broadcast().toAllSubscribers().subscribe().withSubscriber(AssertSubscriber.create(10))
                .assertItems(1, 2, 3)
                .assertCompleted();
    }

    @Test
    public void testPublishAtLeast() {
        AssertSubscriber<Integer> s1 = AssertSubscriber.create(10);
        AssertSubscriber<Integer> s2 = AssertSubscriber.create(10);

        Multi<Integer> multi = Multi.createFrom().range(1, 5).broadcast().toAtLeast(2);

        multi.subscribe(s1);

        s1.assertNotTerminated()
                .assertHasNotReceivedAnyItem();

        multi.subscribe(s2);

        s1.assertCompleted()
                .assertItems(1, 2, 3, 4);
        s2.assertCompleted()
                .assertItems(1, 2, 3, 4);
    }

    @Test
    public void testCancellationAfterLastDeparture() {
        AtomicBoolean cancelled = new AtomicBoolean();
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();
        processor.onTermination(() -> cancelled.set(true));

        Multi<Integer> multi = processor.toMulti().broadcast().withCancellationAfterLastSubscriberDeparture()
                .toAllSubscribers();

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);
        s1.assertItems(1, 2, 3).assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(1));
        s2.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(4).emit(5);
        s1.assertItems(1, 2, 3, 4).assertNotTerminated();
        s2.assertItems(4).assertNotTerminated();

        s2.cancel();
        assertThat(cancelled).isFalse();
        assertThat(processor.isCancelled()).isFalse();
        s1.cancel();
        assertThat(cancelled).isTrue();
        assertThat(processor.isCancelled()).isTrue();
    }

    @Test
    public void testCancellationAfterLastDepartureWithDuration() {
        AtomicBoolean cancelled = new AtomicBoolean();
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();
        processor.onTermination(() -> cancelled.set(true));

        Multi<Integer> multi = processor.toMulti().broadcast()
                .withCancellationAfterLastSubscriberDeparture(Duration.ofSeconds(1))
                .toAllSubscribers();

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);
        s1.assertItems(1, 2, 3).assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(1));
        s2.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(4).emit(5);
        s1.assertItems(1, 2, 3, 4).assertNotTerminated();
        s2.assertItems(4).assertNotTerminated();

        s2.cancel();
        assertThat(cancelled).isFalse();
        s1.cancel();
        assertThat(cancelled).isFalse();
        await().until(cancelled::get);
    }

    @Test
    public void testCancellationWithAtLeastAfterLastDeparture() {
        AtomicBoolean cancelled = new AtomicBoolean();
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();
        processor.onTermination(() -> cancelled.set(true));

        Multi<Integer> multi = processor.toMulti().broadcast().withCancellationAfterLastSubscriberDeparture()
                .toAtLeast(2);

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);

        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));

        s1.assertItems(1, 2, 3).assertNotTerminated();
        s2.assertItems(1, 2, 3).assertNotTerminated();

        s2.cancel();
        assertThat(cancelled).isFalse();
        s1.cancel();
        assertThat(cancelled).isTrue();
    }

    @Test
    public void testCancellationWithAtLeastAfterLastDepartureAndDelay() {
        AtomicBoolean cancelled = new AtomicBoolean();
        MultiEmitterProcessor<Integer> processor = MultiEmitterProcessor.create();
        processor.onTermination(() -> cancelled.set(true));

        Multi<Integer> multi = processor.toMulti().broadcast()
                .withCancellationAfterLastSubscriberDeparture(Duration.ofSeconds(1))
                .toAtLeast(2);

        AssertSubscriber<Integer> s1 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));
        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        processor.emit(1).emit(2).emit(3);

        s1.assertHasNotReceivedAnyItem().assertNotTerminated();

        AssertSubscriber<Integer> s2 = multi.subscribe().withSubscriber(AssertSubscriber.create(10));

        s1.assertItems(1, 2, 3).assertNotTerminated();
        s2.assertItems(1, 2, 3).assertNotTerminated();

        s2.cancel();
        assertThat(cancelled).isFalse();
        s1.cancel();
        assertThat(cancelled).isFalse();
        await().until(cancelled::get);
    }

    @Test
    public void testRequests() {
        Multi<Integer> multi = Multi.createFrom().range(0, 1000)
                .broadcast().toAtLeast(2);

        AssertSubscriber<Integer> subscriber1 = new AssertSubscriber<>(Long.MAX_VALUE);
        AssertSubscriber<Integer> subscriber2 = new AssertSubscriber<>(100);

        multi.subscribe().withSubscriber(subscriber1);
        multi.subscribe().withSubscriber(subscriber2);

        await().until(() -> subscriber1.getItems().size() == 100 && subscriber2.getItems().size() == 100);

        subscriber2.request(1000);

        subscriber1.awaitCompletion();
        assertThat(subscriber1.getItems()).hasSize(1000);
        subscriber2.awaitCompletion();
        assertThat(subscriber2.getItems()).hasSize(1000);
    }

    @Test
    public void rejectBadRequests() {
        MultiEmitterProcessor<Object> emitter = MultiEmitterProcessor.create();
        AssertSubscriber<Object> sub = AssertSubscriber.create();
        emitter.subscribe(sub);
        sub.request(0L).assertFailedWith(IllegalArgumentException.class, "must be greater than 0");

        emitter = MultiEmitterProcessor.create();
        sub = AssertSubscriber.create();
        emitter.subscribe(sub);
        sub.request(-1L).assertFailedWith(IllegalArgumentException.class, "must be greater than 0");
    }

    @Test
    public void requestAndCancelCallbacks() {
        AtomicLong emitCounter = new AtomicLong();
        AtomicLong cancelCounter = new AtomicLong();

        MultiEmitterProcessor<Integer> emitter = MultiEmitterProcessor.create();
        AssertSubscriber<Integer> sub = AssertSubscriber.create();
        emitter.subscribe(sub);

        emitter.onRequest(n -> {
            emitCounter.addAndGet(n);
            for (long i = 0; i < n; i++) {
                emitter.emit(69);
            }
        });
        emitter.onCancellation(cancelCounter::incrementAndGet);

        sub.request(1L);
        sub.assertItems(69);
        sub.request(2L);
        sub.assertItems(69, 69, 69);

        sub.cancel();
        sub.request(10L);
        sub.cancel();

        assertThat(sub.getItems()).hasSize(3);
        assertThat(emitCounter.get()).isEqualTo(3L);
        assertThat(cancelCounter.get()).isEqualTo(1L);
    }
}
