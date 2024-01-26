package io.smallrye.mutiny.operators.multi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.MultiFlatten;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.infrastructure.Infrastructure;

class MultiConcatMapNoPrefetchTest {

    AtomicInteger upstreamRequestCount;
    Multi<Integer> upstream;

    @BeforeEach
    void setUp() {
        upstreamRequestCount = new AtomicInteger();
        upstream = Multi.createFrom().generator(() -> upstreamRequestCount, (counter, emitter) -> {
            int requestCount = counter.getAndIncrement();
            emitter.emit(requestCount);
            return counter;
        });
    }

    @ParameterizedTest
    @MethodSource("argsTransformToUni")
    void testTransformToUni(boolean prefetch, int[] upstreamRequests) {
        Multi<Integer> result = upstream.onItem()
                .transformToUni(integer -> Uni.createFrom().item(integer)
                        .onItem().delayIt().by(Duration.ofMillis(10)))
                .concatenate(prefetch);
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(5);
        result.runSubscriptionOn(Infrastructure.getDefaultExecutor()).subscribe(ts);
        ts.request(5);
        ts.awaitItems(10);
        assertThat(upstreamRequestCount).hasValue(upstreamRequests[0]);
        ts.request(1);
        ts.awaitItems(11);
        assertThat(upstreamRequestCount).hasValue(upstreamRequests[1]);
        ts.request(1);
        ts.awaitItems(12);
        assertThat(upstreamRequestCount).hasValue(upstreamRequests[2]);
    }

    private static Stream<Arguments> argsTransformToUni() {
        return Stream.of(
                Arguments.of(true, new int[] { 11, 12, 13 }),
                Arguments.of(false, new int[] { 10, 11, 12 }));
    }

    @ParameterizedTest
    @MethodSource("argsTransformToMulti")
    void testTransformToMulti(boolean prefetch, int[] upstreamRequests) {
        Multi<Integer> result = upstream.onItem()
                .transformToMulti(i -> Multi.createFrom().items(i, i))
                .concatenate(prefetch);
        AssertSubscriber<Integer> ts = new AssertSubscriber<>();
        result.runSubscriptionOn(Infrastructure.getDefaultExecutor()).subscribe(ts);
        ts.request(10);
        ts.awaitItems(10);
        assertThat(upstreamRequestCount).hasValue(upstreamRequests[0]);
        ts.request(1);
        ts.awaitItems(11);
        assertThat(upstreamRequestCount).hasValue(upstreamRequests[1]);
        ts.request(1);
        ts.awaitItems(12);
        assertThat(upstreamRequestCount).hasValue(upstreamRequests[2]);
    }

    private static Stream<Arguments> argsTransformToMulti() {
        return Stream.of(
                Arguments.of(true, new int[] { 6, 6, 7 }),
                Arguments.of(false, new int[] { 5, 6, 6 }));
    }

    @ParameterizedTest
    @MethodSource("argsNoPrefetchPostponeFailure")
    void testNoPrefetchPostponeFailure(boolean postponeFailure, Integer[] expectedItems, int expectedUpstreamRequest) {
        AtomicInteger itemCounter = new AtomicInteger();
        MultiFlatten<Integer, Integer> flatten = upstream.onItem().transformToMulti(i -> {
            if (itemCounter.incrementAndGet() == 3) {
                return Multi.createFrom().emitter(e -> {
                    e.emit(i);
                    e.fail(new IllegalArgumentException("3rd item"));
                });
            }
            return Multi.createFrom().items(i, i);
        });
        Multi<Integer> result = (postponeFailure ? flatten.collectFailures() : flatten).concatenate(false);
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(5);
        result.runSubscriptionOn(Infrastructure.getDefaultExecutor()).subscribe(ts);
        ts.request(5);
        ts.awaitItems(expectedItems.length);
        ts.assertItems(expectedItems);
        assertThat(upstreamRequestCount).hasValue(expectedUpstreamRequest);
    }

    private static Stream<Arguments> argsNoPrefetchPostponeFailure() {
        return Stream.of(
                Arguments.of(true, new Integer[] { 0, 0, 1, 1, 2, 3, 3, 4, 4, 5 }, 6),
                Arguments.of(false, new Integer[] { 0, 0, 1, 1, 2 }, 3));
    }

    @Test
    void testNoPrefetchWithConcatMapEmptyMulti() {
        Multi<Integer> result = upstream.select().first(20).onItem()
                .transformToMulti(i -> Multi.createFrom().<Integer> empty())
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(5);
        result.runSubscriptionOn(Infrastructure.getDefaultExecutor()).subscribe(ts);
        ts.request(5);
        ts.awaitCompletion();
        assertThat(upstreamRequestCount).hasValueGreaterThan(10);
        ts.assertHasNotReceivedAnyItem().assertCompleted();
    }

    @Test
    void testNoPrefetchWithConcatMapContainingEmpty() {
        Multi<Integer> result = upstream.onItem()
                .transformToMulti(i -> (i % 3 == 0) ? Multi.createFrom().empty() : Multi.createFrom().item(i))
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(5);
        result.runSubscriptionOn(Infrastructure.getDefaultExecutor()).subscribe(ts);
        ts.awaitSubscription();
        ts.request(5);
        ts.awaitItems(10);
        assertThat(upstreamRequestCount).hasValueGreaterThan(10);
        ts.assertItems(1, 2, 4, 5, 7, 8, 10, 11, 13, 14);
        ts.request(1);
        ts.awaitItems(11);
        assertThat(upstreamRequestCount).hasValueGreaterThan(11);
        ts.assertLastItem(16);
    }

    @Test
    void testNoPrefetchWithConcatMapEmptyUni() {
        Multi<Integer> result = upstream.select().first(20).onItem()
                .transformToUni(i -> Uni.createFrom().<Integer> nullItem())
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(5);
        result.runSubscriptionOn(Infrastructure.getDefaultExecutor()).subscribe(ts);
        ts.request(5);
        ts.awaitCompletion();
        assertThat(upstreamRequestCount).hasValueGreaterThan(10);
        ts.assertHasNotReceivedAnyItem().assertCompleted();
    }

    @Test
    void testMapperReturningNull() {
        Multi<Integer> result = upstream
                .onItem().transformToMulti(i -> {
                    if (i == 0) {
                        return Multi.createFrom().items(i);
                    }
                    return null;
                })
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(2);
        result.subscribe(ts);
        ts.assertItems(0);
        ts.assertFailedWith(NullPointerException.class);
    }

    @Test
    void testMapperReturningNullpostponeFailure() {
        Multi<Integer> result = upstream.select().first(5)
                .onItem().transformToUni(i -> (Uni<Integer>) null)
                .collectFailures()
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(5);
        result.subscribe(ts);
        ts.assertHasNotReceivedAnyItem().assertFailedWith(CompositeException.class);
    }

    @Test
    void testUpstreamFailure() {
        Multi<Integer> result = upstream
                .onItem().failWith(() -> new RuntimeException("boom"))
                .onItem().transformToUni(i -> Uni.createFrom().item(i))
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(1);
        result.subscribe(ts);
        ts.assertHasNotReceivedAnyItem().assertFailedWith(RuntimeException.class);
    }

    @Test
    void testUpstreamFailurepostponeFailure() {
        Multi<Integer> result = upstream.select().first(5)
                .onItem().failWith(() -> new RuntimeException("boom"))
                .onItem().transformToUni(i -> Uni.createFrom().item(i))
                .collectFailures()
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(1);
        result.subscribe(ts);
        ts.assertHasNotReceivedAnyItem().assertFailedWith(RuntimeException.class);
    }

    @Test
    void testCancelledSubscription() {
        CompletableFuture<Integer> inner = new CompletableFuture<>();
        AtomicBoolean upstreamCancelled = new AtomicBoolean();
        AtomicBoolean innerCancelled = new AtomicBoolean();
        Multi<Integer> result = upstream
                .onCancellation().invoke(() -> upstreamCancelled.set(true))
                .onItem().transformToUni(i -> Uni.createFrom().completionStage(inner)
                        .onCancellation().invoke(() -> innerCancelled.set(true)))
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(1);
        result.subscribe(ts);
        ts.cancel();
        assertThat(upstreamCancelled).isTrue();
        assertThat(innerCancelled).isTrue();
        ts.assertHasNotReceivedAnyItem();
    }

    @Test
    void testCancelledSubscriptionAfterTermination() {
        upstream = Multi.createFrom().generator(() -> upstreamRequestCount, (counter, emitter) -> {
            int requestCount = counter.getAndIncrement();
            emitter.emit(requestCount);
            emitter.complete();
            return counter;
        });
        CompletableFuture<Integer> inner = new CompletableFuture<>();
        AtomicBoolean innerCancelled = new AtomicBoolean();
        AtomicBoolean downstreamCancelled = new AtomicBoolean();
        Multi<Integer> result = upstream
                .onItem().transformToUni(i -> Uni.createFrom().completionStage(inner)
                        .emitOn(Infrastructure.getDefaultExecutor())
                        .onCancellation().invoke(() -> innerCancelled.set(true)))
                .concatenate()
                .onCancellation().invoke(() -> downstreamCancelled.set(true));
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(1);
        result.subscribe(ts);
        ts.cancel();
        assertThat(innerCancelled).isTrue();
        assertThat(downstreamCancelled).isTrue();
    }

    @Test
    void testInnerCompleteSubscriptionAfterTermination() {
        upstream = Multi.createFrom().generator(() -> upstreamRequestCount, (counter, emitter) -> {
            int requestCount = counter.getAndIncrement();
            emitter.emit(requestCount);
            emitter.complete();
            return counter;
        });
        CompletableFuture<Integer> inner = new CompletableFuture<>();
        Multi<Integer> result = upstream
                .onItem().transformToUni(i -> Uni.createFrom().completionStage(inner)
                        .emitOn(Infrastructure.getDefaultExecutor()))
                .concatenate();
        AssertSubscriber<Integer> ts = new AssertSubscriber<>(1);
        result.subscribe(ts);
        inner.complete(0);
        ts.awaitCompletion();
    }

    @Test
    void testPostponedFailureWithBoundaryDemand() {
        AssertSubscriber<String> sub = Multi.createFrom().items(1, 2, 3)
                .onItem().transformToMulti(n -> Multi.createFrom().<String> emitter(emitter -> {
                    emitter.emit("foo");
                    emitter.emit("bar");
                    emitter.fail(new Exception("baz"));
                })).collectFailures().concatenate()
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.request(2);
        sub.assertNotTerminated().assertItems("foo", "bar");

        sub.request(2);
        sub.assertNotTerminated().assertItems("foo", "bar", "foo", "bar");

        sub.request(3);
        sub.assertTerminated().assertItems("foo", "bar", "foo", "bar", "foo", "bar");

        sub.assertFailedWith(CompositeException.class);
        assertThat(((CompositeException) sub.getFailure()).getCauses())
                .hasSize(3)
                .allMatch(err -> "baz".equals(err.getMessage()));
    }

    @Test
    void testUpfrontCompletion() {
        AssertSubscriber<Integer> sub = Multi.createFrom().empty()
                .onItem().transformToMultiAndConcatenate(n -> Multi.createFrom().items(1, 2, 3))
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.assertCompleted().assertHasNotReceivedAnyItem();
    }

    @Test
    void rejectBadRequests() {
        Multi<Integer> multi = upstream.onItem().transformToMultiAndConcatenate(n -> Multi.createFrom().item(1));

        AssertSubscriber<Integer> sub = multi.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(0L);
        sub.assertHasNotReceivedAnyItem().assertFailedWith(IllegalArgumentException.class,
                "Invalid request number, must be greater than 0");

        sub = multi.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(-10L);
        sub.assertHasNotReceivedAnyItem().assertFailedWith(IllegalArgumentException.class,
                "Invalid request number, must be greater than 0");
    }

    @Test
    void testCancellation() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AssertSubscriber<Integer> sub = Multi.createFrom().ticks().every(Duration.ofMillis(100))
                .onCancellation().invoke(() -> cancelled.set(true))
                .onItem().transformToMultiAndConcatenate(tick -> Multi.createFrom().items(1, 2, 3))
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        await().atMost(Duration.ofSeconds(3)).until(() -> sub.getItems().size() > 12);
        sub.cancel();
        await().atMost(Duration.ofSeconds(3)).until(cancelled::get);
        assertThat(sub.isCancelled()).isTrue();
        assertThat(sub.hasCompleted()).isFalse();
        assertThat(sub.getItems()).contains(1, 2, 3);
    }

    @Test
    void simpleConcatMap() {
        AssertSubscriber<Integer> sub = Multi.createFrom().range(1, 3)
                .onItem().transformToMultiAndConcatenate(n -> Multi.createFrom().items(n * 10, n * 20))
                .subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(1);
        sub.assertItems(10);
        sub.request(2);
        sub.assertItems(10, 20, 20);
        sub.request(Long.MAX_VALUE);
        sub.assertItems(10, 20, 20, 40);
        sub.assertCompleted();
    }

    @RepeatedTest(1000)
    void earlyRequestWithNullInner() {
        var sub = Multi.createFrom().items(1, 2, 3)
                .emitOn(Infrastructure.getDefaultExecutor())
                .onItem().call(i -> Uni.createFrom().item(i))
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.request(1);
        sub.awaitNextItems(1, Duration.ofSeconds(1));
        sub.cancel();
    }
}
