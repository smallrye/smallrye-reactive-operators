package io.smallrye.mutiny.operators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;

public class MultiCreateFromEmitterTest {

    @Test
    public void testWithDefaultBackPressure() {
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create();
        Multi<Integer> multi = Multi.createFrom().emitter(emitter -> {
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();
        });
        multi.subscribe(subscriber);
        subscriber.assertSubscribed()
                .request(Long.MAX_VALUE)
                .assertCompleted()
                .assertItems(1, 2, 3);
    }

    @Test
    public void testRequestsAtSubscription() {
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create(Long.MAX_VALUE);
        Multi<Integer> multi = Multi.createFrom().emitter(emitter -> {
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();
        });
        multi.subscribe(subscriber);
        subscriber.assertSubscribed()
                .assertCompleted()
                .assertItems(1, 2, 3);
    }

    @Test
    public void testWhenConsumerThrowsAnException() {
        AtomicBoolean onTerminationCalled = new AtomicBoolean();
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create();
        Multi<Integer> multi = Multi.createFrom().emitter(emitter -> {
            emitter.onTermination(() -> onTerminationCalled.set(true));
            emitter.emit(1);
            emitter.emit(2);
            throw new IllegalStateException("boom");
        });
        multi.subscribe(subscriber);
        subscriber.assertSubscribed()
                .request(Long.MAX_VALUE)
                .assertFailedWith(IllegalStateException.class, "boom")
                .assertItems(1, 2);
        assertThat(onTerminationCalled).isTrue();
    }

    @Test
    public void testWithRequests() {
        AtomicInteger terminated = new AtomicInteger();
        AtomicReference<MultiEmitter<? super Integer>> reference = new AtomicReference<>();
        Multi.createFrom().<Integer> emitter(emitter -> {
            reference.set(emitter);
            emitter.onTermination(terminated::incrementAndGet);
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();
        })
                .subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .run(() -> {
                    assertThat(reference.get()).isNotNull();
                    assertThat(reference.get().requested()).isEqualTo(0);
                })
                .request(2)
                .run(() -> {
                    // Already emitted
                    assertThat(reference.get().requested()).isEqualTo(0);
                })
                .assertNotTerminated()
                .assertItems(1, 2)
                .request(2)
                .assertItems(1, 2, 3)
                .assertCompleted();

        assertThat(terminated).hasValue(1);
    }

    @Test
    public void testWithMoreRequestsThanValue() {
        AtomicInteger terminated = new AtomicInteger();
        AtomicReference<MultiEmitter<? super Integer>> reference = new AtomicReference<>();
        Multi.createFrom().<Integer> emitter(emitter -> {
            reference.set(emitter);
            emitter.onTermination(terminated::incrementAndGet);
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();
        })
                .subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .run(() -> {
                    assertThat(reference.get()).isNotNull();
                    assertThat(reference.get().requested()).isEqualTo(0);
                })
                .request(2)
                .run(() -> {
                    // Already emitted
                    assertThat(reference.get().requested()).isEqualTo(0);
                })
                .assertNotTerminated()
                .assertItems(1, 2)
                .request(10)
                .assertItems(1, 2, 3)
                .run(() -> {
                    assertThat(reference.get().requested()).isEqualTo(10);
                })
                .assertCompleted();

        assertThat(terminated).hasValue(1);
    }

    @Test
    public void testCancellation() {
        AtomicInteger cancelled = new AtomicInteger();
        AssertSubscriber<Object> subscriber = Multi.createFrom().emitter(emitter -> {
            emitter.onTermination(cancelled::incrementAndGet);
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();
        })
                .subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .request(2)
                .assertNotTerminated()
                .assertItems(1, 2)
                .cancel()
                .request(1)
                .assertItems(1, 2)
                .assertNotTerminated();

        assertThat(cancelled).hasValue(1);
        subscriber.cancel();
        assertThat(cancelled).hasValue(1);
    }

    @Test
    public void testOnTerminationOnCompletion() {
        AtomicInteger termination = new AtomicInteger();
        AssertSubscriber<Integer> subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            emitter.onTermination(termination::incrementAndGet);
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();
        })
                .subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .request(2)
                .assertNotTerminated()
                .assertItems(1, 2)
                .request(1)
                .assertItems(1, 2, 3)
                .assertCompleted();

        assertThat(termination).hasValue(1);
        subscriber.cancel();
        assertThat(termination).hasValue(1);
    }

    @Test
    public void testOnTerminationOnFailure() {
        AtomicInteger termination = new AtomicInteger();
        AssertSubscriber<Integer> subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            emitter.onTermination(termination::incrementAndGet);
            emitter.emit(1);
            emitter.emit(2);
            emitter.fail(new IOException("boom"));
            emitter.emit(3);
            emitter.complete();
        })
                .subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .request(2)
                .assertItems(1, 2)
                .assertFailedWith(IOException.class, "boom")
                .request(1)
                .assertItems(1, 2);

        assertThat(termination).hasValue(1);
        subscriber.cancel();
        assertThat(termination).hasValue(1);
    }

    @Test
    public void testOnTerminationWhenEmpty() {
        AtomicInteger termination = new AtomicInteger();
        AssertSubscriber<Integer> subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            emitter.onTermination(termination::incrementAndGet);
            emitter.complete();
        })
                .subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .request(2)
                .assertHasNotReceivedAnyItem()
                .request(1)
                .assertHasNotReceivedAnyItem()
                .assertCompleted();

        assertThat(termination).hasValue(1);
        subscriber.cancel();
        assertThat(termination).hasValue(1);
    }

    @Test
    public void testCancellationWhenSubscriberThrowAnException() {
        AtomicBoolean errored = new AtomicBoolean();
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger numberOfResults = new AtomicInteger();

        Multi<Integer> source = Multi.createFrom().emitter(emitter -> {
            emitter.onTermination(() -> cancelled.set(true));
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();

        });

        //noinspection SubscriberImplementation
        source.subscribe(new Flow.Subscriber<Integer>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(10);
            }

            @Override
            public void onNext(Integer integer) {
                numberOfResults.incrementAndGet();
                throw new RuntimeException("BOOM!");
            }

            @Override
            public void onError(Throwable t) {
                errored.set(true);
            }

            @Override
            public void onComplete() {
                // ignored.
            }
        });

        assertThat(errored).isFalse();
        assertThat(cancelled).isTrue();
        assertThat(numberOfResults.get()).isEqualTo(1);
    }

    @Test
    public void testIgnoreBackPressureBehavior() {
        Multi<Integer> multi = Multi.createFrom().emitter(e -> e.emit(1).emit(2).emit(3).complete(),
                BackPressureStrategy.IGNORE);

        multi.subscribe().withSubscriber(AssertSubscriber.create(1))
                .assertSubscribed()
                .assertItems(1, 2, 3)
                .request(2)
                .assertItems(1, 2, 3)
                .assertCompleted();
    }

    @Test
    public void testWithoutBackPressure() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.complete();
        }, BackPressureStrategy.IGNORE).subscribe()
                .withSubscriber(AssertSubscriber.create())
                .assertCompleted();
        assertThat(subscriber.getItems()).hasSize(1000);

        subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.complete();
        }, BackPressureStrategy.IGNORE).subscribe()
                .withSubscriber(AssertSubscriber.create(5))
                // The request is ignored by the strategy.
                .assertCompleted();
        assertThat(subscriber.getItems()).hasSize(1000);
    }

    @Test
    public void testLatestBackPressureBehavior() {
        Multi<Integer> multi = Multi.createFrom().emitter(e -> e.emit(1).emit(2).emit(3).complete(),
                BackPressureStrategy.LATEST);

        multi.subscribe().withSubscriber(AssertSubscriber.create(1))
                .assertSubscribed()
                .assertItems(1)
                .request(2)
                .assertItems(1, 3)
                .assertCompleted();
    }

    @Test
    public void testWithLatestBackPressure() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.complete();
        }, BackPressureStrategy.LATEST).subscribe()
                .withSubscriber(AssertSubscriber.create(20))
                .request(Long.MAX_VALUE)
                .assertCompleted();
        // 21 because the 20 first are consumed, and then only the latest is kept.
        assertThat(subscriber.getItems()).hasSize(21);

        subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.complete();
        }, BackPressureStrategy.LATEST).subscribe()
                .withSubscriber(AssertSubscriber.create())
                .request(20)
                .request(Long.MAX_VALUE)
                .assertCompleted();
        assertThat(subscriber.getItems()).hasSize(1).containsExactly(999);

        Multi.createFrom().<Integer> emitter(MultiEmitter::complete, BackPressureStrategy.LATEST)
                .subscribe().withSubscriber(AssertSubscriber.create(20))
                .assertCompleted()
                .assertHasNotReceivedAnyItem();

        subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.fail(new IOException("boom"));
        }, BackPressureStrategy.LATEST).subscribe()
                .withSubscriber(AssertSubscriber.create())
                .request(20)
                .request(Long.MAX_VALUE)
                .assertFailedWith(IOException.class, "boom");
        assertThat(subscriber.getItems()).hasSize(1).containsExactly(999);

    }

    @Test
    public void testDropBackPressureBehavior() {
        Multi<Integer> multi = Multi.createFrom()
                .emitter(e -> e.emit(1).emit(2).emit(3).complete(), BackPressureStrategy.DROP);

        multi.subscribe().withSubscriber(AssertSubscriber.create(1))
                .assertSubscribed()
                .assertItems(1)
                .request(2)
                .assertItems(1)
                .assertCompleted();

        multi.subscribe().withSubscriber(AssertSubscriber.create(3))
                .assertSubscribed()
                .assertItems(1, 2, 3)
                .request(2)
                .assertItems(1, 2, 3)
                .assertCompleted();

        multi.subscribe().withSubscriber(AssertSubscriber.create(0))
                .assertSubscribed()
                .assertHasNotReceivedAnyItem()
                .request(2)
                .assertHasNotReceivedAnyItem()
                .assertCompleted();
    }

    @Test
    public void testWithDropBackPressure() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.complete();
        }, BackPressureStrategy.DROP).subscribe()
                .withSubscriber(AssertSubscriber.create(20))
                .request(Long.MAX_VALUE)
                .assertCompleted();
        // 20 because the 20 first are consumed, others are dropped
        assertThat(subscriber.getItems()).hasSize(20);

        subscriber = Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.complete();
        }, BackPressureStrategy.DROP).subscribe()
                .withSubscriber(AssertSubscriber.create())
                .request(20)
                .request(Long.MAX_VALUE)
                .assertCompleted();
        assertThat(subscriber.getItems()).isEmpty();

        Multi.createFrom().<Integer> emitter(MultiEmitter::complete, BackPressureStrategy.DROP)
                .subscribe().withSubscriber(AssertSubscriber.create(20))
                .assertCompleted()
                .assertHasNotReceivedAnyItem();

        Multi.createFrom().<Integer> emitter(emitter -> {
            IntStream.range(0, 1000).forEach(emitter::emit);
            emitter.fail(new IOException("boom"));
        }, BackPressureStrategy.DROP).subscribe()
                .withSubscriber(AssertSubscriber.create())
                .request(20)
                .request(Long.MAX_VALUE)
                .assertFailedWith(IOException.class, "boom")
                .assertHasNotReceivedAnyItem();
    }

    @Test
    public void testErrorBackPressureBehavior() {
        Multi<Integer> multi = Multi.createFrom().emitter(e -> e.emit(1).emit(2).emit(3).complete(),
                BackPressureStrategy.ERROR);

        multi.subscribe().withSubscriber(AssertSubscriber.create(1))
                .assertSubscribed()
                .assertItems(1)
                .assertFailedWith(BackPressureFailure.class, "requests");

        multi.subscribe().withSubscriber(AssertSubscriber.create(3))
                .assertSubscribed()
                .assertItems(1, 2, 3)
                .assertCompleted();

        Multi.createFrom().emitter(MultiEmitter::complete, BackPressureStrategy.ERROR)
                .subscribe().withSubscriber(AssertSubscriber.create())
                .assertCompleted()
                .assertHasNotReceivedAnyItem();
    }

    @Test
    public void testThatWeCanHaveMultipleSubscribers() {
        AtomicInteger count = new AtomicInteger();

        List<MultiEmitter<? super Integer>> emitters = new ArrayList<>();
        Multi<Integer> multi = Multi.createFrom().emitter(e -> {
            int i = count.incrementAndGet();
            emitters.add(e);
            e.emit(i);
            e.emit(i);
        });

        AssertSubscriber<Integer> subscriber1 = multi.subscribe()
                .withSubscriber(AssertSubscriber.create(10))
                .assertItems(1, 1)
                .assertNotTerminated();

        AssertSubscriber<Integer> subscriber2 = multi.subscribe()
                .withSubscriber(AssertSubscriber.create(10))
                .assertItems(2, 2)
                .assertNotTerminated();

        emitters.forEach(MultiEmitter::complete);
        subscriber1.assertCompleted();
        subscriber2.assertCompleted();

    }

    @Test
    public void testThatWeCanHaveMultipleSubscribersWhenUsingBackPressure() {
        AtomicInteger count = new AtomicInteger();

        List<MultiEmitter<? super Integer>> emitters = new ArrayList<>();
        Multi<Integer> multi = Multi.createFrom().emitter(e -> {
            int i = count.incrementAndGet();
            emitters.add(e);
            e.emit(i);
            e.emit(i);
        }, BackPressureStrategy.DROP);

        AssertSubscriber<Integer> subscriber1 = multi.subscribe()
                .withSubscriber(AssertSubscriber.create(10))
                .assertItems(1, 1)
                .assertNotTerminated();

        AssertSubscriber<Integer> subscriber2 = multi.subscribe()
                .withSubscriber(AssertSubscriber.create(10))
                .assertItems(2, 2)
                .assertNotTerminated();

        emitters.forEach(MultiEmitter::complete);
        subscriber1.assertCompleted();
        subscriber2.assertCompleted();

    }

    @Test
    public void testEmitterWithNegativeBufferSize() {
        assertThatThrownBy(() -> {
            Multi.createFrom().emitter(e -> {
            }, -1);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEmitterWithZeroAsBufferSize() {
        assertThatThrownBy(() -> {
            Multi.createFrom().emitter(e -> {
            }, 0);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEmitterWithBufferSize() {
        AssertSubscriber<Object> subscriber = Multi.createFrom().emitter(emitter -> {
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.complete();
        }, 5)
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.assertSubscribed()
                .request(2)
                .assertItems(1, 2)
                .request(1)
                .assertItems(1, 2, 3)
                .assertCompleted();
    }

    @Test
    public void testEmitterWithBufferSizeAndOverflow() {
        int bufferSize = 5;

        AssertSubscriber<Object> subscriber = Multi.createFrom().emitter(emitter -> {
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.emit(4);
            emitter.emit(5);
            // Overflow:
            emitter.emit(6);

            emitter.emit(7);

            emitter.complete();
        }, bufferSize)
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.assertSubscribed()
                .assertNotTerminated()
                .request(10)
                .assertItems(1, 2, 3, 4, 5)
                .assertFailedWith(BufferOverflowException.class, "emitter");
    }

    @Test
    public void testEmitterWithBufferSizeAndOverflowEndingWithFailure() {
        int bufferSize = 5;

        AssertSubscriber<Object> subscriber = Multi.createFrom().emitter(emitter -> {
            emitter.emit(1);
            emitter.emit(2);
            emitter.emit(3);
            emitter.emit(4);
            emitter.emit(5);
            // Overflow:
            emitter.emit(6);

            emitter.emit(7);

            emitter.fail(new Exception("boom"));
        }, bufferSize)
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.assertSubscribed()
                .assertNotTerminated()
                .request(10)
                .assertItems(1, 2, 3, 4, 5)
                .assertFailedWith(BufferOverflowException.class, "emitter");
    }

    @Test
    public void testBufferOverflowWhenUsingRequest() {
        int bufferSize = 5;
        AtomicReference<MultiEmitter<? super Integer>> reference = new AtomicReference<>();
        AssertSubscriber<Integer> subscriber = Multi.createFrom().<Integer> emitter(reference::set, bufferSize)
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        await().until(() -> reference.get() != null);

        reference.get().emit(1);
        reference.get().emit(2);
        reference.get().emit(3);

        subscriber.assertSubscribed()
                .assertNotTerminated()
                .assertHasNotReceivedAnyItem();

        // Buffer size: 3

        subscriber.request(3)
                .assertItems(1, 2, 3);

        // Buffer size: 0

        reference.get().emit(4).emit(5).emit(6);
        subscriber.assertNotTerminated();

        // Buffer size: 3
        reference.get().emit(7).emit(8).emit(9);

        // The failure is appended, so the subscriber won't get it before consuming everything first.
        subscriber.request(10);
        subscriber.assertFailedWith(BufferOverflowException.class, "emitter");
    }

    @Test
    public void rejectBadRequests() {
        Multi<Object> multi = Multi.createFrom().emitter(emitter -> {
            // Nothing
        });

        AssertSubscriber<Object> sub = multi.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(0L).assertFailedWith(IllegalArgumentException.class, "must be greater than 0");

        sub = multi.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(-1L).assertFailedWith(IllegalArgumentException.class, "must be greater than 0");
    }

    @Test
    public void onRquestAndCancellationCallbacks() {
        AtomicLong emitCounter = new AtomicLong();
        AtomicLong cancelCounter = new AtomicLong();
        Multi<String> multi = Multi.createFrom().emitter(emitter -> {
            emitter.onCancellation(cancelCounter::incrementAndGet);
            emitter.onRequest(n -> {
                emitCounter.addAndGet(n);
                for (int i = 0; i < n; i++) {
                    emitter.emit("yolo");
                }
            });
        });
        AssertSubscriber<String> sub = multi.subscribe().withSubscriber(AssertSubscriber.create(0));

        sub.request(1L);
        sub.assertItems("yolo");
        sub.request(2L);
        sub.assertItems("yolo", "yolo", "yolo");
        sub.cancel();

        sub.request(10L);
        sub.cancel();
        sub.request(10L);
        sub.cancel();

        assertThat(emitCounter.get()).isEqualTo(3L);
        assertThat(cancelCounter.get()).isEqualTo(1L);
    }
}
