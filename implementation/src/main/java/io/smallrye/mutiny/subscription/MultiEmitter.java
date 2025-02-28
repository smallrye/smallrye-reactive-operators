package io.smallrye.mutiny.subscription;

import java.util.concurrent.Flow.Subscription;
import java.util.function.LongConsumer;

import io.smallrye.mutiny.Multi;

/**
 * An object allowing to send signals to the downstream {@link Multi}.
 * {@link Multi} propagates several item event, once a failure or completion event is fired, the other events have
 * no effects.
 * <p>
 * Emitting a {@code null} item is invalid and will cause a failure.
 *
 * @param <T> the expected type of items.
 */
public interface MultiEmitter<T> extends ContextSupport {

    /**
     * Emits an {@code item} event downstream.
     * <p>
     * Calling this method after a failure or a completion events has no effect.
     *
     * @param item the item, must not be {@code null}
     * @return this emitter, so firing item events can be chained.
     */
    MultiEmitter<T> emit(T item);

    /**
     * Emits a {@code failure} event downstream with the given exception.
     * <p>
     * Calling this method multiple times or after the {@link #complete()} method has no effect.
     *
     * @param failure the exception, must not be {@code null}
     */
    void fail(Throwable failure);

    /**
     * Emits a {@code completion} event downstream indicating that no more item will be sent.
     * <p>
     * Calling this method multiple times or after the {@link #fail(Throwable)} method has no effect.
     */
    void complete();

    /**
     * Attaches a @{code termination} event handler invoked when the downstream {@link Subscription} is cancelled,
     * or when the emitter has emitted either a {@code completion} or {@code failure} event.
     * <p>
     * This method allows cleanup resources once the emitter can be disposed (has reached a terminal state).
     * <p>
     * If the registration of the {@code onTermination} callback is done after the termination, it invokes the callback
     * immediately.
     *
     * @param onTermination the action to run on termination, must not be {@code null}
     * @return this emitter
     */
    MultiEmitter<T> onTermination(Runnable onTermination);

    /**
     * @return {@code true} if the downstream cancelled the stream or the emitter was terminated (with a completion
     *         or failure events).
     */
    boolean isCancelled();

    /**
     * @return the current outstanding request amount.
     */
    long requested();

    /**
     * Defines a callback for {@link Subscription#request(long)} signals.
     * <p>
     * This is useful to facilitate the implementation of back-pressured emissions.
     *
     * @param consumer the callback
     * @return this emitter
     */
    default MultiEmitter<T> onRequest(LongConsumer consumer) {
        throw new UnsupportedOperationException("To be implemented");
    }

    /**
     * Defines a callback for {@link Subscription#cancel()} signals.
     * <p>
     * This is useful to facilitate the implementation of cancellation logic.
     *
     * @param onCancellation the callback
     * @return this emitter
     */
    default MultiEmitter<T> onCancellation(Runnable onCancellation) {
        throw new UnsupportedOperationException("To be implemented");
    }
}
