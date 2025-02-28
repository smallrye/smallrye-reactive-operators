package io.smallrye.mutiny;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.*;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.groups.*;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;
import io.smallrye.mutiny.tuples.Functions;

/**
 * A {@link Uni} represents a lazy asynchronous action. It follows the subscription pattern, meaning that the action
 * is only triggered once a {@link UniSubscriber} subscribes to the {@link Uni}.
 * <p>
 * A {@link Uni} can have two outcomes:
 * <ol>
 * <li>An {@code item} event, forwarding the completion of the action (potentially {@code null} if the item
 * does not represent a value, but the action was completed successfully)</li>
 * <li>A {@code failure} event, forwarding an exception</li>
 * </ol>
 * <p>
 * To trigger the computation, a {@link UniSubscriber} must subscribe to the Uni. It will be notified of the outcome
 * once there is an {@code item} or {@code failure} event fired by the observed Uni. A subscriber receives
 * (asynchronously) a {@link UniSubscription} and can cancel the demand at any time. Note that cancelling after
 * having received the outcome is a no-op.
 * <p>
 *
 * @param <T> the type of item produced by the {@link Uni}
 */
public interface Uni<T> {

    /**
     * Creates a new {@link Uni} from various sources such as {@link CompletionStage},
     * {@link UniEmitter}, direct values, {@link Exception}...
     *
     * <p>
     * Examples:
     * </p>
     *
     * <pre>
     * {@code
     * Uni.createFrom().item(1); // Emit 1 at subscription time
     * Uni.createFrom().item(() -> x); // Emit x at subscription time, the supplier is invoked for each subscription
     * Uni.createFrom().completionStage(cs); // Emit the item from this completion stage
     * Uni.createFrom().completionStage(() -> cs); // Emit the item from this completion stage, the stage is not created before subscription
     * Uni.createFrom().failure(exception); // Emit the failure at subscription time
     * Uni.createFrom().deferred(() -> Uni.createFrom().item(x)); // Defer the uni creation until subscription. Each subscription can produce a different uni
     * Uni.createFrom().item(null); // Emit null at subscription time
     * Uni.createFrom().nothing(); // Create a Uni not emitting any signal
     * Uni.createFrom().publisher(publisher); // Create a Uni from a Reactive Streams Publisher
     * }
     * </pre>
     *
     * @return the factory used to create {@link Uni} instances.
     * @see UniCreate
     */
    @CheckReturnValue
    static UniCreate createFrom() {
        return UniCreate.INSTANCE;
    }

    /**
     * Allows structuring the pipeline by creating a logic separation:
     *
     * <pre>
     * {@code
     *     Uni uni = upstream
     *      .stage(u -> { ...})
     *      .stage(u -> { ...})
     *      .stage(u -> { ...})
     * }
     * </pre>
     * <p>
     * With `stage` you can structure and chain groups of processing.
     *
     * @param stage the function receiving this {@link Uni} as parameter and producing the outcome (can be a
     *        {@link Uni} or something else), must not be {@code null}.
     * @param <O> the outcome type
     * @return the outcome of the function.
     */
    @CheckReturnValue
    default <O> O stage(Function<Uni<T>, O> stage) {
        return nonNull(stage, "stage").apply(this);
    }

    /**
     * Creates a new {@link Uni} combining several others unis such as {@code all} or {@code any}.
     *
     * @return the factory use to combine the uni instances
     */
    @CheckReturnValue
    static UniCombine combine() {
        return UniCombine.INSTANCE;
    }

    /**
     * Requests the {@link Uni} to start resolving the item and allows configuring how the signals are propagated
     * (using a {@link UniSubscriber}, callbacks, or a {@link CompletionStage}. Unlike {@link #await()}, this method
     * configures non-blocking retrieval of the item and failure.
     *
     * <p>
     * Examples:
     * </p>
     *
     * <pre>
     * {@code
     *     Uni<String> uni = ...;
     *
     *    Subscription sub = uni.subscribe().with( // The return subscription can be used to cancel the operation
     *              item -> {},           // Callback calls on item
     *              failure -> {}           // Callback calls on failure
     *    );
     *
     *    UniSubscriber<String> myUniSubscriber = ...
     *    uni.subscribe().withSubscriber(myUniSubscriber); // Subscribes to the Uni with the passed subscriber
     *
     *    CompletableFuture future = uni.subscribe().asCompletableFuture(); // Get a CompletionStage receiving the item or failure
     *    // Cancelling the returned future cancels the subscription.
     * }
     * </pre>
     *
     * @return the object to configure the subscription.
     * @see #await() <code>uni.await() </code>for waiting (blocking the caller thread) until the resolution of the observed Uni.
     */
    @CheckReturnValue
    UniSubscribe<T> subscribe();

    /**
     * Shortcut for {@link UniSubscribe#asCompletionStage()}.
     *
     * @return the completion stage receiving the items emitted by this {@link Uni}
     */
    @CheckReturnValue
    default CompletableFuture<T> subscribeAsCompletionStage() {
        return subscribe().asCompletionStage();
    }

    /**
     * Shortcut for {@link UniSubscribe#asCompletionStage(Context)}.
     *
     * @param context the context, cannot {@code null}
     * @return the completion stage receiving the items emitted by this {@link Uni}
     */
    @CheckReturnValue
    default CompletableFuture<T> subscribeAsCompletionStage(Context context) {
        return subscribe().asCompletionStage(context);
    }

    /**
     * Awaits (blocking the caller thread) until the item or a failure is emitted by the observed {@link Uni}.
     * If the observed uni fails, the failure is thrown. In the case of a checked exception, the exception is wrapped
     * into a {@link java.util.concurrent.CompletionException}.
     *
     * <p>
     * Examples:
     * </p>
     *
     * <pre>
     * {@code
     * Uni<T> uni = ...;
     * T res = uni.await().indefinitely(); // Await indefinitely until it get the item.
     * T res = uni.await().atMost(Duration.ofMillis(1000)); // Awaits at most 1s. After that, a TimeoutException is thrown
     * Optional<T> res = uni.await().asOptional().indefinitely(); // Retrieves the item as an Optional, empty if the item is null
     * }
     * </pre>
     *
     * @return the object to configure the retrieval.
     */
    @CheckReturnValue
    UniAwait<T> await();

    /**
     * Awaits (blocking the caller thread) until the item or a failure is emitted by the observed {@link Uni}.
     * If the observed uni fails, the failure is thrown. In the case of a checked exception, the exception is wrapped
     * into a {@link java.util.concurrent.CompletionException}.
     *
     * <p>
     * Examples:
     * </p>
     *
     * <pre>
     * {@code
     * Uni<T> uni = ...;
     * T res = uni.awaitUsing(context).indefinitely(); // Await indefinitely until it get the item.
     * T res = uni.awaitUsing(context).atMost(Duration.ofMillis(1000)); // Awaits at most 1s. After that, a TimeoutException is thrown
     * Optional<T> res = uni.awaitUsing(context).asOptional().indefinitely(); // Retrieves the item as an Optional, empty if the item is null
     * }
     * </pre>
     *
     * @param context the context, cannot be {@code null}
     * @return the object to configure the retrieval.
     */
    @CheckReturnValue
    default UniAwait<T> awaitUsing(Context context) {
        throw new UnsupportedOperationException("Default method added to limit binary incompatibility");
    }

    /**
     * Configures the action to execute when the observed {@link Uni} emits the item (potentially {@code null}).
     *
     * <p>
     * Examples:
     * </p>
     *
     * <pre>
     * {@code
     * Uni<T> uni = ...;
     * uni.onItem().transform(x -> ...); // Transform the item into another item (~ map)
     * uni.onItem().transformToUni(x -> ...); // Transform the item into a Uni (~ flatMap)
     * }
     * </pre>
     *
     * @return the object to configure the action to execute when an item is emitted
     */
    @CheckReturnValue
    UniOnItem<T> onItem();

    /**
     * Configures the action to execute when the observed {@link Uni} sends a {@link UniSubscription}.
     * The downstream does not have a subscription yet. It will be passed once the configured action completes.
     *
     * <p>
     * Example:
     * </p>
     *
     * <pre>
     * {@code
     * uni.onSubscription().invoke(sub -> System.out.println("subscribed"));
     * // Delay the subscription by 1 second (or until an asynchronous action completes)
     * uni.onSubscription().call(sub -> Uni.createFrom(1).onItem().delayIt().by(Duration.ofSecond(1)));
     * }
     * </pre>
     *
     * @return the object to configure the action to execution on subscription.
     */
    @CheckReturnValue
    UniOnSubscribe<T> onSubscription();

    /**
     * Configures the action to execute when the observed {@link Uni} emits either an item (potentially {@code null}))
     * or a failure. Unlike {@link #onItem()} and {@link #onFailure()} the action would handle both cases in on "go".
     *
     * @return the object to configure the action to execute when an item is emitted or when a failure is propagated.
     */
    @CheckReturnValue
    UniOnItemOrFailure<T> onItemOrFailure();

    /**
     * Like {@link #onFailure(Predicate)} but applied to all failures fired by the upstream uni.
     * It allows configuring the on failure behavior (recovery, retry...).
     *
     * @return a UniOnFailure on which you can specify the on failure action
     */
    @CheckReturnValue
    UniOnFailure<T> onFailure();

    /**
     * Configures a predicate filtering the failures on which the behavior (specified with the returned
     * {@link UniOnFailure}) is applied.
     * <p>
     * For instance, to only when an {@code IOException} is fired as failure you can use:
     * <code>uni.onFailure(IOException.class).recoverWithItem("hello")</code>
     * <p>
     * The fallback value ({@code hello}) will only be used if the upstream uni fire a failure of type
     * {@code IOException}.
     *
     * @param predicate the predicate, {@code null} means applied to all failures
     * @return a UniOnFailure configured with the given predicate on which you can specify the on failure action
     */
    @CheckReturnValue
    UniOnFailure<T> onFailure(Predicate<? super Throwable> predicate);

    /**
     * Configures a type of failure filtering the failures on which the behavior (specified with the returned
     * {@link UniOnFailure}) is applied.
     * <p>
     * For instance, to only when an {@code IOException} is fired as failure you can use:
     * <code>uni.onFailure(IOException.class).recoverWithItem("hello")</code>
     * <p>
     * The fallback value ({@code hello}) will only be used if the upstream uni fire a failure of type
     * {@code IOException}.
     *
     * @param typeOfFailure the class of exception, must not be {@code null}
     * @return a UniOnFailure configured with the given predicate on which you can specify the on failure action
     */
    @CheckReturnValue
    UniOnFailure<T> onFailure(Class<? extends Throwable> typeOfFailure);

    /**
     * Produces a {@link Uni} reacting when a no item event is fired by the upstream uni during the specified time
     * period.
     * <p>
     * This {@link Uni} detects if this {@link Uni} does not emit an item before the configured timeout.
     * <p>
     * Examples:
     *
     * <pre>{@code
     * uni.ifNoItem().after(Duration.ofMillis(1000)).fail() // Propagate a TimeOutException
     * uni.ifNoItem().after(Duration.ofMillis(1000)).recoverWithValue("fallback") // Inject a fallback item on timeout
     * uni.ifNoItem().after(Duration.ofMillis(1000)).on(myExecutor)... // Configure the executor calling on timeout actions
     * uni.ifNoItem().after(Duration.ofMillis(1000)).fail().onFailure().retry().atMost(5) // Retry five times
     * }</pre>
     *
     * @return the on timeout group
     */
    @CheckReturnValue
    UniIfNoItem<T> ifNoItem();

    /**
     * Produces a new {@link Uni} invoking the {@link UniSubscriber#onItem(Object)} and
     * {@link UniSubscriber#onFailure(Throwable)} on the supplied {@link Executor}.
     * <p>
     * Instead of receiving the {@code item} event on the thread firing the event, this method influences the
     * threading context to switch to a thread from the given executor.
     * <p>
     * <strong>Be careful as this operator can lead to concurrency problems with non thread-safe objects such as
     * CDI request-scoped beans.</strong>
     *
     * @param executor the executor to use, must not be {@code null}
     * @return a new {@link Uni}
     */
    @CheckReturnValue
    Uni<T> emitOn(Executor executor);

    /**
     * When a subscriber subscribes to this {@link Uni}, executes the subscription to the upstream {@link Uni} on a thread
     * from the given executor. As a result, the {@link UniSubscriber#onSubscribe(UniSubscription)} method will be called
     * on this thread (except mentioned otherwise)
     *
     * @param executor the executor to use, must not be {@code null}
     * @return a new {@link Uni}
     */
    @CheckReturnValue
    Uni<T> runSubscriptionOn(Executor executor);

    /**
     * Configure memoization of the {@link Uni} item or failure.
     *
     * @return the object to configure memoization
     * @apiNote This is an experimental API
     */
    @CheckReturnValue
    UniMemoize<T> memoize();

    /**
     * Transforms the item (potentially null) emitted by this {@link Uni} by applying a (synchronous) function to it.
     * This method is equivalent to {@code uni.onItem().transform(x -> ...)}
     * For asynchronous composition, look at flatMap.
     *
     * @param mapper the mapper function, must not be {@code null}
     * @param <O> the output type
     * @return a new {@link Uni} computing an item of type {@code <O>}.
     */
    @CheckReturnValue
    default <O> Uni<O> map(Function<? super T, ? extends O> mapper) {
        return onItem().transform(nonNull(mapper, "mapper"));
    }

    /**
     * Produces a new {@link Uni} invoking the given callback when the {@code item} event is fired. Note that the
     * item can be {@code null}.
     * <p>
     * If the callback throws an exception, this exception is propagated to the downstream as failure.
     * <p>
     * This method is a shortcut on {@link UniOnItem#invoke(Consumer)}
     *
     * @param callback the callback, must not be {@code null}
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<T> invoke(Consumer<? super T> callback) {
        Consumer<? super T> actual = nonNull(callback, "callback");
        return onItem().invoke(actual);
    }

    /**
     * Produces a new {@link Uni} invoking the given callback when the {@code item} event is fired, but ignoring it.
     * <p>
     * If the callback throws an exception, this exception is propagated to the downstream as failure.
     * <p>
     * This method is a shortcut on {@link UniOnItem#invoke(Consumer)}
     *
     * @param callback the callback, must not be {@code null}
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<T> invoke(Runnable callback) {
        Runnable actual = nonNull(callback, "callback");
        return onItem().invoke(actual);
    }

    /**
     * Produces a new {@link Uni} invoking the given @{code action} when the {@code item} event is received. Note that
     * the received item can be {@code null}.
     * <p>
     * Unlike {@link #invoke(Consumer)}, the passed function returns a {@link Uni}. When the produced {@code Uni} sends
     * its item, this item is discarded, and the original {@code item} is forwarded downstream. If the produced
     * {@code Uni} fails, the failure is propagated downstream. If the callback throws an exception, this exception
     * is propagated downstream as failure.
     * <p>
     * This method is a shortcut on {@link UniOnItem#call(Function)}
     *
     * @param function the function taking the item and returning a {@link Uni}, must not be {@code null}, must not return
     *        {@code null}
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<T> call(Function<? super T, Uni<?>> function) {
        return onItem().call(function);
    }

    /**
     * Produces a new {@link Uni} invoking the given @{code action} when the {@code item} event is received, but
     * ignoring it.
     * <p>
     * Unlike {@link #invoke(Consumer)}, the passed function returns a {@link Uni}. When the produced {@code Uni} sends
     * its item, this item is discarded, and the original {@code item} is forwarded downstream. If the produced
     * {@code Uni} fails, the failure is propagated downstream. If the callback throws an exception, this exception
     * is propagated downstream as failure.
     * <p>
     * This method is a shortcut on {@link UniOnItem#call(Function)}
     *
     * @param supplier the supplier taking the item and returning a {@link Uni}, must not be {@code null}, must not return
     *        {@code null}
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<T> call(Supplier<Uni<?>> supplier) {
        return onItem().call(supplier);
    }

    /**
     * Transforms the received item asynchronously, forwarding the events emitted by another {@link Uni} produced by
     * the given {@code mapper}.
     * <p>
     * The mapper is called with the item event of the current {@link Uni} and produces an {@link Uni}, possibly
     * using another type of item ({@code R}). The events fired by produced {@link Uni} are forwarded to the
     * {@link Uni} returned by this method.
     * <p>
     * This operation is generally named {@code flatMap}.
     * This method is a shortcut on {@link UniOnItem#transformToUni(Function)} {@code onItem().transformToUni(mapper)}.
     *
     * @param mapper the function called with the item of this {@link Uni} and producing the {@link Uni},
     *        must not be {@code null}, must not return {@code null}.
     * @param <O> the type of item
     * @return a new {@link Uni} that would fire events from the uni produced by the mapper function, possibly
     *         in an asynchronous manner.
     */
    @CheckReturnValue
    default <O> Uni<O> flatMap(Function<? super T, Uni<? extends O>> mapper) {
        return onItem().transformToUni(nonNull(mapper, "mapper"));
    }

    /**
     * Once the observed {@code Uni} emits an item, execute the given {@code mapper}. This mapper produces another
     * {@code Uni}. The downstream receives the events emitted by this produced {@code Uni}.
     * <p>
     * This operation allows <em>chaining</em> asynchronous operations: when the upstream completes with an item, run
     * the mapper and emits the item (or failure) sent by the produced {@code Uni}:
     *
     * <pre>
     * Uni&lt;Session&gt; uni = getSomeSession();
     * return uni.chain(session -&gt; session.persist(fruit))
     *         .chain(session -&gt; session.flush())
     *         .map(x -&gt; Response.ok(fruit).status(201).build());
     * </pre>
     * <p>
     * The mapper is called with the item event of the current {@link Uni} and produces an {@link Uni}, possibly
     * using another type of item ({@code O}). The events fired by produced {@link Uni} are forwarded to the
     * {@link Uni} returned by this method.
     * <p>
     * This operation is generally named {@code flatMap}.
     * This method is a shortcut for {@link UniOnItem#transformToUni(Function)} {@code onItem().transformToUni(mapper)}.
     *
     * @param mapper the function called with the item of this {@link Uni} and producing the {@link Uni},
     *        must not be {@code null}, must not return {@code null}.
     * @param <O> the type of item
     * @return a new {@link Uni} that would fire events from the uni produced by the mapper function, possibly
     *         in an asynchronous manner.
     * @see #chain(Supplier)
     */
    @CheckReturnValue
    default <O> Uni<O> chain(Function<? super T, Uni<? extends O>> mapper) {
        Function<? super T, Uni<? extends O>> actual = nonNull(mapper, "mapper");
        return onItem().transformToUni(actual);
    }

    /**
     * Once the observed {@code Uni} emits an item, execute the given {@code supplier}. This supplier produces another
     * {@code Uni}. The downstream receives the events emitted by this produced {@code Uni}.
     * <p>
     * This operation allows <em>chaining</em> asynchronous operations: when the upstream completes with an item, run
     * the supplier and emits the item (or failure) sent by the produced {@code Uni}:
     *
     * <pre>
     * Uni&lt;Session&gt; uni = getSomeSession();
     * return uni.chain(session -&gt; session.persist(fruit))
     *         .chain(session -&gt; session.flush())
     *         .chain(() -&gt; server.close());
     * </pre>
     * <p>
     * The supplier ignores the item event of the current {@link Uni} and produces an {@link Uni}, possibly
     * using another type of item ({@code O}). The events fired by produced {@link Uni} are forwarded to the
     * {@link Uni} returned by this method.
     * <p>
     * This method is a shortcut for {@link UniOnItem#transformToUni(Function)}
     * {@code onItem().transformToUni(ignored -> supplier.get())}.
     *
     * @param supplier the supplier producing the {@link Uni}, must not be {@code null}, must not return {@code null}.
     * @param <O> the type of item
     * @return a new {@link Uni} that would fire events from the uni produced by the mapper function, possibly
     *         in an asynchronous manner.
     * @see #chain(Supplier)
     */
    @CheckReturnValue
    default <O> Uni<O> chain(Supplier<Uni<? extends O>> supplier) {
        Supplier<Uni<? extends O>> actual = nonNull(supplier, "supplier");
        return onItem().transformToUni(ignored -> actual.get());
    }

    /**
     * Execute an action after an item, a failure or a cancellation has been received.
     * This is semantically equivalent to a {@code finally} block in Java.
     *
     * <pre>
     * {@code
     * String id = ...;
     * Session session = getSomeSession();
     * session.find(Fruit.class, id)
     *        .chain(fruit -> session.remove(fruit)
     *        .chain(ignored -> session.flush())
     *        .eventually(() -> session.close());
     * }
     * </pre>
     * <p>
     * This method is a shortcut for {@link UniOnTerminate#invoke(Runnable)}:
     * {@code onTermination().invoke(action)}
     *
     * @param action an action to perform, must not be {@code null}.
     * @return a new {@link Uni} that emits events once the action has completed.
     * @see #onItemOrFailure()
     */
    @CheckReturnValue
    default Uni<T> eventually(Runnable action) {
        return onTermination().invoke(nonNull(action, "action"));
    }

    /**
     * When this {@link Uni} emits an item, a failure or is being cancelled, invoke a {@link Uni} supplier then invoke the
     * supplied {@link Uni}.
     * When the supplied {@link Uni} emits an item then it is ignored, and when it emits a failure it is reported.
     * <p>
     * This is equivalent to a {@code finally} block in Java.
     *
     * <pre>
     * {@code
     * String id = ...;
     * Session session = getSomeSession();
     * session.find(Fruit.class, id)
     *        .chain(fruit -> session.remove(fruit)
     *        .eventually(() -> session.close());
     * }
     * </pre>
     * <p>
     * This method is a shortcut for {@link UniOnTerminate#call(Functions.Function3)}:
     * {@code onTermination().call((item, err, cancelled) -> actual.get())}
     *
     * @param supplier a {@link Uni} supplier, cannot be {@code null} and cannot return {@code null}.
     * @param <O> the type of the item
     * @return a new {@link Uni} that emits events once the supplied {@link Uni} emits an item or a failure.
     * @see #onItemOrFailure()
     */
    @CheckReturnValue
    default <O> Uni<T> eventually(Supplier<Uni<? extends O>> supplier) {
        Supplier<Uni<? extends O>> actual = nonNull(supplier, "supplier");
        return onTermination().call((item, err, cancelled) -> actual.get());
    }

    /**
     * Converts an {@link Uni} to other types such as {@link CompletionStage}
     *
     * <p>
     * Examples:
     * </p>
     *
     * <pre>
     * {@code
     * uni.convert().toCompletionStage(); // Convert to CompletionStage using convenience method
     * uni.convert().with(BuiltinConverters.toCompletionStage()); // Convert to CompletionStage using BuiltInConverters
     * uni.convert().with(uni -> x); // Convert with a custom lambda converter
     * }
     * </pre>
     *
     * @return the object to convert an {@link Uni} instance
     * @see UniConvert
     */
    @CheckReturnValue
    UniConvert<T> convert();

    /**
     * Creates an instance of {@link Multi} from this {@link Uni}.
     * <p>
     * When a subscriber subscribes to the returned {@link Multi} and <strong>request</strong> an item, it subscribes
     * to this {@link Uni} and the events from this {@link Uni} are propagated to the {@link Multi}:
     * <ul>
     * <li>if this {@link Uni} emits a non-{@code null} item - this item is propagated to the {@link Multi}
     * and followed with the completion event</li>
     * <li>if this {@link Uni} emits a {@code null} item - the {@link Multi} fires the completion event</li>
     * <li>if this {@link Uni} emits a failure, this failure event is propagated by the {@link Multi}</li>
     * </ul>
     * <p>
     * It's important to note that the subscription to this {@link Uni} happens when the subscriber to the produced
     * {@link Multi} <strong>requests</strong> items, and not at subscription time.
     *
     * @return the produced {@link Multi}, never {@code null}
     */
    @CheckReturnValue
    Multi<T> toMulti();

    /**
     * Allows configuring repeating behavior.
     * Repeating allow transforming a {@link Uni} into a {@link Multi} either a specific amount of times or indefinitely.
     * Each time, a new subscription is attempted on the {@link Uni}.
     * Cancelling the subscription stops the repeating behavior.
     *
     * @return the object to configure the repeating behavior.
     */
    @CheckReturnValue
    UniRepeat<T> repeat();

    /**
     * Configures actions to be performed on termination, that is, on item, on failure, or when the subscriber cancels
     * the subscription.
     *
     * @return the object to configure the termination actions.
     */
    @CheckReturnValue
    UniOnTerminate<T> onTermination();

    /**
     * Configures actions to be performed when the subscriber cancels the subscription.
     *
     * @return the object to configure the cancellation actions.
     */
    @CheckReturnValue
    UniOnCancel<T> onCancellation();

    /**
     * Plug a user-defined operator that does not belong to the existing Mutiny API.
     *
     * @param operatorProvider a function to create and bind a new operator instance, taking {@code this} {@link Uni} as a
     *        parameter and returning a new {@link Uni}. Must neither be {@code null} nor return {@code null}.
     * @param <R> the output type
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default <R> Uni<R> plug(Function<Uni<T>, Uni<R>> operatorProvider) {
        Function<Uni<T>, Uni<R>> provider = nonNull(operatorProvider, "operatorProvider");
        return Infrastructure.onUniCreation(nonNull(provider.apply(this), "uni"));
    }

    /**
     * Ignore the item emitted by this {@link Uni} and replace it with another value.
     * <p>
     * This is a shortcut for {@code uni.onItem().transform(ignore -> item)}.
     *
     * @param item the replacement value
     * @param <O> the output type
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default <O> Uni<O> replaceWith(O item) {
        return onItem().transform(ignore -> item);
    }

    /**
     * Ignore the item emitted by this {@link Uni} and replace it with another value using a {@link Supplier}.
     * <p>
     * This is a shortcut for {@code uni.onItem().transform(ignore -> supplier.get())}.
     *
     * @param supplier the replacement value supplier
     * @param <O> the output type
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default <O> Uni<O> replaceWith(Supplier<O> supplier) {
        return onItem().transform(ignore -> supplier.get());
    }

    /**
     * Ignore the item emitted by this {@link Uni} and replace it with another value using a {@link Uni}.
     * <p>
     * This is a shortcut for {@code uni.onItem().transformToUni(ignore -> uni)}.
     *
     * @param uni the replacement value {@link Uni}
     * @param <O> the output type
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default <O> Uni<O> replaceWith(Uni<O> uni) {
        return onItem().transformToUni(ignore -> uni);
    }

    /**
     * Ignore the item emitted by this {@link Uni} and replace it with {@code null}.
     * <p>
     * This is a shortcut for {@code uni.onItem().transform(ignore -> null)}.
     *
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<T> replaceWithNull() {
        return onItem().transform(ignore -> null);
    }

    /**
     * Ignore the item emitted by this {@link Uni} and replace it with {@code null} and type {@link Void}.
     * <p>
     * This method is in effect similar to {@link Uni#replaceWithNull()}, except that it returns a {@code Uni<Void>}
     * instead of a {@code Uni<T>}.
     * <p>
     * This is a shortcut for {@code uni.onItem().transform(ignored -> null)}.
     *
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<Void> replaceWithVoid() {
        return onItem().transform(ignored -> null);
    }

    /**
     * When this {@link Uni} emits {@code null}, replace with the value provided by the given {@link Supplier}.
     * <p>
     * This is a shortcut for {@code uni.onItem().ifNull().continueWith(supplier)}
     *
     * @param supplier the supplier
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<T> replaceIfNullWith(Supplier<T> supplier) {
        return onItem().ifNull().continueWith(supplier);
    }

    /**
     * When this {@link Uni} emits {@code null}, replace with the provided value.
     * <p>
     * This is a shortcut for {@code uni.onItem().ifNull().continueWith(value)}
     *
     * @param value the value
     * @return the new {@link Uni}
     */
    @CheckReturnValue
    default Uni<T> replaceIfNullWith(T value) {
        return onItem().ifNull().continueWith(value);
    }

    /**
     * Log events (onSubscribe, onItem, ...) as they come from the upstream or the subscriber.
     * <p>
     * Events will be logged as long as the {@link Uni} hasn't been cancelled or terminated.
     * Logging is framework-agnostic and can be configured in the {@link Infrastructure} class.
     *
     * @param identifier an identifier of this operator to be used in log events
     * @return a new {@link Uni}
     * @see Infrastructure#setOperatorLogger(Infrastructure.OperatorLogger)
     */
    @CheckReturnValue
    Uni<T> log(String identifier);

    /**
     * Log events (onSubscribe, onItem, ...) as they come from the upstream or the subscriber, and derives the identifier from
     * the upstream operator class "simple name".
     * <p>
     * Events will be logged as long as the {@link Uni} hasn't been cancelled or terminated.
     * Logging is framework-agnostic and can be configured in the {@link Infrastructure} class.
     *
     * @return a new {@link Uni}
     * @see Uni#log(String)
     * @see Infrastructure#setOperatorLogger(Infrastructure.OperatorLogger)
     */
    @CheckReturnValue
    Uni<T> log();

    /**
     * Join the results from multiple {@link Uni} (e.g., collect all values, pick the first to respond, etc).
     * <p>
     * Here is an example where several {@link Uni} are joined, and result in a {@code Uni<List<Number>>}:
     *
     * <pre>
     * Uni&lt;Number&gt; a = Uni.createFrom().item(1);
     * Uni&lt;Number&gt; b = Uni.createFrom().item(2L);
     * Uni&lt;Number&gt; c = Uni.createFrom().item(3);
     *
     * Uni&lt;List&lt;Number&gt;&gt; uni = Uni.join().all(a, b, c).andCollectFailures();
     * </pre>
     * <p>
     * The list of {@code Unis} must not be empty, as in that case, no event will be sent.
     *
     * @return the object to configure the join behavior.
     */
    @CheckReturnValue
    static UniJoin join() {
        return UniJoin.SHARED_INSTANCE;
    }

    /**
     * Materialize the subscriber {@link Context} for a sub-pipeline.
     *
     * <p>
     * The provided function takes this {@link Uni} and the {@link Context} as parameters, and returns a {@link Uni}
     * to build the sub-pipeline, as in:
     *
     * <pre>
     * {@code
     * someUni.withContext((uni, ctx) -> uni.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")));
     * }
     * </pre>
     *
     * <p>
     * Note that the {@code builder} function is called <strong>at subscription time</strong>, so it cannot see
     * context updates from upstream operators yet.
     *
     * @param builder the function that builds the sub-pipeline from this {@link Uni} and the {@link Context},
     *        must not be {@code null}, must not return {@code null}.
     * @param <R> the resulting {@link Uni} type
     * @return the resulting {@link Uni}
     */
    @CheckReturnValue
    default <R> Uni<R> withContext(BiFunction<Uni<T>, Context, Uni<R>> builder) {
        throw new UnsupportedOperationException("Default method added to limit binary incompatibility");
    }

    /**
     * Materialize the context by attaching it to items using the {@link ItemWithContext} wrapper class.
     *
     * <p>
     * This is a shortcut for:
     *
     * <pre>
     * {@code
     * someUni.withContext((uni, ctx) -> uni.onItem().transform(item -> new ItemWithContext<>(ctx, item)));
     * }
     * </pre>
     *
     * @return the resulting {@link Uni}
     * @see #withContext(BiFunction)
     */
    @CheckReturnValue
    default Uni<ItemWithContext<T>> attachContext() {
        return this.withContext((uni, ctx) -> uni.onItem().transform(item -> new ItemWithContext<>(ctx, item)));
    }
}
