package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.EmptyUniSubscription.propagateFailureEvent;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.smallrye.mutiny.helpers.ParameterValidation;

public class UniCreateFromCompletionStage<O> extends UniOperator<Void, O> {
    private final Supplier<? extends CompletionStage<? extends O>> supplier;

    public UniCreateFromCompletionStage(Supplier<? extends CompletionStage<? extends O>> supplier) {
        super(null);
        this.supplier = ParameterValidation.nonNull(supplier, "supplier");
    }

    private static <O> void forwardFromCompletionStage(CompletionStage<? extends O> stage,
            UniSerializedSubscriber<? super O> subscriber) {
        subscriber.onSubscribe(() -> stage.toCompletableFuture().cancel(false));
        stage.whenComplete((res, fail) -> {
            if (fail != null) {
                if (fail instanceof CompletionException) {
                    subscriber.onFailure(fail.getCause());
                } else {
                    subscriber.onFailure(fail);
                }
            } else {
                subscriber.onItem(res);
            }
        });
    }

    @Override
    protected void subscribing(UniSerializedSubscriber<? super O> subscriber) {
        CompletionStage<? extends O> stage;
        try {
            stage = supplier.get();
        } catch (Throwable e) {
            propagateFailureEvent(subscriber, e);
            return;
        }

        if (stage == null) {
            propagateFailureEvent(subscriber, new NullPointerException("The produced completion stage is `null`"));
            return;
        }

        forwardFromCompletionStage(stage, subscriber);
    }
}
