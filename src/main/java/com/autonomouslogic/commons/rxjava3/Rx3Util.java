package com.autonomouslogic.commons.rxjava3;

import com.autonomouslogic.commons.rxjava3.internal.CheckOrder;
import com.autonomouslogic.commons.rxjava3.internal.ErrorWrapFlowableTransformer;
import com.autonomouslogic.commons.rxjava3.internal.ErrorWrapObservableTransformer;
import com.autonomouslogic.commons.rxjava3.internal.OrderedMerger;
import com.autonomouslogic.commons.rxjava3.internal.ZipAll;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.ObservableTransformer;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Predicate;
import java.time.Duration;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Publisher;

/**
 * Various helper methods for working with RxJava 3.
 */
public class Rx3Util {
	private Rx3Util() {}

	/**
	 * Converts a {@link CompletionStage} to a {@link Single}.
	 *
	 * Null return values will result in an error from RxJava, as those aren't allowed.
	 * Use {@link #toMaybe(CompletionStage)} instead to handle null values properly.
	 *
	 * {@link Single#fromFuture(Future)} works in a blocking fashion, whereas {@link CompletionStage} can be utilised to avoid blocking calls.
	 *
	 * @param future the completion stage
	 * @return the Single
	 * @param <T> the return parameter of the future
	 */
	public static <T> Single<T> toSingle(CompletionStage<T> future) {
		return Single.create(emitter -> {
			future.whenComplete((result, throwable) -> {
				if (!emitter.isDisposed()) {
					if (throwable != null) {
						emitter.onError(new ExecutionException(throwable));
					} else if (result != null) {
						emitter.onSuccess(result);
					} else {
						emitter.onError(new NullPointerException("CompletionStage completed with a null result"));
					}
				}
			});
			emitter.setCancellable(() -> {
				if (future instanceof CompletableFuture<?>) {
					((CompletableFuture<?>) future).cancel(false);
				}
			});
		});
	}

	/**
	 * Converts a {@link CompletionStage} to a {@link Maybe}.
	 *
	 * Null return values will result in an empty Maybe.
	 *
	 * {@link Maybe#fromFuture(Future)} works in a blocking fashion, whereas {@link CompletionStage} can be utilised to avoid blocking calls.
	 *
	 * @param future the completion stage
	 * @return the Maybe
	 * @param <T> the return parameter of the future
	 */
	public static <T> Maybe<T> toMaybe(CompletionStage<T> future) {
		return Maybe.create(emitter -> {
			future.whenComplete((result, throwable) -> {
				if (!emitter.isDisposed()) {
					if (throwable != null) {
						emitter.onError(new ExecutionException(throwable));
					} else if (result != null) {
						emitter.onSuccess(result);
					} else {
						emitter.onComplete();
					}
				}
			});
			emitter.setCancellable(() -> {
				if (future instanceof CompletableFuture<?>) {
					((CompletableFuture<?>) future).cancel(false);
				}
			});
		});
	}

	/**
	 * Converts a {@link CompletionStage} to a {@link Completable}.
	 *
	 * {@link Completable#fromFuture(Future)} works in a blocking fashion, whereas {@link CompletionStage} can be utilised to avoid blocking calls.
	 *
	 * @param future the completion stage
	 * @return the Completable
	 */
	public static Completable toCompletable(CompletionStage<Void> future) {
		return Completable.create(emitter -> {
			future.whenComplete((result, throwable) -> {
				if (!emitter.isDisposed()) {
					if (throwable != null) {
						// Emit only the cause message
						if (throwable.getCause() != null) {
							emitter.onError(new ExecutionException(throwable));
						} else {
							emitter.onError(throwable);
						}
					} else {
						emitter.onComplete();
					}
				}
			});

			emitter.setCancellable(() -> {
				if (future instanceof CompletableFuture<?>) {
					((CompletableFuture<?>) future).cancel(false);
				}
			});
		});
	}

	/**
	 * Wraps an {@link ObservableTransformer} such that any errors thrown from anything done by the
	 * <code>transformer</code> will be wrapped in a {@link RuntimeException} with the provided message.
	 * Any errors occurring before or after the <code>transformer</code> is applied are not subject to exception
	 * wrapping.
	 * This is useful when debugging multi-threaded asynchronous code where the relevant stack trace can get lost.
	 *
	 * @param message message for the exception
	 * @param transformer transformer used for composition
	 * @return a new transformer
	 * @param <U> see ObservableTransformer
	 * @param <D> see ObservableTransformer
	 */
	public static <U, D> ObservableTransformer<U, D> wrapTransformerErrors(
			String message, ObservableTransformer<U, D> transformer) {
		return new ErrorWrapObservableTransformer<>(message, transformer);
	}

	/**
	 * Wraps an {@link FlowableTransformer} such that any errors thrown from anything done by the
	 * <code>transformer</code> will be wrapped in a {@link RuntimeException} with the provided message.
	 * Any errors occurring before or after the <code>transformer</code> is applied are not subject to exception
	 * wrapping.
	 * This is useful when debugging multi-threaded asynchronous code where the relevant stack trace can get lost.
	 *
	 * @param message message for the exception
	 * @param transformer transformer used for composition
	 * @return a new transformer
	 * @param <U> see FlowableTransformer
	 * @param <D> see FlowableTransformer
	 */
	public static <U, D> FlowableTransformer<U, D> wrapTransformerErrors(
			String message, FlowableTransformer<U, D> transformer) {
		return new ErrorWrapFlowableTransformer<>(message, transformer);
	}

	/**
	 * Merges a number of sources together always picking the next item from the source which compares the lowest.
	 * In order to merge sources in a completely ordered way, it is assumed the sources are already themselves sorted.
	 * @param comparator
	 * @param sources
	 * @return the merged Publisher
	 * @param <T> the type of the Publisher to merge
	 */
	public static <T> Publisher<T> orderedMerge(Comparator<T> comparator, Publisher<T>... sources) {
		return new OrderedMerger<>(comparator, sources).createPublisher();
	}

	/**
	 * Like {@link Flowable#zipArray(Function, boolean, int, Publisher[])}, but keeps going until all the sources
	 * have ended. It does this by wrapping all the values in {@link Optional}s and replacing the ended sources with empty
	 * ones.
	 */
	public static <@NonNull T, @NonNull R> Flowable<R> zipAllFlowable(
			@NonNull Function<? super Object[], ? extends R> zipper,
			boolean delayError,
			int bufferSize,
			@NonNull Publisher<? extends T>... sources) {
		return new ZipAll<T, R>(zipper, delayError, bufferSize, sources).createFlowable();
	}

	public static <@NonNull T, @NonNull R> Flowable<R> zipAllFlowable(
			@NonNull Function<? super Object[], ? extends R> zipper, @NonNull Publisher<? extends T>... sources) {
		return zipAllFlowable(zipper, false, Flowable.bufferSize(), sources);
	}

	/**
	 * Sorts a stream within a sliding window.
	 * @param comparator the comparator
	 * @param minWindowSize the minimum window size. The actual sorting window will be larger.
	 */
	public static <@NonNull T> WindowSort<T> windowSort(Comparator<T> comparator, int minWindowSize) {
		return new WindowSort<>(comparator, minWindowSize);
	}

	/**
	 * Creates a transformer which will error if the stream isn't strictly ordered.
	 * @param comparator the comparator
	 */
	public static <@NonNull T> CheckOrder<T> checkOrder(Comparator<T> comparator) {
		return new CheckOrder<T>(comparator);
	}

	public static <T> FlowableTransformer<T, T> retryWithDelayFlowable(int times, Duration delay) {
		return retryWithDelayFlowable(times, delay, e -> true);
	}

	public static <T> FlowableTransformer<T, T> retryWithDelayFlowable(
			int times, Duration delay, Predicate<? super Throwable> predicate) {
		if (times < 0) {
			throw new IllegalArgumentException("times >= 0 required but it was " + times);
		}
		var delayNs = delay.toNanos();
		if (delayNs < 0) {
			throw new IllegalArgumentException("delay must be zero or more");
		}
		return upstream -> upstream.retryWhen(e -> {
			var t = new AtomicInteger();
			return e.flatMap(err -> {
				int i = t.incrementAndGet();
				if (i <= times && predicate.test(err)) {
					return Flowable.timer(delayNs, TimeUnit.NANOSECONDS);
				}
				return Flowable.error(err);
			});
		});
	}
}
