package com.autonomouslogic.commons.rxjava3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableTransformer;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Rx3UtilTest {
	private static final ObservableTransformer<String, String> IDENTITY_OBSERVABLE_TRANSFORMER = upstream -> upstream;
	private static final FlowableTransformer<String, String> IDENTITY_FLOWABLE_TRANSFORMER = upstream -> upstream;

	RuntimeException textEx = new RuntimeException("test error");

	static final AtomicReference<Throwable> caughtError = new AtomicReference<>();

	@BeforeAll
	static void beforeAll() {
		RxJavaPlugins.setErrorHandler(caughtError::set);
	}

	@BeforeEach
	void setup() {
		caughtError.set(null);
	}

	//////////////
	// toSingle()

	@Test
	void shouldConvertCompletionStageToSingle() {
		var future = CompletableFuture.completedStage("test");
		var result = Rx3Util.toSingle(future).blockingGet();
		assertEquals("test", result);
	}

	@Test
	void confirmCatchSingleFromFuture() {
		// This test confirms how RxJava's fromFuture works, so it can be replicated below
		var future = CompletableFuture.failedFuture(textEx);
		var single = Single.fromFuture(future);
		var ex = assertThrows(RuntimeException.class, single::blockingGet);
		ex.printStackTrace();
		assertEquals(
				"java.util.concurrent.ExecutionException: java.lang.RuntimeException: test error", ex.getMessage());

		assertNull(caughtError.get());
	}

	@Test
	void shouldCatchCompletionStageErrorsToSingle() {
		var future = CompletableFuture.failedFuture(textEx);
		var single = Rx3Util.toSingle(future);
		var ex = assertThrows(RuntimeException.class, single::blockingGet);
		ex.printStackTrace();
		assertEquals(
				"java.util.concurrent.ExecutionException: java.lang.RuntimeException: test error", ex.getMessage());

		assertNull(caughtError.get());
	}

	@Test
	@SneakyThrows
	void shouldCancelSingle() {
		var future = delayedErrorFuture();
		var single = Rx3Util.toSingle(future);
		var disposable = single.subscribe();
		disposable.dispose();
		assertTrue(disposable.isDisposed());
		assertTrue(future.isCancelled());
		Thread.sleep(200);
		assertNull(caughtError.get());
	}

	//////////////
	// toMaybe()

	@Test
	void shouldConvertCompletionStageToMaybe() {
		var future = CompletableFuture.completedStage("test");
		var result = Rx3Util.toMaybe(future).blockingGet();
		assertEquals("test", result);
	}

	@Test
	void shouldConvertCompletionStageNullResultToMaybe() {
		var future = CompletableFuture.completedStage(null);
		var result = Rx3Util.toMaybe(future).defaultIfEmpty("empty").blockingGet();
		assertEquals("empty", result);
	}

	@Test
	void confirmCatchMaybeFromFuture() {
		// This test confirms how RxJava's fromFuture works, so it can be replicated below
		var future = CompletableFuture.failedFuture(textEx);
		var maybe = Maybe.fromFuture(future);
		var ex = assertThrows(RuntimeException.class, maybe::blockingGet);
		ex.printStackTrace();
		assertEquals("test error", ex.getMessage());

		assertNull(caughtError.get());
	}

	@Test
	void shouldCatchCompletionStageErrorsToMaybe() {
		var future = CompletableFuture.failedFuture(textEx);
		var maybe = Rx3Util.toMaybe(future);
		var ex = assertThrows(RuntimeException.class, maybe::blockingGet);
		ex.printStackTrace();
		assertEquals(
				"java.util.concurrent.ExecutionException: java.lang.RuntimeException: test error", ex.getMessage());

		assertNull(caughtError.get());
	}

	@Test
	@SneakyThrows
	void shouldCancelMaybe() {
		var future = delayedErrorFuture();
		var maybe = Rx3Util.toMaybe(future);
		var disposable = maybe.subscribe();
		disposable.dispose();
		assertTrue(disposable.isDisposed());
		assertTrue(future.isCancelled());
		Thread.sleep(200);
		assertNull(caughtError.get());
	}

	//////////////
	// toCompletable()

	@Test
	void shouldConvertCompletionStageToCompletable() {
		var future = CompletableFuture.completedStage((Void) null);
		AtomicBoolean complete = new AtomicBoolean();
		Rx3Util.toCompletable(future).doOnComplete(() -> complete.set(true)).blockingAwait();
		assertTrue(complete.get());
	}

	@Test
	void confirmCatchCompletableFromFuture() {
		// This test confirms how RxJava's fromFuture works, so it can be replicated below
		var future = CompletableFuture.runAsync(() -> {
			throw textEx;
		});
		var completable = Completable.fromFuture(future);
		var ex = assertThrows(RuntimeException.class, completable::blockingAwait);
		ex.printStackTrace();
		assertEquals(
				"java.util.concurrent.ExecutionException: java.lang.RuntimeException: test error", ex.getMessage());

		assertNull(caughtError.get());
	}

	@Test
	void shouldCatchCompletionStageErrorsToCompletable() {
		var future = CompletableFuture.runAsync(() -> {
			throw textEx;
		});
		var completable = Rx3Util.toCompletable(future);
		var ex = assertThrows(RuntimeException.class, completable::blockingAwait);
		ex.printStackTrace();
		assertEquals(
				"java.util.concurrent.ExecutionException: java.util.concurrent.CompletionException: java.lang.RuntimeException: test error",
				ex.getMessage());

		assertNull(caughtError.get());
	}

	@Test
	@SneakyThrows
	void shouldCancelCompletable() {
		var future = delayedErrorFuture();
		var completable = Rx3Util.toCompletable(future);
		var disposable = completable.subscribe();
		disposable.dispose();
		assertTrue(disposable.isDisposed());
		assertTrue(future.isCancelled());
		Thread.sleep(200);
		assertNull(caughtError.get());
	}

	//////////////
	// wrapTransformerErrors()

	@Test
	void shouldWrapObservableTransformerErrors() {
		var observable = Observable.just("result")
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", (ObservableTransformer<String, String>)
						upstream -> Observable.error(new RuntimeException("inner error"))));
		var ex = assertThrows(RuntimeException.class, observable::blockingFirst);
		ex.printStackTrace();
		assertEquals("wrapped error", ex.getMessage());
		assertEquals("inner error", ex.getCause().getMessage());
	}

	@Test
	void shouldNotAffectErrorsBeforeWrappingObservableTransformer() {
		Observable<String> observable = Observable.just("result")
				.observeOn(Schedulers.computation())
				.compose((ObservableTransformer<String, String>)
						upstream -> Observable.error(new RuntimeException("before error")))
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", IDENTITY_OBSERVABLE_TRANSFORMER));
		var ex = assertThrows(RuntimeException.class, observable::blockingFirst);
		ex.printStackTrace();
		assertEquals("before error", ex.getMessage());
		assertNull(ex.getCause());
	}

	@Test
	void shouldNotAffectErrorsAfterWrappingObservableTransformer() {
		Observable<String> observable = Observable.just("result")
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", IDENTITY_OBSERVABLE_TRANSFORMER))
				.observeOn(Schedulers.computation())
				.compose(upstream -> Observable.error(new RuntimeException("after error")));
		var ex = assertThrows(RuntimeException.class, observable::blockingFirst);
		ex.printStackTrace();
		assertEquals("after error", ex.getMessage());
		assertNull(ex.getCause());
	}

	@Test
	void shouldNotBlockResultsWhenWrappingObservableTransformer() {
		Observable<String> observable = Observable.just("result")
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", IDENTITY_OBSERVABLE_TRANSFORMER));
		var result = observable.blockingFirst();
		assertEquals("result", result);
	}

	@Test
	void shouldWrapFlowableTransformerErrors() {
		var flowable = Flowable.just("result")
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", (FlowableTransformer<? super String, String>)
						upstream -> Flowable.error(new RuntimeException("inner error"))));
		var ex = assertThrows(RuntimeException.class, flowable::blockingFirst);
		ex.printStackTrace();
		assertEquals("wrapped error", ex.getMessage());
		assertEquals("inner error", ex.getCause().getMessage());
	}

	@Test
	void shouldNotAffectErrorsBeforeWrappingFlowableTransformer() {
		Flowable<String> flowable = Flowable.just("result")
				.observeOn(Schedulers.computation())
				.compose((FlowableTransformer<String, String>)
						upstream -> Flowable.error(new RuntimeException("before error")))
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", IDENTITY_FLOWABLE_TRANSFORMER));
		var ex = assertThrows(RuntimeException.class, flowable::blockingFirst);
		ex.printStackTrace();
		assertEquals("before error", ex.getMessage());
		assertNull(ex.getCause());
	}

	@Test
	void shouldNotAffectErrorsAfterWrappingFlowableTransformer() {
		Flowable<String> flowable = Flowable.just("result")
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", IDENTITY_FLOWABLE_TRANSFORMER))
				.observeOn(Schedulers.computation())
				.compose(upstream -> Flowable.error(new RuntimeException("after error")));
		var ex = assertThrows(RuntimeException.class, flowable::blockingFirst);
		ex.printStackTrace();
		assertEquals("after error", ex.getMessage());
		assertNull(ex.getCause());
	}

	@Test
	void shouldNotBlockResultsWhenWrappingFlowableTransformer() {
		Flowable<String> flowable = Flowable.just("result")
				.observeOn(Schedulers.computation())
				.compose(Rx3Util.wrapTransformerErrors("wrapped error", IDENTITY_FLOWABLE_TRANSFORMER));
		var result = flowable.blockingFirst();
		assertEquals("result", result);
	}

	//////////////
	// Others

	@Test
	@SneakyThrows
	void shouldZipAll() {
		var sub = new TestSubscriber<Object[]>();
		Rx3Util.zipAllFlowable(v -> v, Flowable.just(1, 2), Flowable.just(3, 4, 5), Flowable.just(6, 7, 8, 9))
				.subscribe(sub);
		var values = sub.await()
				.assertNoErrors()
				.assertComplete()
				.assertValueCount(4)
				.values();
		// First row.
		assertEquals(Optional.of(1), values.get(0)[0]);
		assertEquals(Optional.of(3), values.get(0)[1]);
		assertEquals(Optional.of(6), values.get(0)[2]);
		assertEquals(3, values.get(0).length);
		// Second row.
		assertEquals(Optional.of(2), values.get(1)[0]);
		assertEquals(Optional.of(4), values.get(1)[1]);
		assertEquals(Optional.of(7), values.get(1)[2]);
		assertEquals(3, values.get(0).length);
		// Third row.
		assertEquals(Optional.empty(), values.get(2)[0]);
		assertEquals(Optional.of(5), values.get(2)[1]);
		assertEquals(Optional.of(8), values.get(2)[2]);
		assertEquals(3, values.get(0).length);
		// Fourth row.
		assertEquals(Optional.empty(), values.get(3)[0]);
		assertEquals(Optional.empty(), values.get(3)[1]);
		assertEquals(Optional.of(9), values.get(3)[2]);
		assertEquals(3, values.get(0).length);
	}

	@Test
	@SneakyThrows
	void shouldWindowSort() {
		var sub = new TestSubscriber<Integer>();
		Flowable.fromIterable(List.of(4, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 3, 2, 1))
				.compose(Rx3Util.windowSort(Integer::compareTo, 5))
				.subscribe(sub);
		sub.await().assertComplete().assertNoErrors();
		System.out.println(sub.values());
		sub.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 3, 4, 4);
	}

	@Test
	void shouldPassCheckOrderOnOrderedStuff() {
		var ints = List.of(1, 2, 3, 3, 4, 5, 6);
		var result = Flowable.fromIterable(ints)
				.compose(Rx3Util.checkOrder(Integer::compareTo))
				.toList()
				.blockingGet();
		assertEquals(ints, result);
	}

	@Test
	void shouldErrorCheckOrderOnUnorderedStuff() {
		var ints = List.of(1, 2, 3, 5, 4, 5, 6);
		var result = Flowable.fromIterable(ints)
				.compose(Rx3Util.checkOrder(Integer::compareTo))
				.toList();
		var ex = assertThrows(RuntimeException.class, () -> result.blockingGet());
		ex.printStackTrace();
		assertEquals("Stream isn't ordered - last: 5, current: 4", ex.getMessage());
	}

	@Test
	@SneakyThrows
	void shouldRetryWithDelay() {
		var count = new AtomicInteger();
		var times = new ArrayList<Instant>();
		var result = Flowable.defer(() -> {
					times.add(Instant.now());
					if (count.incrementAndGet() < 3) {
						return Flowable.error(new RuntimeException("test error"));
					}
					return Flowable.just("result");
				})
				.subscribeOn(Schedulers.computation())
				.compose(Rx3Util.retryWithDelayFlowable(2, Duration.ofSeconds(1)))
				.blockingFirst();
		assertEquals("result", result);
		assertEquals(3, count.get());
		assertEquals(3, times.size());
		for (int j = 0; j < 2; j++) {
			assertEquals(1000, Duration.between(times.get(j), times.get(j + 1)).toMillis(), 100);
		}
	}

	@Test
	@SneakyThrows
	void shouldRetryWithDelayPredicate() {
		var count = new AtomicInteger();
		var times = new ArrayList<Instant>();
		Supplier<?> func = () -> Flowable.defer(() -> {
					times.add(Instant.now());
					return Flowable.error(new RuntimeException("test error " + count.getAndIncrement()));
				})
				.subscribeOn(Schedulers.computation())
				.compose(Rx3Util.retryWithDelayFlowable(
						100, Duration.ofSeconds(1), e -> !e.getMessage().equals("test error 2")))
				.blockingFirst();
		var ex = assertThrows(RuntimeException.class, func::get);
		ex.printStackTrace();
		assertEquals("test error 2", ex.getMessage());
		assertEquals(3, count.get());
		assertEquals(3, times.size());
		for (int j = 0; j < 2; j++) {
			assertEquals(1000, Duration.between(times.get(j), times.get(j + 1)).toMillis(), 100);
		}
	}

	//////////////
	// Utils

	private static CompletableFuture<Void> delayedErrorFuture() {
		return CompletableFuture.runAsync(() -> {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			throw new RuntimeException("delayed error");
		});
	}
}
