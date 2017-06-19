package nakadi;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamProcessorObserverBuilder {

  <T> Flowable<StreamBatchRecord<T>> createStreamObservable(
      StreamObserver<T> streamObserver,
      Callable<Response> resourceSupplier,
      Function<? super Response, Flowable<StreamBatchRecord<T>>> sourceSupplier,
      Consumer<? super Response> resourceDisposer,
      StreamConnectionRetryFlowable retryFlowable,
      FlowableTransformer<StreamBatchRecord<T>, StreamBatchRecord<T>> composer,
      Scheduler scheduler,
      int backpressureBufferSize,
      StreamProcessorHalfOpenKick kick
  ) {

    final Flowable<StreamBatchRecord<T>> flowable = Flowable.using(
        resourceSupplier,
        sourceSupplier,
        resourceDisposer
    )
        .subscribeOn(scheduler)
        .unsubscribeOn(scheduler)
        .doOnSubscribe(subscription -> streamObserver.onStart())
        .doOnComplete(streamObserver::onCompleted)
        .doOnCancel(streamObserver::onStop)
        .doOnError(streamObserver::onError)
        .timeout(kick.halfOpenKick(), kick.halfOpenUnit())
        // retries handle issues like network failures and 409 conflicts
        .retryWhen(retryFlowable)
        // restarts handle when the server closes the connection (eg checkpointing fell behind)
        .compose(composer)
        /*
         todo: investigate why Integer.max causes
        io.reactivex.exceptions.UndeliverableException: java.lang.NegativeArraySizeException
         at io.reactivex.plugins.RxJavaPlugins.onError(RxJavaPlugins.java:366)
         */
        .onBackpressureBuffer(backpressureBufferSize, true, true);

    return Flowable.defer(() -> flowable);
  }

}
