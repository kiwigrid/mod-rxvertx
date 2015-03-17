package io.vertx.rxcore.java.filesystem;

import io.vertx.rxcore.RxSupport;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.file.AsyncFile;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;

/**
 * @author Andreas Berger <andreas.berger@kiwigrid.com>
 */
public class RxFile {
	private AsyncFile asyncFile;

	public RxFile(AsyncFile asyncFile) {
		this.asyncFile = asyncFile;
	}

	/**
	 * @param data the data to write
	 * @return an observable with the total bytes written per chunk
	 */
	public Observable<Long> write(Observable<Buffer> data) {
		return RxSupport
				.stream(data, asyncFile)
				.doOnError(new Action1<Throwable>() {
					@Override
					public void call(Throwable throwable) {
						asyncFile.close();
					}
				})
				.doOnCompleted(new Action0() {
					@Override
					public void call() {
						asyncFile.close();
					}
				});
	}

	public Observable<Buffer> read() {
		return RxSupport
				.toObservable(asyncFile)
				.doOnCompleted(new Action0() {
					@Override
					public void call() {
						asyncFile.close();
					}
				});
	}
}
