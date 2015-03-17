package io.vertx.rxcore.java.filesystem;

import io.vertx.rxcore.RxSupport;
import io.vertx.rxcore.java.impl.AsyncResultMemoizeHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.file.FileProps;
import org.vertx.java.core.file.FileSystem;
import org.vertx.java.core.file.FileSystemProps;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author Andreas Berger <andreas.berger@kiwigrid.com>
 */
public class RxFileSystem {
	private FileSystem core;

	public RxFileSystem(FileSystem fileSystem) {
		core = fileSystem;
	}

	/**
	 * Copy a file from the path {@code from} to path {@code to}, asynchronously.<p>
	 * The copy will fail if the destination already exists.<p>
	 *
	 * @param from
	 * @param to
	 */
	public Observable<Void> copy(String from,
								 String to)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.copy(from, to, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Copy a file from the path {@code from} to path {@code to}, asynchronously.<p>
	 * If {@code recursive} is {@code true} and {@code from} represents a directory, then the directory and its contents
	 * will be copied recursively to the destination {@code to}.<p>
	 * The copy will fail if the destination if the destination already exists.<p>
	 *
	 * @param from
	 * @param to
	 * @param recursive
	 */
	public Observable<Void> copy(String from,
								 String to,
								 boolean recursive)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.copy(from, to, recursive, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Move a file from the path {@code from} to path {@code to}, asynchronously.<p>
	 * The move will fail if the destination already exists.<p>
	 *
	 * @param from
	 * @param to
	 */
	public Observable<Void> move(String from,
								 String to)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.move(from, to, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Truncate the file represented by {@code path} to length {@code len} in bytes, asynchronously.<p>
	 * The operation will fail if the file does not exist or {@code len} is less than {@code zero}.
	 *
	 * @param path
	 * @param len
	 */
	public Observable<Void> truncate(String path,
									 long len)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.truncate(path, len, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Change the permissions on the file represented by {@code path} to {@code perms}, asynchronously.
	 * The permission String takes the form rwxr-x--- as
	 * specified <a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>.<p>
	 *
	 * @param path
	 * @param perms
	 */
	public Observable<Void> chmod(String path,
								  String perms)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.chmod(path, perms, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Change the permissions on the file represented by {@code path} to {@code perms}, asynchronously.
	 * The permission String takes the form rwxr-x--- as
	 * specified in {<a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>}.<p>
	 * If the file is directory then all contents will also have their permissions changed recursively. Any directory permissions will
	 * be set to {@code dirPerms}, whilst any normal file permissions will be set to {@code perms}.<p>
	 *
	 * @param path
	 * @param perms
	 * @param dirPerms
	 */
	public Observable<Void> chmod(String path,
								  String perms,
								  String dirPerms)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.chmod(path, perms, dirPerms, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Change the ownership on the file represented by {@code path} to {@code user} and {code group}, asynchronously.
	 *
	 * @param path
	 * @param user
	 * @param group
	 */
	public Observable<Void> chown(String path,
								  String user,
								  String group)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.chmod(path, user, group, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Obtain properties for the file represented by {@code path}, asynchronously.
	 * If the file is a link, the link will be followed.
	 *
	 * @param path
	 */
	public Observable<FileProps> props(String path)
	{
		AsyncResultMemoizeHandler<FileProps, FileProps> rh = new AsyncResultMemoizeHandler<>();
		core.props(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Obtain properties for the link represented by {@code path}, asynchronously.
	 * The link will not be followed.
	 *
	 * @param path
	 */
	public Observable<FileProps> lprops(String path)
	{
		AsyncResultMemoizeHandler<FileProps, FileProps> rh = new AsyncResultMemoizeHandler<>();
		core.lprops(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Create a hard link on the file system from {@code link} to {@code existing}, asynchronously.
	 *
	 * @param link
	 * @param existing
	 */
	public Observable<Void> link(String link,
								 String existing)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.link(link, existing, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Create a symbolic link on the file system from {@code link} to {@code existing}, asynchronously.
	 *
	 * @param link
	 * @param existing
	 */
	public Observable<Void> symlink(String link,
									String existing)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.symlink(link, existing, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Unlinks the link on the file system represented by the path {@code link}, asynchronously.
	 *
	 * @param link
	 */
	public Observable<Void> unlink(String link)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.unlink(link, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Returns the path representing the file that the symbolic link specified by {@code link} points to, asynchronously.
	 *
	 * @param link
	 */
	public Observable<String> readSymlink(String link)
	{
		AsyncResultMemoizeHandler<String, String> rh = new AsyncResultMemoizeHandler<>();
		core.readSymlink(link, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Deletes the file represented by the specified {@code path}, asynchronously.
	 *
	 * @param path
	 */
	public Observable<Void> delete(String path)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.delete(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Deletes the file represented by the specified {@code path}, asynchronously.<p>
	 * If the path represents a directory and {@code recursive = true} then the directory and its contents will be
	 * deleted recursively.
	 *
	 * @param path
	 * @param recursive
	 */
	public Observable<Void> delete(String path,
								   boolean recursive)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.delete(path, recursive, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Create the directory represented by {@code path}, asynchronously.<p>
	 * The operation will fail if the directory already exists.
	 *
	 * @param path
	 */
	public Observable<Void> mkdir(String path)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.mkdir(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Create the directory represented by {@code path}, asynchronously.<p>
	 * If {@code createParents} is set to {@code true} then any non-existent parent directories of the directory
	 * will also be created.<p>
	 * The operation will fail if the directory already exists.
	 *
	 * @param path
	 * @param createParents
	 */
	public Observable<Void> mkdir(String path,
								  boolean createParents)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.mkdir(path, createParents, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Create the directory represented by {@code path}, asynchronously.<p>
	 * The new directory will be created with permissions as specified by {@code perms}.
	 * The permission String takes the form rwxr-x--- as specified
	 * in <a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>.<p>
	 * The operation will fail if the directory already exists.
	 *
	 * @param path
	 * @param perms
	 */
	public Observable<Void> mkdir(String path,
								  String perms)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.mkdir(path, perms, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Create the directory represented by {@code path}, asynchronously.<p>
	 * The new directory will be created with permissions as specified by {@code perms}.
	 * The permission String takes the form rwxr-x--- as specified
	 * in <a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>.<p>
	 * If {@code createParents} is set to {@code true} then any non-existent parent directories of the directory
	 * will also be created.<p>
	 * The operation will fail if the directory already exists.<p>
	 *
	 * @param path
	 * @param perms
	 * @param createParents
	 */
	public Observable<Void> mkdir(String path,
								  String perms,
								  boolean createParents)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.mkdir(path, perms, createParents, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Read the contents of the directory specified by {@code path}, asynchronously.<p>
	 * The result is an array of String representing the paths of the files inside the directory.
	 *
	 * @param path
	 */
	public Observable<String[]> readDir(String path)
	{
		AsyncResultMemoizeHandler<String[], String[]> rh = new AsyncResultMemoizeHandler<>();
		core.readDir(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Read the contents of the directory specified by {@code path}, asynchronously.<p>
	 * The parameter {@code filter} is a regular expression. If {@code filter} is specified then only the paths that
	 * match  @{filter}will be returned.<p>
	 * The result is an array of String representing the paths of the files inside the directory.
	 *
	 * @param path
	 * @param filter
	 */
	public Observable<String[]> readDir(String path,
										String filter)
	{
		AsyncResultMemoizeHandler<String[], String[]> rh = new AsyncResultMemoizeHandler<>();
		core.readDir(path, filter, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Reads the entire file as represented by the path {@code path} as a {@link Buffer}, asynchronously.<p>
	 * Do not user this method to read very large files or you risk running out of available RAM.
	 *
	 * @param path
	 */
	public Observable<Buffer> readFile(String path)
	{
		return readFileChunked(path).reduce(new Buffer(), RxSupport.mergeBuffers);
	}

	/**
	 * Reads the file as represented by the path {@code path} as a {@link Buffer}, asynchronously.<p>
	 * The {@link rx.Subscriber#onNext(Object)} Method will be invoked by each chunk read.
	 *
	 * @param path
	 */
	public Observable<Buffer> readFileChunked(String path)
	{
		return open(path, null, true, false, false).flatMap(new Func1<RxFile, Observable<Buffer>>() {
			@Override
			public Observable<Buffer> call(RxFile rxFile) {
				return rxFile.read();
			}
		});
	}

	/**
	 * Creates the file, and writes the specified {@code Buffer data} to the file represented by the path {@code path},
	 * asynchronously.
	 *
	 * @param path
	 * @param data
	 */
	public Observable<Long> writeFile(String path,
									  final Buffer data)
	{
		return open(path, null, false, true, true).flatMap(new Func1<RxFile, Observable<Long>>() {
			@Override
			public Observable<Long> call(RxFile rxFile) {
				return rxFile.write(Observable.just(data));
			}
		});
	}

	/**
	 * Open the file represented by {@code path}, asynchronously.<p>
	 * The file is opened for both reading and writing. If the file does not already exist it will be created.
	 * Write operations will not automatically flush to storage.
	 *
	 * @param path
	 */
	public Observable<RxFile> open(String path)
	{
		AsyncResultMemoizeHandler<RxFile, AsyncFile> rh = new RxAsyncFileHandler();
		core.open(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Open the file represented by {@code path}, asynchronously.<p>
	 * The file is opened for both reading and writing. If the file does not already exist it will be created with the
	 * permissions as specified by {@code perms}.
	 * Write operations will not automatically flush to storage.
	 *
	 * @param path
	 * @param perms
	 */
	public Observable<RxFile> open(String path,
								   String perms)
	{
		AsyncResultMemoizeHandler<RxFile, AsyncFile> rh = new RxAsyncFileHandler();
		core.open(path, perms, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Open the file represented by {@code path}, asynchronously.<p>
	 * The file is opened for both reading and writing. If the file does not already exist and
	 * {@code createNew} is {@code true} it will be created with the permissions as specified by {@code perms}, otherwise
	 * the operation will fail.
	 * Write operations will not automatically flush to storage.
	 *
	 * @param path
	 * @param perms
	 * @param createNew
	 */
	public Observable<RxFile> open(String path,
								   String perms,
								   boolean createNew)
	{
		AsyncResultMemoizeHandler<RxFile, AsyncFile> rh = new RxAsyncFileHandler();
		core.open(path, perms, createNew, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Open the file represented by {@code path}, asynchronously.<p>
	 * If {@code read} is {@code true} the file will be opened for reading. If {@code write} is {@code true} the file
	 * will be opened for writing.<p>
	 * If the file does not already exist and
	 * {@code createNew} is {@code true} it will be created with the permissions as specified by {@code perms}, otherwise
	 * the operation will fail.<p>
	 * Write operations will not automatically flush to storage.
	 *
	 * @param path
	 * @param perms
	 * @param read
	 * @param write
	 * @param createNew
	 */
	public Observable<RxFile> open(String path,
								   String perms,
								   boolean read,
								   boolean write,
								   boolean createNew)
	{
		AsyncResultMemoizeHandler<RxFile, AsyncFile> rh = new RxAsyncFileHandler();
		core.open(path, perms, read, write, createNew, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Open the file represented by {@code path}, asynchronously.<p>
	 * If {@code read} is {@code true} the file will be opened for reading. If {@code write} is {@code true} the file
	 * will be opened for writing.<p>
	 * If the file does not already exist and
	 * {@code createNew} is {@code true} it will be created with the permissions as specified by {@code perms}, otherwise
	 * the operation will fail.<p>
	 * If {@code flush} is {@code true} then all writes will be automatically flushed through OS buffers to the underlying
	 * storage on each write.
	 *
	 * @param path
	 * @param perms
	 * @param read
	 * @param write
	 * @param createNew
	 * @param flush
	 */
	public Observable<RxFile> open(String path,
								   String perms,
								   boolean read,
								   boolean write,
								   boolean createNew,
								   boolean flush)
	{
		AsyncResultMemoizeHandler<RxFile, AsyncFile> rh = new RxAsyncFileHandler();
		core.open(path, perms, read, write, createNew, flush, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Creates an empty file with the specified {@code path}, asynchronously.
	 *
	 * @param path
	 */
	public Observable<Void> createFile(String path)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.createFile(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Creates an empty file with the specified {@code path} and permissions {@code perms}, asynchronously.
	 *
	 * @param path
	 * @param perms
	 */
	public Observable<Void> createFile(String path,
									   String perms)
	{
		AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
		core.createFile(path, perms, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Determines whether the file as specified by the path {@code path} exists, asynchronously.
	 *
	 * @param path
	 */
	public Observable<Boolean> exists(String path)
	{
		AsyncResultMemoizeHandler<Boolean, Boolean> rh = new AsyncResultMemoizeHandler<>();
		core.exists(path, rh);
		return Observable.create(rh.subscribe);
	}

	/**
	 * Returns properties of the file-system being used by the specified {@code path}, asynchronously.
	 *
	 * @param path
	 */
	public Observable<FileSystemProps> fsProps(String path)
	{
		AsyncResultMemoizeHandler<FileSystemProps, FileSystemProps> rh = new AsyncResultMemoizeHandler<>();
		core.fsProps(path, rh);
		return Observable.create(rh.subscribe);
	}

	private static class RxAsyncFileHandler extends AsyncResultMemoizeHandler<RxFile, AsyncFile> {
		@Override
		public RxFile wrap(final AsyncFile value) {
			return new RxFile(value);
		}
	}
}
