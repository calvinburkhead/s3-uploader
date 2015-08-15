package org.nwmsrocks.s3uploader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;

public class Uploader implements UploaderInterface, Runnable {

	private static final String SYNTAX = "s3upload -source <SOURCE> -bucket <BUCKET> -credential <CREDENTIAL>";
	public static final int DEFAULT_MAX_WORKER_THREADS = 10;

	private static void launch(CommandLine cmd) throws InterruptedException {
		String sourcePath = cmd.getOptionValue("source");
		String bucket = cmd.getOptionValue("bucket");
		String credentialPath = cmd.getOptionValue("credential");
		String prefix = cmd.getOptionValue("prefix", "");
		Region region = Region.getRegion(Regions.fromName(cmd.getOptionValue("region", "")));
		boolean recurse = cmd.hasOption("recurse");
		boolean pretend = cmd.hasOption("pretend");
		boolean delete = cmd.hasOption("delete");
		boolean create = cmd.hasOption("create");
		boolean purge = cmd.hasOption("purge");
		int maxThreads = Integer.valueOf(cmd.getOptionValue("threads", "10"));
		S3Object s3Object = new S3Object(prefix, sourcePath);

		Uploader uploader = new Uploader();
		uploader.setBucketName(bucket);
		uploader.setCredentialPath(credentialPath);
		uploader.setDestination(prefix);
		uploader.setRegion(region);
		uploader.setRecurse(recurse);
		uploader.setPretend(pretend);
		uploader.setDeleteAfterUpload(delete);
		uploader.setPurge(purge);
		uploader.setCreate(create);
		uploader.setMaximumThreadCount(maxThreads);

		// Queue the file
		uploader.queue(s3Object);

		// Upload
		uploader.upload();
	}

	public static void main(String[] args) throws InterruptedException {

		Options options = new Options();
		options.addOption(Option.builder("source")
								.required()
								.desc("Source file to upload")
								.hasArg()
								.argName("SOURCE_FILE")
								.build());
		options.addOption(Option.builder("bucket")
								.required()
								.desc("Bucket to upload file(s) into")
								.hasArg()
								.argName("BUCKET_NAME")
								.build());
		options.addOption(Option.builder("prefix")
								.desc("Prefix to prepend")
								.hasArg()
								.build());
		options.addOption(Option.builder("credential")
								.required()
								.desc("AWS credentials file")
								.hasArg()
								.argName("AWS_CREDENTIAL_FILE")
								.build());
		options.addOption(Option.builder("region")
								.desc("AWS region")
								.hasArg()
								.build());
		options.addOption(Option.builder("r")
								.longOpt("recurse")
								.desc("Recurse into sub-directories")
								.build());
		options.addOption(Option.builder("t")
								.longOpt("threads")
								.desc("Maxmium upload threads (Default: " + DEFAULT_MAX_WORKER_THREADS + ")")
								.hasArg()
								.type(Integer.TYPE)
								.build());
		options.addOption(Option.builder("delete")
								.desc("Delete local files after upload")
								.build());
		options.addOption(Option.builder("pretend")
								.desc("Don't actually upload the files")
								.build());
		options.addOption(Option.builder("h")
								.longOpt("help")
								.desc("Display this help menu")
								.build());
		options.addOption(Option.builder("create")
								.desc("Create the specified bucket if it does not exist")
								.build());
		options.addOption(Option.builder("purge")
								.desc("Purge bucket before uploading")
								.build());

		CommandLineParser parser = new DefaultParser();

		try {
			CommandLine cmd = parser.parse(options, args, true);
			validateArguments(cmd);
			launch(cmd);
		} catch (ParseException | IllegalArgumentException e) {
			System.out.println(e.getMessage());
			showHelp(options);
		}
	}

	private static void showHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(SYNTAX, "WARNING: This will ALWAYS overwrite matching keys in specifed bucket", options,
				"Calvin Burkhead 2015");
	}

	private static void validateArguments(CommandLine cmd) throws IllegalArgumentException {
		// Validate existence of source file
		File file = new File(cmd.getOptionValue("source"));
		if (!file.exists()) {
			throw new IllegalArgumentException(cmd.getOptionValue("source") + " does not exist.");
		}

		// Validate existence of credentials file
		file = new File(cmd.getOptionValue("credential"));
		if (!file.exists()) {
			throw new IllegalArgumentException(cmd.getOptionValue("credential") + " does not exist.");
		}

		// Validate region
		if (cmd.hasOption("region") && null == Regions.fromName(cmd.getOptionValue("region"))) {
			throw new IllegalArgumentException("Invalid region: " + cmd.getOptionValue("region"));
		}

		// Validate thread count
		if (cmd.hasOption("threads")) {
			try {
				Integer.valueOf(cmd.getOptionValue("threads"));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(e);
			}
		}

	}

	private String bucketName;
	private AmazonS3Client client;
	private BlockingQueue<S3Object> queue;
	private ThreadPoolExecutor sessionThreadPool;
	private int liveThreadCount;
	private boolean running;
	private String destination;
	private int maximumThreadCount;
	private boolean deleteAfterUpload;
	private boolean pretend;
	private Region region;
	private String credentialPath;
	private boolean purge;
	private boolean create;
	private boolean recurse;

	public Uploader() {
		// Set defaults
		queue = new LinkedBlockingQueue<S3Object>();
		sessionThreadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		liveThreadCount = 0;
		running = false;
		recurse = false;
		destination = "";
		maximumThreadCount = DEFAULT_MAX_WORKER_THREADS;
		setDeleteAfterUpload(false);
		setPretend(false);
		setRegion(Region.getRegion(Regions.DEFAULT_REGION));
	}

	private AmazonS3Client connect() {

		// AmazonS3Client client = new AmazonS3Client(new
		// ClasspathPropertiesFileCredentialsProvider(getCredentialPath()));
		AmazonS3Client client = new AmazonS3Client(new PropertiesFileCredentialsProvider(getCredentialPath()));
		client.setRegion(getRegion());
		return client;
	}

	private synchronized void dropLiveThreadCount() {
		liveThreadCount--;
		notifyAll();
	}

	@Override
	public String getBucketName() {
		return bucketName;
	}

	private AmazonS3Client getClient() {
		return client;
	}

	@Override
	public String getCredentialPath() {
		return credentialPath;
	}

	@Override
	public String getDestination() {
		return destination;
	}

	@Override
	public int getLiveThreadCount() {
		return liveThreadCount;
	}

	@Override
	public int getMaximumThreadCount() {
		return maximumThreadCount;
	}

	private S3Object getNext() {
		S3Object s3Object;
		try {
			s3Object = queue.poll(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			s3Object = null;
		}

		return s3Object;
	}

	@Override
	public Region getRegion() {
		return region;
	}

	@Override
	public boolean isCreate() {
		return create;
	}

	@Override
	public boolean isDeleteAfterUpload() {
		return deleteAfterUpload;
	}

	@Override
	public boolean isPretend() {
		return pretend;
	}

	@Override
	public boolean isPurge() {
		return purge;
	}

	public boolean isRecurse() {
		return recurse;
	}

	@Override
	public synchronized boolean isRunning() {
		return running;
	}

	@Override
	public S3Object peek() {
		return queue.peek();
	}

	private void prepare() {
		System.out.println("Preparing for uploading");
		// Connect AWS
		setClient(connect());

		// TODO check if bucket exists and is writable

		// Create Bucket
		if (isCreate()) {
			System.out.println("Create bucket " + getBucketName());
			if (!isPretend()) {
				getClient().createBucket(getBucketName(), getRegion().getName());
			}
		}

		// Purge bucket
		if (isPurge()) {
			System.out.println("Purge bucket");
			if (!isPretend()) {
				// TODO write method to purge bucket
			}
		}
	}

	// Walk through a directory and add all files to the queue
	private void processDirectories(S3Object s3Object) {
		File directory = new File(s3Object.getPath());

		if (directory.isFile()) {
			throw new IllegalArgumentException("File supplied is not a directory");
		}

		System.out.println("Walking directory:" + directory.toString());

		for (File file : directory.listFiles()) {
			if (file.isDirectory()) {
				try {
					StringBuilder prefix = new StringBuilder();
					if (null != s3Object.getPrefix() && !"".equals(s3Object.getPrefix())) {
						prefix.append(s3Object.getPrefix());
						prefix.append("/");
					}

					prefix.append(file.getName());

					processDirectories(new S3Object(prefix.toString(), file.getCanonicalPath()));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					queue(new S3Object(s3Object.getPrefix(), file.getCanonicalPath()));
				} catch (InterruptedException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void queue(S3Object s3Object) throws InterruptedException {
		System.out.println(s3Object.toString() + " added to queue");
		queue.put(s3Object);
	}

	private synchronized void raiseLiveThreadCount() {
		liveThreadCount++;
		notifyAll();
	}

	@Override
	public void run() {
		raiseLiveThreadCount();

		S3Object s3Object = null;
		while ((s3Object = getNext()) != null) {
			try {
				File file = new File(s3Object.getPath());

				if (file.isFile()) {
					StringBuilder destinationPath = new StringBuilder();
					if (null != getDestination() && !"".equals(getDestination())) {
						destinationPath.append(getDestination());
						destinationPath.append("/");
					}

					if (null != s3Object.getPrefix() && !"".equals(s3Object.getPrefix())) {
						destinationPath.append(s3Object.getPrefix());
						destinationPath.append("/");
					}

					destinationPath.append(file.getName());

					try {

						System.out.println("Uploading " + destinationPath.toString());

						// If we are pretending, don't upload
						if (!isPretend()) {
							// UPLOAD THE DAMN FILE!
							getClient().putObject(getBucketName(), destinationPath.toString(), file);
						}

						// If we are deleting, delete the local file
						if (isDeleteAfterUpload()) {
							System.out.println("Delete local copy of " + file.getCanonicalPath());
							// If we are pretending, don't delete
							if (!isPretend()) {
								// DELETE THE FUCKER!
								file.delete();
							}
						}

					} catch (Exception e) {
						e.printStackTrace();
					}

				} else if (file.isDirectory() && isRecurse()) {
					processDirectories(s3Object);
				} else {
					System.out.println("Skipping " + file.getCanonicalPath());
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		dropLiveThreadCount();
	}

	@Override
	public void setBucketName(String bucketName) {
		if (!isRunning()) {
			synchronized (this) {
				this.bucketName = bucketName;
			}
		} else
			throw new IllegalStateException("Cannot change bucket while uploading");
	}

	private void setClient(AmazonS3Client client) {
		if (!isRunning() && getLiveThreadCount() < 1 && getClient() == null) {
			synchronized (this) {
				this.client = client;
			}

		} else {
			throw new IllegalStateException("Cannot set client while uploading");
		}
	}

	@Override
	public void setCreate(boolean create) {
		if (!isRunning() && getLiveThreadCount() < 1) {
			synchronized (this) {
				this.create = create;
			}
		} else {
			throw new IllegalArgumentException("Cannot change create setting while uploading");
		}
	}

	@Override
	public void setCredentialPath(String credentialPath) {
		if (!isRunning() && getLiveThreadCount() < 1 && client == null) {
			synchronized (this) {
				this.credentialPath = credentialPath;
			}

		} else {
			throw new IllegalStateException("Cannot set credential path while uploading");
		}
	}

	@Override
	public void setDeleteAfterUpload(boolean deleteAfterUpload) {
		if (!isRunning() && getLiveThreadCount() < 1) {
			synchronized (this) {
				this.deleteAfterUpload = deleteAfterUpload;
			}

		} else {
			throw new IllegalStateException("Cannot change delete mode while uploading");
		}
	}

	@Override
	public void setDestination(String destination) {
		if (!isRunning()) {
			synchronized (this) {
				this.destination = destination;
			}
		} else
			throw new IllegalStateException("Cannot change destination while uploading");

	}

	@Override
	public void setMaximumThreadCount(int maximumThreadCount) {
		if (!isRunning() && getLiveThreadCount() < 1) {
			synchronized (this) {
				this.maximumThreadCount = maximumThreadCount;
			}

		} else
			throw new IllegalAccessError("Cannot change thread count while uploading");

	}

	@Override
	public void setPretend(boolean pretend) {
		if (!isRunning() && getLiveThreadCount() < 1) {
			synchronized (this) {
				this.pretend = pretend;
			}

		} else {
			throw new IllegalStateException("Cannot change recursion setting whiel uploading");
		}
	}

	@Override
	public void setPurge(boolean purge) {
		if (!isRunning() && getLiveThreadCount() < 1) {
			synchronized (this) {
				this.purge = purge;
			}
		} else {
			throw new IllegalArgumentException("Cannot change purge setting while uploading");
		}

	}

	@Override
	public void setRecurse(boolean recurse) throws IllegalStateException {
		if (!isRunning()) {
			synchronized (this) {
				this.recurse = recurse;
			}
		} else
			throw new IllegalArgumentException("Cannot change recurse setting while uploading");

	}

	@Override
	public void setRegion(Region region) {
		if (!isRunning() && getLiveThreadCount() < 1 && client == null) {
			synchronized (this) {
				this.region = region;
			}

		} else {
			throw new IllegalStateException("Cannot set region while uploading");
		}
	}

	private void setRunning(boolean running) throws IllegalStateException {
		if (getLiveThreadCount() < 1) {
			synchronized (this) {
				this.running = running;
				notifyAll();
			}
		} else
			throw new IllegalStateException("Cannot change value of running while uploading");
	}

	@Override
	public void upload() {
		upload(true);
	}

	private void upload(boolean firstPass) {
		if (firstPass) {
			// Prep environment (connect to AWS, purge, create...)
			prepare();

			setRunning(true);
		}

		// Start threads
		for (int i = 0; i < getMaximumThreadCount(); i++)
			sessionThreadPool.submit(this);

		// Monitor threads
		int liveThreadCount;
		while ((liveThreadCount = getLiveThreadCount()) > 0) {
			System.out.println("Active Worker Threads:" + liveThreadCount);
			synchronized (this) {
				try {
					wait(10000);
				} catch (InterruptedException e) {
				}
			}
		}

		// Call upload again (this is necessary when directories are found and
		// recursed)
		if (queue.size() > 0) {
			upload(false);
		}

		if (firstPass) {
			// Shutdown the thread pool
			sessionThreadPool.shutdown();

			System.out.println("Finished");
			setRunning(false);
		}
	}
}
