package io.quorumfs.storage;

import static io.quorumfs.storage.StorageException.Code.*;

import io.quorumfs.ring.HashRing;
import io.quorumfs.versioning.Siblings;
import io.quorumfs.versioning.VectorClock;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;
import org.rocksdb.*;

/**
 * Durable immutable local versions. Causal metadata is atomic with publication; no distributed
 * acknowledgments.
 */
public final class ObjectStorage implements AutoCloseable {
  public static final int CHUNK_BYTES = 256 * 1024;
  public static final long MAX_OBJECT_BYTES = 64L * 1024 * 1024;
  static final List<String> FAMILIES =
      List.of(
          "default",
          "manifests",
          "chunks",
          "counters",
          "staging",
          "vectors",
          "requests",
          "hints",
          "tombstones",
          "ring",
          "heads");
  private static final byte[] IDENTITY = "identity/v1".getBytes(StandardCharsets.UTF_8);
  private static final byte[] FORMAT = "storage-format".getBytes(StandardCharsets.UTF_8);
  private static final byte[] COUNTER = new byte[] {1};
  private static final byte[] FORMAT_V1 = new byte[] {1};
  private static final byte[] RING = {1}, NODE = {2};
  private HashRing ring;
  private String node;
  private final Path directory;
  private final String identity;
  private final Limits limits;
  private final Faults faults;
  private final LongSupplier clock;
  private final DBOptions options;
  private final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
  private final List<ColumnFamilyHandle> handles = new ArrayList<>();
  private final Cache cache;
  private final WriteBufferManager buffers;
  private final Map<UUID, Upload> uploads = new HashMap<>();
  private RocksDB db;
  private boolean closed;
  private boolean failed;
  private long committed, aborted, recovered, corruptions, ioFailures;

  static {
    RocksDB.loadLibrary();
  }

  public record Limits(long maxObjectBytes, int maxUploads, Duration uploadTtl) {
    public Limits {
      if (maxObjectBytes < 0
          || maxObjectBytes > MAX_OBJECT_BYTES
          || maxUploads < 1
          || maxUploads > 64
          || uploadTtl.isNegative()
          || uploadTtl.isZero()
          || uploadTtl.compareTo(Duration.ofDays(1)) > 0) {
        throw new IllegalArgumentException("Invalid storage limits");
      }
    }

    public static Limits defaults() {
      return new Limits(MAX_OBJECT_BYTES, 8, Duration.ofMinutes(15));
    }
  }

  public record Version(UUID id, long sequence, long size, byte[] sha256) {
    public Version {
      sha256 = sha256.clone();
    }

    @Override
    public byte[] sha256() {
      return sha256.clone();
    }
  }

  public record Stats(
      int activeUploads,
      long committed,
      long aborted,
      long recovered,
      long corruptions,
      long ioFailures,
      boolean failed) {}

  enum Point {
    COUNTER_WRITE,
    STAGE_WRITE,
    CHUNK_WRITE,
    WAL_SYNC,
    MANIFEST_WRITE,
    AFTER_MANIFEST,
    CLEANUP
  }

  @FunctionalInterface
  interface Faults {
    void at(Point point) throws IOException;
  }

  public ObjectStorage(Path directory, String identity) throws IOException {
    this(directory, identity, Limits.defaults());
  }

  public ObjectStorage(Path directory, String identity, Limits limits) throws IOException {
    this(directory, identity, limits, point -> {}, System::nanoTime);
  }

  ObjectStorage(Path directory, String identity, Limits limits, Faults faults, LongSupplier clock)
      throws IOException {
    this.directory = directory.toAbsolutePath().normalize();
    this.identity = Objects.requireNonNull(identity);
    this.limits = Objects.requireNonNull(limits);
    this.faults = faults;
    this.clock = clock;
    if (identity.isBlank() || identity.length() > 16384)
      throw new IllegalArgumentException("Invalid storage identity");
    Files.createDirectories(this.directory);
    boolean existing = Files.exists(this.directory.resolve("CURRENT"));
    cache = new LRUCache(8L * 1024 * 1024);
    buffers = new WriteBufferManager(16L * 1024 * 1024, cache);
    options =
        new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setMaxOpenFiles(128)
            .setMaxBackgroundJobs(2)
            .setWriteBufferManager(buffers);
    try {
      List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
      for (String family : FAMILIES) {
        ColumnFamilyOptions cf =
            new ColumnFamilyOptions()
                .setWriteBufferSize(2L * 1024 * 1024)
                .setMaxWriteBufferNumber(2)
                .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
        cfOptions.add(cf);
        descriptors.add(new ColumnFamilyDescriptor(family.getBytes(StandardCharsets.UTF_8), cf));
      }
      db = RocksDB.open(options, this.directory.toString(), descriptors, handles);
      byte[] stored = db.get(IDENTITY);
      byte[] expected = identity.getBytes(StandardCharsets.UTF_8);
      if ((existing && stored == null) || (stored != null && !Arrays.equals(stored, expected))) {
        throw new IOException(
            "Persisted node/cluster identity or membership differs; refusing startup");
      }
      byte[] format = db.get(FORMAT);
      if (format != null && !Arrays.equals(format, FORMAT_V1))
        throw new StorageException(CORRUPT, "Unsupported storage format");
      try (WriteBatch batch = new WriteBatch();
          WriteOptions write = sync()) {
        batch.put(IDENTITY, expected);
        batch.put(FORMAT, FORMAT_V1);
        db.write(write, batch);
      }
      byte[] persistedRing = db.get(cf("ring"), RING);
      byte[] persistedNode = db.get(cf("ring"), NODE);
      if ((persistedRing == null) != (persistedNode == null))
        throw new StorageException(CORRUPT, "Incomplete ring identity");
      if (persistedRing != null) {
        ring = HashRing.deserialize(persistedRing);
        node = new String(persistedNode, StandardCharsets.UTF_8);
        if (!ring.nodeIds().contains(node))
          throw new StorageException(CORRUPT, "Invalid ring node");
      }
      recover();
    } catch (RocksDBException e) {
      close();
      throw new StorageException(IO_FAILURE, "Cannot open storage", e);
    } catch (IOException | RuntimeException e) {
      close();
      throw e;
    }
  }

  private ColumnFamilyHandle cf(String name) {
    return handles.get(FAMILIES.indexOf(name));
  }

  private static WriteOptions sync() {
    return new WriteOptions().setSync(true).setDisableWAL(false);
  }

  public static byte[] sha256(byte[] value) {
    return digest().digest(value);
  }

  private static MessageDigest digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private void open() throws StorageException {
    if (closed) throw new StorageException(CLOSED, "Storage is closed");
    if (failed)
      throw new StorageException(IO_FAILURE, "Storage failed; close and reopen before retrying");
  }

  private StorageException failure(Exception e) {
    failed = true;
    ioFailures++;
    return new StorageException(
        IO_FAILURE,
        "Storage I/O failed; commit outcome may be unknown; reopen and inspect version",
        e);
  }

  private void objectKey(String namespace, byte[] key) throws StorageException {
    if (namespace == null
        || !namespace.matches("[a-zA-Z0-9_-]{1,64}")
        || key == null
        || key.length < 1
        || key.length > 1024) {
      throw new StorageException(INVALID, "Invalid namespace or key size");
    }
  }

  /** Allocated synchronously; cancellation and failed uploads never recycle sequence numbers. */
  private long allocate(long minimum) throws IOException, RocksDBException {
    byte[] raw = db.get(cf("counters"), COUNTER);
    if (raw != null && raw.length != 8)
      throw new StorageException(CORRUPT, "Invalid counter encoding");
    long stored = raw == null ? 0 : ByteBuffer.wrap(raw).getLong();
    if (stored < 0) throw new StorageException(CORRUPT, "Invalid counter");
    long previous = Math.max(stored, minimum);
    if (previous < 0 || previous == Long.MAX_VALUE)
      throw new StorageException(CAPACITY, "Counter exhausted or invalid");
    long next = previous + 1;
    faults.at(Point.COUNTER_WRITE);
    try (WriteOptions write = sync()) {
      db.put(cf("counters"), write, COUNTER, ByteBuffer.allocate(8).putLong(next).array());
    }
    return next;
  }

  public synchronized Upload begin(String namespace, byte[] key, long size, byte[] sha256)
      throws IOException {
    return beginVersion(namespace, key, size, sha256, null, false);
  }

  private Upload beginVersion(
      String namespace, byte[] key, long size, byte[] sha256, VectorClock context, boolean replica)
      throws IOException {
    return beginVersion(namespace, key, size, sha256, context, replica, UUID.randomUUID());
  }

  private Upload beginVersion(
      String namespace,
      byte[] key,
      long size,
      byte[] sha256,
      VectorClock context,
      boolean replica,
      UUID id)
      throws IOException {
    open();
    expire();
    objectKey(namespace, key);
    if (context != null) {
      if (ring == null) throw new StorageException(INVALID, "Configure ring before causal writes");
      context.requireMembers(ring.nodeIds());
      if (replica && context.counters().isEmpty())
        throw new StorageException(INVALID, "Empty replica vector");
    }
    requireKind(prefix(namespace, key), context != null);
    if (size < 0 || size > limits.maxObjectBytes() || sha256 == null || sha256.length != 32)
      throw new StorageException(INVALID, "Invalid size or SHA-256");
    if (uploads.size() >= limits.maxUploads())
      throw new StorageException(CAPACITY, "Upload capacity exhausted");
    try {
      if (uploads.containsKey(id) || db.get(cf("requests"), StorageKeys.id(id)) != null)
        throw new StorageException(INVALID, "Version ID is already in use");
      long sequence = allocate(context == null ? 0 : context.get(node));
      Upload upload =
          new Upload(
              id,
              StorageKeys.manifest(namespace, key, id),
              sequence,
              size,
              sha256.clone(),
              context == null ? null : replica ? context : context.with(node, sequence));
      faults.at(Point.STAGE_WRITE);
      try (WriteOptions write = sync()) {
        db.put(cf("staging"), write, StorageKeys.id(id), upload.manifestKey);
      }
      uploads.put(id, upload);
      return upload;
    } catch (RocksDBException e) {
      throw failure(e);
    } catch (IOException e) {
      if (e instanceof StorageException) throw e;
      throw failure(e);
    }
  }

  /** Persist the exact vnode map and local identity before allowing causal operations. */
  public synchronized void configureRing(HashRing expected, String localNode) throws IOException {
    open();
    if (!expected.nodeIds().contains(localNode))
      throw new StorageException(INVALID, "Unknown local node");
    if (ring != null) {
      ring.requireCompatible(expected);
      if (!node.equals(localNode))
        throw new StorageException(INVALID, "Local counter identity mismatch");
      return;
    }
    try (WriteBatch batch = new WriteBatch();
        WriteOptions write = sync()) {
      batch.put(cf("ring"), RING, expected.serialize());
      batch.put(cf("ring"), NODE, localNode.getBytes(StandardCharsets.UTF_8));
      db.write(write, batch);
      ring = expected;
      node = localNode;
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  public synchronized Upload beginCausal(
      String namespace, byte[] key, long size, byte[] sha256, VectorClock context)
      throws IOException {
    return beginVersion(namespace, key, size, sha256, Objects.requireNonNull(context), false);
  }

  /**
   * Internal local ingestion primitive; callers must supply the peer's full fixed configuration.
   */
  public synchronized Upload beginReplica(
      String namespace, byte[] key, long size, byte[] sha256, VectorClock vector, HashRing peer)
      throws IOException {
    open();
    if (ring == null) throw new StorageException(INVALID, "Ring not configured");
    ring.requireCompatible(peer);
    return beginVersion(namespace, key, size, sha256, Objects.requireNonNull(vector), true);
  }

  /** Persist a coordinator event without publishing a non-owner object copy. */
  public synchronized VectorClock allocateVector(VectorClock context) throws IOException {
    open();
    if (ring == null) throw new StorageException(INVALID, "Ring not configured");
    context.requireMembers(ring.nodeIds());
    try {
      return context.with(node, allocate(context.get(node)));
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  /** Network replicas preserve the coordinator-assigned immutable UUID. */
  public synchronized Upload beginReplica(
      String namespace,
      byte[] key,
      long size,
      byte[] sha256,
      VectorClock vector,
      HashRing peer,
      UUID id)
      throws IOException {
    open();
    if (ring == null) throw new StorageException(INVALID, "Ring not configured");
    ring.requireCompatible(peer);
    return beginVersion(namespace, key, size, sha256, Objects.requireNonNull(vector), true, id);
  }

  /** Resolve precisely the observed contexts; a concurrent unobserved sibling remains visible. */
  public synchronized Upload resolve(
      String namespace, byte[] key, long size, byte[] sha256, Collection<UUID> observed)
      throws IOException {
    open();
    if (observed.isEmpty() || observed.size() > Siblings.LIMIT)
      throw new StorageException(INVALID, "Resolution needs 1..32 observed versions");
    VectorClock context = VectorClock.empty();
    for (UUID id : observed) {
      head(namespace, key, id);
      context = context.merge(vector(id));
    }
    return beginCausal(namespace, key, size, sha256, context);
  }

  public synchronized VectorClock vector(UUID id) throws IOException {
    open();
    try {
      byte[] raw = db.get(cf("vectors"), StorageKeys.id(id));
      if (raw == null) throw new StorageException(NOT_FOUND, "Causal version not found");
      return checkedVector(raw);
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  private VectorClock checkedVector(byte[] raw) throws StorageException {
    try {
      if (ring == null) throw new IllegalArgumentException("Causal data without ring");
      VectorClock vector = VectorClock.deserialize(raw);
      vector.requireMembers(ring.nodeIds());
      if (vector.counters().isEmpty()) throw new IllegalArgumentException("Empty stored vector");
      return vector;
    } catch (IllegalArgumentException e) {
      throw new StorageException(CORRUPT, "Invalid vector", e);
    }
  }

  private static boolean startsWith(byte[] value, byte[] prefix) {
    return value.length >= prefix.length
        && Arrays.equals(value, 0, prefix.length, prefix, 0, prefix.length);
  }

  private static byte[] prefix(String namespace, byte[] key) {
    byte[] encoded = StorageKeys.manifest(namespace, key, new UUID(0, 0));
    return Arrays.copyOf(encoded, encoded.length - 16);
  }

  private static byte[] manifestKey(byte[] prefix, UUID id) {
    return ByteBuffer.allocate(prefix.length + 16)
        .put(prefix)
        .putLong(id.getMostSignificantBits())
        .putLong(id.getLeastSignificantBits())
        .array();
  }

  private static Siblings.Entry entry(Version version, VectorClock vector) {
    return new Siblings.Entry(version.id(), vector, HexFormat.of().formatHex(version.sha256()));
  }

  private List<Siblings.Entry> heads(byte[] prefix) throws IOException, RocksDBException {
    List<Siblings.Entry> result = new ArrayList<>();
    try (RocksIterator iterator = db.newIterator(cf("heads"))) {
      for (iterator.seek(prefix);
          iterator.isValid() && startsWith(iterator.key(), prefix);
          iterator.next()) {
        UUID id = StorageKeys.manifestId(iterator.key());
        result.add(
            entry(
                decode(id, db.get(cf("manifests"), iterator.key())),
                checkedVector(iterator.value())));
        if (result.size() > Siblings.LIMIT)
          throw new StorageException(CORRUPT, "Too many persisted siblings");
      }
      iterator.status();
    }
    result.sort(Comparator.comparing(e -> e.id().toString()));
    if (!Siblings.merge(result).equals(result))
      throw new StorageException(CORRUPT, "Nonmaximal head index");
    return List.copyOf(result);
  }

  public synchronized List<Siblings.Entry> siblings(String namespace, byte[] key)
      throws IOException {
    open();
    objectKey(namespace, key);
    try {
      return heads(prefix(namespace, key));
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  private List<Siblings.Entry> mergedHeads(byte[] manifestKey, Version version, VectorClock vector)
      throws IOException {
    byte[] prefix = Arrays.copyOf(manifestKey, manifestKey.length - 16);
    try {
      // Check all immutable history, including versions already superseded by another head.
      try (RocksIterator iterator = db.newIterator(cf("manifests"))) {
        for (iterator.seek(prefix);
            iterator.isValid() && startsWith(iterator.key(), prefix);
            iterator.next()) {
          UUID id = StorageKeys.manifestId(iterator.key());
          VectorClock previous = checkedVector(db.get(cf("vectors"), StorageKeys.id(id)));
          if (previous.equals(vector)
              && !MessageDigest.isEqual(decode(id, iterator.value()).sha256(), version.sha256()))
            throw new StorageException(CORRUPT, "Equal vector has different content");
        }
        iterator.status();
      }
      List<Siblings.Entry> candidates = new ArrayList<>(heads(prefix));
      candidates.add(entry(version, vector));
      return Siblings.merge(candidates);
    } catch (IllegalStateException e) {
      throw new StorageException(CAPACITY, "Sibling limit exceeded", e);
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  private void requireKind(byte[] prefix, boolean causal) throws IOException {
    try (RocksIterator iterator = db.newIterator(cf("manifests"))) {
      for (iterator.seek(prefix);
          iterator.isValid() && startsWith(iterator.key(), prefix);
          iterator.next()) {
        byte[] raw = iterator.value();
        decode(StorageKeys.manifestId(iterator.key()), raw);
        if ((raw[0] == 2) != causal)
          throw new StorageException(
              INVALID, "Legacy and causal versions require separate object keys");
      }
      iterator.status();
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  private void expire() throws IOException {
    for (Upload upload : List.copyOf(uploads.values())) {
      if (clock.getAsLong() - upload.started >= limits.uploadTtl().toNanos()) upload.abort();
    }
  }

  public final class Upload implements AutoCloseable {
    private final UUID id;
    private final byte[] manifestKey;
    private final long sequence, size, started;
    private final byte[] expected;
    private final VectorClock vector;
    private final MessageDigest checksum = digest();
    private long received;
    private boolean done;

    private Upload(
        UUID id, byte[] key, long sequence, long size, byte[] expected, VectorClock vector) {
      this.id = id;
      this.manifestKey = key;
      this.sequence = sequence;
      this.size = size;
      this.expected = expected;
      this.vector = vector;
      started = clock.getAsLong();
    }

    public UUID id() {
      return id;
    }

    private void live() throws IOException {
      open();
      if (done) throw new StorageException(CANCELLED, "Upload is no longer active");
      if (Thread.currentThread().isInterrupted()
          || clock.getAsLong() - started >= limits.uploadTtl().toNanos()) {
        abort();
        throw new StorageException(CANCELLED, "Upload cancelled or deadline exceeded");
      }
    }

    private void invalid(String reason) throws IOException {
      abort();
      throw new StorageException(INVALID, reason);
    }

    /**
     * Exactly one fixed-size chunk except the final chunk; rejects gaps, overlaps and duplicate
     * frames.
     */
    public void append(long offset, byte[] bytes, byte[] sha256) throws IOException {
      synchronized (ObjectStorage.this) {
        live();
        if (offset != received
            || bytes == null
            || bytes.length == 0
            || bytes.length != Math.min(CHUNK_BYTES, size - received)
            || sha256 == null
            || sha256.length != 32) {
          invalid("Invalid offset, chunk length or checksum");
        }
        byte[] data = bytes.clone();
        byte[] chunkHash = sha256.clone();
        if (!MessageDigest.isEqual(sha256(data), chunkHash)) invalid("Chunk checksum mismatch");
        byte[] encoded = ByteBuffer.allocate(32 + data.length).put(chunkHash).put(data).array();
        try {
          faults.at(Point.CHUNK_WRITE);
          try (WriteOptions write = new WriteOptions().setDisableWAL(false)) {
            db.put(cf("chunks"), write, StorageKeys.chunk(id, offset), encoded);
          }
          checksum.update(data);
          received += data.length;
        } catch (RocksDBException | IOException e) {
          throw failure(e);
        }
      }
    }

    public Version commit() throws IOException {
      synchronized (ObjectStorage.this) {
        live();
        if (received != size || !MessageDigest.isEqual(checksum.digest(), expected))
          invalid("Object size or whole-object checksum mismatch");
        requireKind(Arrays.copyOf(manifestKey, manifestKey.length - 16), vector != null);
        Version result = new Version(id, sequence, size, expected);
        List<Siblings.Entry> next =
            vector == null ? List.of() : mergedHeads(manifestKey, result, vector);
        try {
          faults.at(Point.WAL_SYNC);
          db.syncWal();
          faults.at(Point.MANIFEST_WRITE);
          try (WriteBatch batch = new WriteBatch();
              WriteOptions write = sync()) {
            byte[] manifest = encode(result);
            if (vector != null) {
              manifest[0] = 2;
              batch.put(cf("vectors"), StorageKeys.id(id), vector.serialize());
              byte[] prefix = Arrays.copyOf(manifestKey, manifestKey.length - 16);
              for (Siblings.Entry head : heads(prefix))
                batch.delete(cf("heads"), manifestKey(prefix, head.id()));
              for (Siblings.Entry head : next)
                batch.put(cf("heads"), manifestKey(prefix, head.id()), head.clock().serialize());
            }
            batch.put(cf("manifests"), manifestKey, manifest);
            batch.put(cf("requests"), StorageKeys.id(id), manifestKey);
            batch.delete(cf("staging"), StorageKeys.id(id));
            db.write(write, batch);
          }
          done = true;
          uploads.remove(id);
          committed++;
          faults.at(Point.AFTER_MANIFEST);
          return result;
        } catch (RocksDBException | IOException e) {
          throw failure(e);
        }
      }
    }

    public void abort() throws IOException {
      synchronized (ObjectStorage.this) {
        if (done || closed) return;
        open();
        try {
          cleanup(id);
          done = true;
          uploads.remove(id);
          aborted++;
        } catch (RocksDBException | IOException e) {
          throw failure(e);
        }
      }
    }

    @Override
    public void close() throws IOException {
      abort();
    }
  }

  private static byte[] encode(Version version) {
    return ByteBuffer.allocate(53)
        .put((byte) 1)
        .putLong(version.sequence())
        .putLong(version.size())
        .putInt(CHUNK_BYTES)
        .put(version.sha256())
        .array();
  }

  private static Version decode(UUID id, byte[] bytes) throws StorageException {
    if (bytes == null) throw new StorageException(NOT_FOUND, "Version not found");
    if (bytes.length != 53 || (bytes[0] != 1 && bytes[0] != 2))
      throw new StorageException(CORRUPT, "Invalid manifest encoding");
    ByteBuffer b = ByteBuffer.wrap(bytes);
    b.get();
    long sequence = b.getLong(), size = b.getLong();
    int chunk = b.getInt();
    byte[] hash = new byte[32];
    b.get(hash);
    if (sequence < 1 || size < 0 || size > MAX_OBJECT_BYTES || chunk != CHUNK_BYTES)
      throw new StorageException(CORRUPT, "Invalid manifest fields");
    return new Version(id, sequence, size, hash);
  }

  public synchronized Version head(String namespace, byte[] key, UUID version) throws IOException {
    open();
    objectKey(namespace, key);
    try {
      return decode(
          version, db.get(cf("manifests"), StorageKeys.manifest(namespace, key, version)));
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  /**
   * Preflight verifies the whole object before exposing any bytes; each emitted chunk is
   * reverified.
   */
  public synchronized Version read(String namespace, byte[] key, UUID version, OutputStream output)
      throws IOException {
    Version metadata = head(namespace, key, version);
    try {
      scan(metadata, OutputStream.nullOutputStream());
      scan(metadata, output);
      return metadata;
    } catch (StorageException e) {
      if (e.code() == CORRUPT) corruptions++;
      throw e;
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  private void scan(Version version, OutputStream output) throws IOException, RocksDBException {
    MessageDigest hash = digest();
    for (long offset = 0; offset < version.size(); offset += CHUNK_BYTES) {
      open();
      if (Thread.currentThread().isInterrupted())
        throw new StorageException(CANCELLED, "Read cancelled");
      byte[] chunk = db.get(cf("chunks"), StorageKeys.chunk(version.id(), offset));
      int expected = (int) Math.min(CHUNK_BYTES, version.size() - offset);
      if (chunk == null || chunk.length != 32 + expected)
        throw new StorageException(CORRUPT, "Missing or truncated chunk");
      MessageDigest part = digest();
      part.update(chunk, 32, expected);
      if (!MessageDigest.isEqual(part.digest(), Arrays.copyOf(chunk, 32)))
        throw new StorageException(CORRUPT, "Chunk checksum mismatch");
      hash.update(chunk, 32, expected);
      output.write(chunk, 32, expected);
    }
    if (!MessageDigest.isEqual(hash.digest(), version.sha256()))
      throw new StorageException(CORRUPT, "Whole-object checksum mismatch");
  }

  private void cleanup(UUID id) throws RocksDBException, IOException {
    // Never collect committed chunks, even if a future/corrupt staging record refers to them.
    byte[] stage = db.get(cf("staging"), StorageKeys.id(id));
    if (db.get(cf("requests"), StorageKeys.id(id)) != null
        || (stage != null && db.get(cf("manifests"), stage) != null))
      throw new StorageException(CORRUPT, "Staging overlaps a committed version");
    faults.at(Point.CLEANUP);
    try (WriteBatch batch = new WriteBatch();
        WriteOptions write = sync()) {
      batch.deleteRange(
          cf("chunks"), StorageKeys.chunk(id, 0), StorageKeys.chunk(id, Long.MAX_VALUE));
      batch.delete(cf("staging"), StorageKeys.id(id));
      db.write(write, batch);
    }
  }

  private void recover() throws RocksDBException, IOException {
    try (RocksIterator iterator = db.newIterator(cf("staging"))) {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        UUID id;
        try {
          id = StorageKeys.id(iterator.key());
        } catch (IllegalArgumentException e) {
          throw new StorageException(CORRUPT, "Invalid staging key", e);
        }
        cleanup(id);
        recovered++;
      }
      iterator.status();
    }
    verifyAll();
  }

  /** Startup and restore audit every committed version with one chunk-sized buffer. */
  public synchronized long verifyAll() throws IOException {
    open();
    long count = 0;
    long maximumSequence = 0;
    try {
      try (RocksIterator iterator = db.newIterator(cf("manifests"))) {
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
          byte[] key = iterator.key();
          UUID id = StorageKeys.manifestId(key);
          if (!Arrays.equals(key, db.get(cf("requests"), StorageKeys.id(id)))) {
            throw new StorageException(
                CORRUPT, "Manifest has missing or inconsistent committed index");
          }
          Version version = decode(id, iterator.value());
          maximumSequence = Math.max(maximumSequence, version.sequence());
          byte[] rawVector = db.get(cf("vectors"), StorageKeys.id(id));
          if ((iterator.value()[0] == 2) != (rawVector != null))
            throw new StorageException(CORRUPT, "Missing or unexpected causal metadata");
          if (rawVector != null) {
            VectorClock vector = checkedVector(rawVector);
            if (vector.get(node) > version.sequence())
              throw new StorageException(CORRUPT, "Counter behind causal version");
            List<Siblings.Entry> visible = heads(Arrays.copyOf(key, key.length - 16));
            Siblings.Entry entry = entry(version, vector);
            List<Siblings.Entry> withVersion = new ArrayList<>(visible);
            withVersion.add(entry);
            if (!Siblings.merge(withVersion).equals(visible))
              throw new StorageException(CORRUPT, "Causal head index inconsistent with history");
          }
          scan(version, OutputStream.nullOutputStream());
          count++;
        }
        iterator.status();
      }
      long indexed = 0;
      try (RocksIterator iterator = db.newIterator(cf("requests"))) {
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
          UUID id = StorageKeys.id(iterator.key());
          byte[] key = iterator.value();
          if (!id.equals(StorageKeys.manifestId(key)) || db.get(cf("manifests"), key) == null) {
            throw new StorageException(
                CORRUPT, "Committed index has missing or inconsistent manifest");
          }
          indexed++;
        }
        iterator.status();
      }
      try (RocksIterator iterator = db.newIterator(cf("vectors"))) {
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
          byte[] key = db.get(cf("requests"), iterator.key());
          byte[] manifest = key == null ? null : db.get(cf("manifests"), key);
          if (manifest == null || manifest[0] != 2)
            throw new StorageException(CORRUPT, "Orphan causal vector");
          checkedVector(iterator.value());
        }
        iterator.status();
      }
      try (RocksIterator iterator = db.newIterator(cf("heads"))) {
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
          UUID id = StorageKeys.manifestId(iterator.key());
          if (db.get(cf("manifests"), iterator.key()) == null
              || !Arrays.equals(iterator.value(), db.get(cf("vectors"), StorageKeys.id(id))))
            throw new StorageException(CORRUPT, "Orphan or inconsistent causal head");
        }
        iterator.status();
      }
      byte[] counterBytes = db.get(cf("counters"), COUNTER);
      if ((counterBytes == null && maximumSequence > 0)
          || (counterBytes != null
              && (counterBytes.length != 8
                  || ByteBuffer.wrap(counterBytes).getLong() < maximumSequence))) {
        throw new StorageException(CORRUPT, "Durable counter missing or behind committed versions");
      }
      if (count != indexed) throw new StorageException(CORRUPT, "Committed index count mismatch");
    } catch (RocksDBException e) {
      throw failure(e);
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new StorageException(CORRUPT, "Invalid committed metadata", e);
    }
    return count;
  }

  public synchronized void checkpoint(Path target) throws IOException {
    open();
    expire();
    if (!uploads.isEmpty())
      throw new StorageException(CAPACITY, "Finish or cancel uploads before checkpoint");
    Path absolute = target.toAbsolutePath().normalize();
    if (absolute.startsWith(directory) || directory.startsWith(absolute) || Files.exists(absolute))
      throw new StorageException(
          INVALID, "Checkpoint requires a new directory outside the database");
    verifyAll();
    try (Checkpoint checkpoint = Checkpoint.create(db)) {
      db.syncWal();
      checkpoint.createCheckpoint(absolute.toString());
    } catch (RocksDBException e) {
      throw failure(e);
    }
  }

  /**
   * Restore into a fresh path, validate identity and every object, then atomically publish the
   * directory.
   */
  public static void restore(Path checkpoint, Path destination, String identity)
      throws IOException {
    Path source = checkpoint.toRealPath(), target = destination.toAbsolutePath().normalize();
    if (Files.exists(target)
        || target.startsWith(source)
        || !Files.isRegularFile(source.resolve("CURRENT")))
      throw new StorageException(INVALID, "Restore requires a checkpoint and a fresh destination");
    Path parent = target.getParent();
    Files.createDirectories(parent);
    Path temporary = Files.createTempDirectory(parent, ".quorumfs-restore-");
    boolean moved = false;
    try {
      try (var files = Files.list(source)) {
        for (Path file : files.toList()) {
          if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS))
            throw new StorageException(INVALID, "Invalid checkpoint entry");
          Path copied = temporary.resolve(file.getFileName());
          Files.copy(file, copied);
          try (var channel = java.nio.channels.FileChannel.open(copied, StandardOpenOption.WRITE)) {
            channel.force(true);
          }
        }
      }
      try (ObjectStorage restored = new ObjectStorage(temporary, identity)) {
        restored.verifyAll();
      }
      try (var channel = java.nio.channels.FileChannel.open(temporary, StandardOpenOption.READ)) {
        channel.force(true);
      }
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
      moved = true;
      try (var channel = java.nio.channels.FileChannel.open(parent, StandardOpenOption.READ)) {
        channel.force(true);
      }
    } finally {
      if (!moved) {
        try (var files = Files.walk(temporary)) {
          for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(file);
        }
      }
    }
  }

  public synchronized Stats stats() {
    return new Stats(
        uploads.size(), committed, aborted, recovered, corruptions, ioFailures, failed);
  }

  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    uploads.clear();
    for (ColumnFamilyHandle handle : handles) handle.close();
    if (db != null) db.close();
    for (ColumnFamilyOptions cf : cfOptions) cf.close();
    options.close();
    buffers.close();
    cache.close();
  }
}
