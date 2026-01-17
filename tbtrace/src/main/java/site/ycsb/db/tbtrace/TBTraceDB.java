package site.ycsb.db.tbtrace;

import site.ycsb.*;
import site.ycsb.ByteIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A YCSB DB binding that writes each operation to a trace file.
 * READ -> GET, UPDATE/INSERT/DELETE -> SET (or tombstone).
 */
public class TBTraceDB extends DB {
  // -------- Properties (fully parameterized) --------
  // Required:
  //   tbtrace.file=<path/to/trace.txt>
  //
  // Optional:
  //   tbtrace.keyprefix=user
  //   tbtrace.value.bytes=4096
  //   tbtrace.value.encoding=hex   (hex | base64)
  //   tbtrace.value.seed=1337
  //   tbtrace.opcase=upper         (upper | lower)
  //   tbtrace.flush.every=10000
  //   tbtrace.threads.require1=true   (recommended for deterministic single-file ordering)

  private BufferedWriter out;
  private String file;
  private String keyPrefix;
  private int valueBytes;
  private String encoding;
  private long seed;
  private String opCase;
  private int flushEvery;
  private boolean requireSingleThread;

  private final AtomicLong lineCount = new AtomicLong(0);

  @Override
  public void init() throws DBException {
    Properties p = getProperties();
    file = must(p, "tbtrace.file");

    keyPrefix = p.getProperty("tbtrace.keyprefix", "user");
    valueBytes = Integer.parseInt(p.getProperty("tbtrace.value.bytes", "4096"));
    encoding = p.getProperty("tbtrace.value.encoding", "hex").trim().toLowerCase(Locale.ROOT);
    seed = Long.parseLong(p.getProperty("tbtrace.value.seed", "1337"));
    opCase = p.getProperty("tbtrace.opcase", "upper").trim().toLowerCase(Locale.ROOT);
    flushEvery = Integer.parseInt(p.getProperty("tbtrace.flush.every", "10000"));
    requireSingleThread = Boolean.parseBoolean(p.getProperty("tbtrace.threads.require1", "true"));

    if (requireSingleThread) {
      // YCSB doesn't expose "threads" directly here reliably, so enforce in your run command.
      // This flag is here to make the intent explicit.
    }

    try {
      File f = new File(file);
      File parent = f.getParentFile();
      if (parent != null) {
        parent.mkdirs();
      }
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, false), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new DBException("Failed to open tbtrace.file=" + file, e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (out != null) {
        out.flush();
        out.close();
      }
    } catch (IOException e) {
      throw new DBException("Failed to close trace file", e);
    }
  }

  // -------- Mapping to Treebeard trace grammar --------
  // READ -> GET <key>
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    writeLine(fmtOp("GET") + " " + normalizeKey(key));
    return Status.OK;
  }

  // INSERT/UPDATE -> SET <key> <valueToken>
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    writeLine(fmtOp("SET") + " " + normalizeKey(key) + " " + valueTokenFor(key, "insert"));
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    writeLine(fmtOp("SET") + " " + normalizeKey(key) + " " + valueTokenFor(key, "update"));
    return Status.OK;
  }

  // Optional ops (if your workloads include them). You can:
  // - map DELETE to SET <key> <tombstone>, or
  // - reject them to avoid surprising traces.
  @Override
  public Status delete(String table, String key) {
    // Safer: encode as a SET tombstone token (Treebeard only knows GET/SET).
    writeLine(fmtOp("SET") + " " + normalizeKey(key) + " " + "TOMBSTONE");
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // Treebeard trace grammar doesn't have SCAN. Fail loudly so you don't get "incorrect" traces.
    return Status.NOT_IMPLEMENTED;
  }

  // -------- Helpers --------
  private void writeLine(String s) {
    try {
      out.write(s);
      out.newLine();
      long n = lineCount.incrementAndGet();
      if (flushEvery > 0 && (n % flushEvery == 0)) {
        out.flush();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed writing trace line", e);
    }
  }

  private String normalizeKey(String key) {
    // Many YCSB workloads already produce keys like "user12345".
    // If they produce numeric keys, you can prefix them deterministically.
    if (key.startsWith(keyPrefix)) {
      return key;
    }
    return keyPrefix + key;
  }

  private String fmtOp(String op) {
    if ("lower".equals(opCase)) {
      return op.toLowerCase(Locale.ROOT);
    }
    return op.toUpperCase(Locale.ROOT);
  }

  private String valueTokenFor(String key, String kind) {
    // Deterministic value: hash(seed || kind || key) expanded to valueBytes, encoded as one whitespace-free token.
    byte[] raw = deterministicBytes(seed, kind + ":" + key, valueBytes);
    if ("base64".equals(encoding)) {
      return Base64.getEncoder().withoutPadding().encodeToString(raw);
    }
    // default hex
    return toHex(raw);
  }

  private static byte[] deterministicBytes(long seed, String msg, int n) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      ByteArrayOutputStream baos = new ByteArrayOutputStream(n);

      long counter = 0;
      while (baos.size() < n) {
        sha.update(longToBytes(seed));
        sha.update(msg.getBytes(StandardCharsets.UTF_8));
        sha.update(longToBytes(counter++));
        byte[] block = sha.digest();
        int need = Math.min(block.length, n - baos.size());
        baos.write(block, 0, need);
      }
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] longToBytes(long x) {
    return new byte[] {
        (byte)(x >>> 56), (byte)(x >>> 48), (byte)(x >>> 40), (byte)(x >>> 32),
        (byte)(x >>> 24), (byte)(x >>> 16), (byte)(x >>> 8),  (byte)(x)
    };
  }

  private static String toHex(byte[] b) {
    char[] hex = "0123456789abcdef".toCharArray();
    char[] out = new char[b.length * 2];
    for (int i = 0; i < b.length; i++) {
      int v = b[i] & 0xFF;
      out[i * 2] = hex[v >>> 4];
      out[i * 2 + 1] = hex[v & 0x0F];
    }
    return new String(out);
  }

  private static String must(Properties p, String k) {
    String v = p.getProperty(k);
    if (v == null || v.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing property: " + k);
    }
    return v.trim();
  }
}