package io.quorumfs.storage;

import java.io.IOException;

/** Explicit local storage errors; IO_FAILURE may have an unknown commit outcome. */
public final class StorageException extends IOException {
  private static final long serialVersionUID = 1L;

  public enum Code {
    INVALID,
    CAPACITY,
    CANCELLED,
    NOT_FOUND,
    CORRUPT,
    IO_FAILURE,
    CLOSED
  }

  private final Code code;

  public StorageException(Code code, String message) {
    super(message);
    this.code = code;
  }

  public StorageException(Code code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public Code code() {
    return code;
  }
}
