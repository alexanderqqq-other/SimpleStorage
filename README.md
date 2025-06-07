# Simple Storage File Format Specification

This library uses LSM (Log-Structured Merge-tree) storage format with SST (Sorted String Table) files as a main storage unit.

## Storage folder structure

```
manifest.json
.lock
data/
  level0/
	L0_{seq_num}_.vsst
	L0_{seq_num}.vsst
	...
	L0_{seq_num}.vsst
  level1/
    lgeneral_{seq_num}_{unique_level_num}.vsst
	...
```

## Levels Overview

### Memtable

- Default size: **64MB** (Can be configured: **8MB - 512MB**)

### Level 0 (L0)

- Maximum number of files: **4** (Can be configured: **2 - 16**)
- File size: **see memtable**

### Level 1 (L1)

- File size: **< 2GB**

## SST File Structure

```
[Header]
[DataBlock0]
[DataBlock1]
...
[DataBlockN-1]
[IndexBlock]
```

---

### Header

| Field           | Size     | Description                                  |
| ---------       | -------- | -------------------------------------------- |
| Signature       | 4 bytes  | Signature, "VSSF" (very simple storage file) |
| Version         | 1 byte   | File format version                          |
| Sequence Number | 8 bytes  | Globaly incremented sequence number of the file |

---

### DataBlock (N per file, configurable <= 32-256 KB each)

```
[Entries][OffsetTable][Count]
```

- **Entries:** Sequential serialized records:

  - KeyLen (2 bytes)
  - Key (KeyLen bytes, UTF-8, maximum 1024)
  - Expiration (8 byte, timestamp, optional, 0 if not set, 1 means entry is deleted)
  - ValueType (1 byte)

  depending on type:

  - Value (fixed size)

    or

  - ValueLen (4 bytes)
  - Value (ValueLen bytes)

- **OffsetTable:** Array of `uint32_t`, N elements (offsets of each record from the start of the block)
- **Count:** `uint32_t`, number of records in the block

---

### IndexBlock

Array of entries, one per DataBlock:

```
[Entries][Size]
```

- **Entries:**

| Field  | Size         | Description                            |
| ------ | ------------ | -------------------------------------- |
| KeyLen | 2 bytes      | Length of the minimal key in DataBlock |
| Key    | KeyLen bytes | Minimal key of the block (UTF-8)       |
| Offset | 8 bytes      | Offset of the DataBlock in the file    |

Number of entries: same as number of DataBlocks in the file. Entries are sorted by Key

- **Size:** uint32_t, size of the IndexBlock in bytes

---

## Limitations

- Key length: **<= 1024 bytes**
- Key length + Value length: **<= Block_size - Block_metadata size**

### Notes

- All data is serialized in little-endian order.
- Keys should be UTF-8 encoded strings, but library does not enforce this, the keys are treated as raw byte sequence.
- values might be integrals of 32bit and 64bit, floating numbers of 32 and 64bit and raw bytes, includeing UTF-8 strings.

---

# Simple Storage API

## Core Operations

The library provides the following minimal set of operations for working with the key-value store:

### constructor

Open an existing or create a new storage in the specified directory.
Throws an exception if the storage is locked already or cannot be created.
Save config for created storage, if storage exists ignore parameter and load config from the disk.

Only one process can work with the storage. It is guaranteed by .lock file.

**Parameters**:

- path to the storage directory.
- configuration options - ignored if storage exists.

### put

Insert or update a value by key, with an optional time-to-live (TTL) for automatic deletion.
Throws an exception if error occurs during insertion.
**Parameters**:

- key: UTF-8 string, maximum length 1024 bytes.
- value: fixed-size integral types, float, double, unicode string, arbitrary sequence of bytes.
  value + key length must not exceed block size minus 7 bytes.

### get

Retrieve the value by key. Returns an optional value, which is empty if the key does not exist or has been deleted.

**Return**:

- `std::optional<Entry>`: the value associated with the key, or std::nullopt if not found or expired.

```
using Value = std::variant<
    uint8_t,
    int8_t,
    uint16_t,
    int16_t,
    uint32_t,
    int32_t,
    uint64_t,
    int64_t,
    float,
    double,
    std::string,
    std::u8string,
    std::vector<uint8_t>
>;

enum class ValueType : sst::datablock::ValueTypeFieldType {
    UINT8 = 0,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    U8STRING,
    BLOB,
    REMOVED = std::numeric_limits<sst::datablock::ValueTypeFieldType>::max(),
};

struct Entry {
    ValueType type;
    Value value;
};
```

### keysWithPrefix

Retrieve a list of keys that start with the specified prefix. Optionally, limit the number of results returned.
**Parameters**:

- prefix: UTF-8 string, maximum length 65535 bytes.
- max_results: maximum number of results to return (default is 1000).

**Return**:

- `std::vector<std::string>`: a vector of keys that match the prefix.

### remove

Logically delete a value by key. If the key does not exist, it does nothing. Does not delete the data, just marks it as deleted.

### exists

Check if a key exists in the storage. Returns true if the key is found, false otherwise.

**Parameters**:

- key: UTF-8 string, maximum length 1024 bytes.

**Return**:

- `bool`: true if the key exists, `false`` otherwise.

### flush

Force writing of all buffered data to disk. This ensures that all changes made to the storage are persisted.

### shrink

Shrink the storage by removing all the keys marked as deleted and compacting the data. This operation is optional and can be used to optimize storage space.

---

## Example Interface

```cpp
// Open or create storage
auto db = SimpleStorage(const std::string& path);

// Insert value with optional TTL (time-to-live seconds)
bool put(const std::string& key, const std::vector<uint8_t>& value, std::optional<uint64_t> ttl = std::nullopt);

// Get value
std::optional<Entry> get(const std::string& key);

// Remove value
bool remove(const std::string& key);

// Check if key exists
bool exists(const std::string& key);

// Prefix search (optionally limit results)
std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results = 1000);

// Force flush all data to disk
void flush();

// Shrink storage to remove deleted keys 
void shrink();

// Wait until all background tasks are finished
void waitAllAsync();
```
---
# Thread Safety and Synchronization


The key-value storage is **thread-safe** as long as **a single instance of `SimpleStorage` is used per storage path**. If multiple instances are created for the same path, **thread safety is not guaranteed** and data corruption may occur.

### Core Synchronization Mechanisms

The library uses the following primitives for internal synchronization:

* **One `std::shared_mutex`** (`readwrite_mutex_`) for concurrent access control. Protect all in-memory changes.
* **One asynchronous worker thread with a `std::queue` and condition variable**, used for background tasks (e.g., merge, shrink, deferred remove).
The queue guaranties that only one thread is performing any file operation. The only exception is flush() operation that can be procesed safely without
queue because it only creates new file with its unique id wich will be ignored by all the async tasks sheduled earlier. File-rename operations are fast and involves in-memroty chages, so they processed under readwrite_mutex_ 

### Fast Operations

The following operations are considered **fast** and acquire **locks briefly**:

* `put`
* `flush`
* `readWithPrefix`

These operations use the shared `readwrite_mutex_`:

* **`put`, `flush`** acquire an **exclusive lock**.
* **`get`, `keysWithPrefix`** acquire a **shared lock**.

### Internal Behavior of Operations

#### `put`

* Writes data to `MemTable` under exclusive lock.
* May trigger an automatic `flush()` if `MemTable` becomes full.

#### `flush`

* Transfers entries from `MemTable` to disk under exclusive lock.
* May trigger an **asynchronous `merge`** to deeper levels via the task queue.

#### `merge` (Asynchronous)

* Heavy data processing (reading immutable files, generating temporary files) is done **without any locks**.
* Final steps—**file renaming and registration**—are protected with **exclusive lock**.

#### `shrink` (Asynchronous)

* Similar to `merge`: performs file compaction off the lock.
* Acquires **exclusive lock** only briefly for atomic renaming and cleanup.
* Can be configured to run periodically using Config::shrink_timer_minutes.
By default value is 0, which means shrink timer is disabled.

#### `remove`, `removeAsync`
* remove just add or overwite remove record in MemTable under under **exclusive lock**.
* removeAsync tries to remove the key directly from `MemTable` under **exclusive lock**.
* If the key is not found, it defers a background task to **mark the key as `REMOVED`** in SST files using:

  * Asynchronous queue.
  * **Exclusive lock** for fast in-place mark.
  * 
### waitAllAsync

Block the current thread until all queued background tasks are processed.

---

## Operation Synchronization Table

| Operation           | Lock Type                | Additional Notes                                |
| ------------------- | ------------------------ | ----------------------------------------------- |
| `get`               | `shared_lock`            | Reads only, fast and parallelizable             |
| `keysWithPrefix`    | `shared_lock`            | Reads only, optimized for prefix scans          |
| `put`               | `exclusive_lock`         | May trigger `flush()`                           |
| `flush()`           | `exclusive_lock`         | May schedule async `merge()`                    |
| `remove`            | `exclusive_lock`         | Add remove record             |
| `removeAsync`       | Queue + `exclusive_lock` | Marks key as `REMOVED` in SST files             |
| `merge`             | Queue + `exclusive_lock` | Heavy part async, lock held for rename/register |
| `shrink`            | Queue + `exclusive_lock` | Similar to `merge`, lock only for final step    |


---
# Building and Running tests
```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
ctest 
(optional)
ctest -V -L perf //performance tests only
```

environment variables for performance test:
PERF_TOTAL_SIZE_MB = 100 # Set total size in MB for performance tests, default is 100MB
PERF_BLOCK_SIZE_KB = 32 # Set block size in KB for performance tests, default is 32KB
PERF_THREADS = 8 # Set number of threads for performance tests, default is 8
PERF_MEMTABLE_SIZE_MB = 64 # Set memtable size in MB for performance tests, default is 64MB

Test use random pseudo-random data, uncluding huge BLOBs with size ~10kb. So minimum recommended block size is 16kb


---
**License:** [MIT](LICENSE)
