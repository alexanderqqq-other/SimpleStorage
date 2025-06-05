# Simple Storage File Format Specification

This library uses LSM (Log-Structured Merge-tree) storage format with SST (Sorted String Table) files as a main storage unit.

## Storage folder structure

```
manifest.json
.lock
.WAL (optional, Write-Ahead Log for durability)
data/
  L0/
	dat0.vsst
	dat1.vsst
	...
	datN.vsst
  L1/
	...
```

## Levels Overview

### Memtable

- Default size: **64MB** (Can be configured: **8MB � 512MB**)

### Level 0 (L0)

- Maximum number of files: **4** (Can be configured: **2 � 16**)
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
- Key lenght + Value length: **<= Block_size - Block_metadata size**

### Notes

- All data is serialized in little-endian order.
- Keys should be UTF-8 encoded strings, but library does not enforce this, the keys are treated as raw byte sequence.
- values might be integrals of 32bit and 64bit, floating numbers of 32 and 64bit and raw bytes, includeing UTF-8 strings.

---

# Simple Storage API

## Core Operations

The library provides the following minimal set of operations for working with the key-value store:

### open (static)

Open an existing or create a new storage in the specified directory.
Throws an exception if the storage is locked already or cannot be created.

**Parameters**:

- path to the storage directory.
- configuration options - ignored if storage exists.

**Return**:

- `std::shared_ptr<Storage>`: a shared pointer to the storage instance.

### put

Insert or update a value by key, with an optional time-to-live (TTL) for automatic deletion.
Throws an exception if error occurs during insertion.
**Parameters**:

- key: UTF-8 string, maximum length 65535 bytes.
- value: fixed-size integral types, float, double, unicode string, arbitrary sequence of bytes.
  value + key length must not exceed block size minus 7 bytes.

### get

Retrieve the value by key. Returns an optional value, which is empty if the key does not exist or has been deleted.

**Return**:

- `std::optional<Entry>`: the value associated with the key, or std::nullopt if not found or expired.

```
 using Value = std::variant<
    uint32_t,
    int32_t,
    uint64_t,
    int64_t,
    float,
    double,
    std::string,
    std::vector<uint8_t>
    >;


  enum class ValueType : uint8_t {
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    BLOB
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

### delete

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
auto db = SimpleStorage::open(const std::string& path);

// Insert value with optional TTL (time-to-live seconds)
bool put(const std::string& key, const std::vector<uint8_t>& value, std::optional<uint64_t> ttl = std::nullopt);

// Get value
std::optional<Entry> get(const std::string& key);

// Delete value
bool delete(const std::string& key);

// Check if key exists
bool exists(const std::string& key);

// Prefix search (optionally limit results)
std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results = 1000);

// Force flush all data to disk
void flush();

// Shrink storage to remove deleted keys and compact data
void shrink();
```

---
# Building and Running tests
```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
ctest 
```


---
**License:** [MIT](LICENSE)
