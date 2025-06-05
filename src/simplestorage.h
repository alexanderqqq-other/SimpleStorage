#pragma once
#include "types.h"
#include "sstfile.h"
#include "manifest.h"
#include "ilevel.h"
#include "utils.h"
#include "lockfile.h"

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <queue>
#include <thread>


class MemTable;

struct MergeTask {
    int level;
    uint64_t seq_num;
};
struct RemoveSSTTask {
    std::string key;
    uint64_t seq_num;
};

struct ShrinkTask {
};

using StorageTask = std::variant<MergeTask, RemoveSSTTask, ShrinkTask>;

class SimpleStorage {
public:
    SimpleStorage(const std::filesystem::path&, const Config& config);
    SimpleStorage(const SimpleStorage&) = delete;
    SimpleStorage& operator=(const SimpleStorage&) = delete;
    SimpleStorage(SimpleStorage&&) = delete;
    SimpleStorage& operator=(SimpleStorage&&) = delete;
    ~SimpleStorage();
    template <AllSupportedTypes T>
    void put(const std::string& key, const T& value, std::optional<uint32_t> ttl_seconds = std::nullopt) {
        if (key.empty()) {
            throw std::invalid_argument("Key cannot be empty");
        } 
        if (key.size() > sst::datablock::MAX_KEY_LENGTH) {
            throw std::invalid_argument("Key size exceeds maximum allowed size");
        }
        if (Utils::onDiskEntrySize(key, value) + sst::datablock::DATABLOCK_COUNT_SIZE > config_.block_size) {
            throw std::invalid_argument("Entry size exceeds maximum allowed size");
        }
        uint64_t expiration_ms = ttl_seconds.has_value() ?
            Utils::getNow() + ttl_seconds.value() * 1000ull :
            sst::datablock::EXPIRATION_NOT_SET;
        putImpl(key, Entry{ valueTypeFromType<T>(), value }, expiration_ms);
    }

    std::optional<Entry> get(const std::string& key) const;
    bool remove(const std::string& key);
    bool exists(const std::string& key) const;

    std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results = 1000) const;

    void flush();
    void shrink();

private:
    void putImpl(const std::string& key, const Entry& entry, uint64_t ttl);
    void flushImpl();
    void completeMerge();
    void removeAllTemporaryFiles();
    void mergeAsync(int level, uint64_t maxSeqNum);
    MemTable* memTable();
    void workerLoop(std::stop_token stop_token);
    void handleMergeTask(const MergeTask&);
    void handleRemoveSST(const RemoveSSTTask&);
    void handleShrink(const ShrinkTask&);
    Config config_;
    std::vector<std::unique_ptr<ILevel>> levels_;
    Manifest manifest_;
    std::filesystem::path data_dir_;
    mutable std::shared_mutex readwrite_mutex_; 
    mutable std::mutex queue_mutex_; 
    std::condition_variable queue_cv_;
    std::queue<StorageTask> task_queue_;
    std::jthread worker_thread_;
    StorageLockFile lock_file_;

    static uint64_t sst_sequence_number;

    // std::unique_ptr<WAL> wal_;
};

