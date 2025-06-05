#include "simplestorage.h"
#include "constants.h"
#include "levelzero.h"
#include "generallevel.h"
#include "sstfile.h"
#include "memtable.h"
#include "mergelog.h"

#include <format>
#include <unordered_set>
#include <unordered_map>
namespace {
    constexpr std::string_view level0_name = "level0";
    constexpr std::string_view levelN_prefix = "level";
    constexpr std::string_view merge_log_name = "merge_log.sstlog";
    constexpr std::string_view memtable_name = "memtable.vsst.tmp";
    constexpr std::string_view lock_file_name = ".lock";
    constexpr int GENERAL_LEVEL_COUNT = 1;
}
uint64_t SimpleStorage::sst_sequence_number = 0; 

SimpleStorage::SimpleStorage(const std::filesystem::path& data_dir, const Config& config)
    : manifest_(data_dir, config), data_dir_(data_dir),
    worker_thread_([this](std::stop_token st) { workerLoop(st); }), lock_file_(data_dir / lock_file_name){
    const auto& real_config = manifest_.getConfig();
    levels_.push_back(std::make_unique<MemTable>(real_config.memtable_size_bytes)); // First level is MemTable
    levels_.push_back(std::make_unique<LevelZero>(data_dir / level0_name, real_config.l0_max_files)); // Level 0 of the storage
    int non_general_level_count = static_cast<int>(levels_.size());
    for (int i = 0; i < GENERAL_LEVEL_COUNT; ++i) {
        levels_.push_back(std::make_unique<GeneralLevel>(data_dir / (levelN_prefix.data() + std::to_string(i + non_general_level_count - 1)),
            MAX_L1_SST_FILE_SIZE, std::numeric_limits<size_t>::max())); // Level 1
    }
    completeMerge();
    removeAllTemporaryFiles();
}

SimpleStorage::~SimpleStorage() {
    worker_thread_.request_stop();
    queue_cv_.notify_all();
}

std::optional<Entry> SimpleStorage::get(const std::string& key) const {
    std::shared_lock lock(readwrite_mutex_);
    for (const auto& level : levels_) {
        std::optional<Entry> entry = level->get(key);
        if (entry.has_value()) {
            if (entry.value().type != ValueType::REMOVED) {
                return entry;
            }
            else {
                return std::nullopt;
            }
        }
    }
    return std::nullopt;
}


bool SimpleStorage::remove(const std::string& key) {
    bool success;
    uint64_t seq_num;
    {
        std::lock_guard lock(readwrite_mutex_);
        success = memTable()->remove(key);
        if (success) {
            return true; 
        }
        seq_num = sst_sequence_number; // Get the current sequence number from MemTable
    }
    //failed to delene in memtable make async remove in Level 0 and higher
    std::lock_guard lock(queue_mutex_);
    task_queue_.push(RemoveSSTTask{ key });
    return false;
}


bool SimpleStorage::exists(const std::string& key) const {
    std::shared_lock lock(readwrite_mutex_);
    for (const auto& level : levels_) {
        auto status = level->status(key);
        switch (status) {
        case EntryStatus::EXISTS:
            return true;
        case EntryStatus::NOT_FOUND:
            continue;
        case EntryStatus::REMOVED:
            return false;
        }
    }
    return false;
}

std::vector<std::string> SimpleStorage::keysWithPrefix(const std::string& prefix, unsigned int max_results) const {
    std::shared_lock lock(readwrite_mutex_);
    std::vector<std::string> ret;
    ret.reserve(max_results);
    std::unordered_set<std::string> seen;
    for (const auto& level : levels_) {
        auto keys = level->keysWithPrefix(prefix, max_results - ret.size());
        for (auto& key : keys) {
            if (seen.insert(key).second) {
                ret.push_back(std::move(key));
                if (ret.size() >= max_results)
                    break;
            }
        }
    }
    return ret;
}


void SimpleStorage::flush() {
    std::lock_guard readwrite_lock(readwrite_mutex_);
    if (memTable()->count() != 0) {
        flushImpl();
    }
}

void SimpleStorage::putImpl(const std::string& key, const Entry& entry, uint64_t expiration_ms) {
    std::lock_guard lock(readwrite_mutex_);
    auto* memtable = memTable();
    memtable->put(key, entry, expiration_ms);
    if (memtable->full()) {
        flushImpl();
    }
}

void SimpleStorage::flushImpl() {
    std::vector<SSTFile> ssts;
    ssts.push_back(SSTFile::writeAndCreate(data_dir_ / std::filesystem::path(memtable_name),
        manifest_.getConfig().block_size,
        ++sst_sequence_number,
        memTable()->begin(), memTable()->end()));
    auto* l = static_cast<IFileLevel*>(levels_[1].get());
    l->addSST(std::move(ssts));
    memTable()->clear();
    mergeAsync(1, l->maxSeqNum()); // Merge the MemTable into Level 0
}

void SimpleStorage::completeMerge() {
    std::lock_guard lock(readwrite_mutex_);
    MergeLog merge_log(data_dir_ / merge_log_name);
    for (const auto& [level, sst_paths] : merge_log.filesToRegister()) {
        std::vector<SSTFile> to_merge;
        auto* level_ptr = static_cast<IFileLevel*>(levels_[level].get());
        for (const auto& sst_path : sst_paths) {
            to_merge.push_back(SSTFile::readAndCreate(sst_path));
        }
        level_ptr->addSST(std::move(to_merge));
    }
    merge_log.removeFiles(); // Remove the merge log file after processing
}

void SimpleStorage::removeAllTemporaryFiles() {
    std::scoped_lock lock(readwrite_mutex_);
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".tmp") {
            std::filesystem::remove(entry.path());
        }
    }
}

void SimpleStorage::mergeAsync(int level, uint64_t maxSeqNum) {
    std::lock_guard lock(queue_mutex_);
    task_queue_.push(MergeTask{ level, maxSeqNum });
    queue_cv_.notify_one();
}

MemTable* SimpleStorage::memTable() {
    return static_cast<MemTable*>(levels_[0].get());
}

void SimpleStorage::workerLoop(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        StorageTask task;
        {
            std::unique_lock lock(queue_mutex_);
            queue_cv_.wait(lock, [this, &stop_token] {
                return !task_queue_.empty() || stop_token.stop_requested();
                });
            if (stop_token.stop_requested()) {
                return;
            }
            if (task_queue_.empty()) {
                continue;
            }
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }

        std::visit([this](auto&& t) {
            using T = std::decay_t<decltype(t)>;
            if constexpr (std::is_same_v<T, MergeTask>) {
                handleMergeTask(t);
            }
            else if constexpr (std::is_same_v<T, RemoveSSTTask>) {
                handleRemoveSST(t);
            }
            else if constexpr (std::is_same_v<T, ShrinkTask>) {
                handleShrink(t);
            }
            }, task);
    }
}

void SimpleStorage::handleMergeTask(const MergeTask& t) {
    if (t.level <= 0 || t.level >= static_cast<int>(levels_.size()) - 1) {
        return; //We don't merge MemTable and the last level
    }
    int dst_level = t.level + 1;
    auto* level = static_cast<IFileLevel*>(levels_[t.level].get());
    auto* next_level = static_cast<IFileLevel*>(levels_[dst_level].get());
    std::vector<std::filesystem::path> files_to_merge;
    {
        std::shared_lock lock(readwrite_mutex_);
        files_to_merge = level->filelistToMerge(t.seq_num);
    }
    if (files_to_merge.empty()) {
        return; // Nothing to merge
    }
    //use merge log to complete merge in case of abnormal termination
    MergeLog merge_log(data_dir_/merge_log_name);
    uint64_t seq_num = 0;
    for(const auto& sst_path: files_to_merge) {
        auto merge_result = next_level->mergeToTmp(sst_path, manifest_.getConfig().block_size);
        merge_log.addToRemove(sst_path);
        for (const auto& sst : merge_result.new_files) {
            merge_log.addToRegister(dst_level, sst.path());
        }
        for (const auto& sst_path : merge_result.files_to_remove) {
            merge_log.addToRemove(sst_path);
        }
        merge_log.commit();
        {
            std::lock_guard lock(readwrite_mutex_);
            next_level->removeSSTs(merge_result.files_to_remove); // Remove merged SST file from the next level
            next_level->addSST(std::move(merge_result.new_files));
            level->removeSSTs({ sst_path });
            seq_num = level->maxSeqNum(); // Get the maximum sequence number after merging
        }
        merge_log.removeFiles();
    }
    if (dst_level < levels_.size()  - 1) {
        mergeAsync(dst_level, seq_num); // Schedule the next level merge if needed
    }
}

void SimpleStorage::handleRemoveSST(const RemoveSSTTask& t) {
    std::lock_guard lock(readwrite_mutex_);
    for (const auto& level : levels_) {
        if (static_cast<IFileLevel*>(level.get())->remove(t.key, t.seq_num)) {
            return;
        }
    }
}

void SimpleStorage::handleShrink(const ShrinkTask&) {

}


void SimpleStorage::shrink() {
    std::lock_guard lock(queue_mutex_);
    task_queue_.push(ShrinkTask{});
    queue_cv_.notify_one();
}