#include "memtable.h"
#include "constants.h"
#include "utils.h"
MemTable::MemTable(size_t max_size_bytes)
    : max_size_bytes_(max_size_bytes) {
    //aproximate initial size, to know exact size we need to know datablock size, but we don't want MemTable to manage it.
    current_size_bytes_ = sst::header::SST_HEADER_SIZE + sst::indexblock::BLOCK_OFFSET_SIZE + sst::indexblock::INDEX_KEY_LEN;
}

void MemTable::put(const std::string& key, const Entry& entry, uint64_t expiration_ms) {
    auto [_, inserted] = data_.insert_or_assign(key, MemEntry{entry, expiration_ms});
    if (inserted) {
        //KeyLengh + key + Expiration + ValueType + ValueLength (optional) + value + offset
        current_size_bytes_ += Utils::onDiskEntrySize(key, entry.value);
    }
}

std::optional<Entry> MemTable::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end())
        return std::nullopt;
    if (isExpired(it->second)) {
        return Entry{ ValueType::REMOVED, {} };
    }
    return it->second.entry;
}

EntryStatus MemTable::status(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end())
        return EntryStatus::NOT_FOUND;
    if (isExpired(it->second)) {
        return EntryStatus::REMOVED;
    }
    return EntryStatus::EXISTS;
}

std::vector<std::string> MemTable::keysWithPrefix(const std::string& prefix, unsigned int max_results) const {
    std::vector<std::string> result;
    result.reserve(std::min(static_cast<size_t>(max_results), data_.size()));

    for (auto it = data_.lower_bound(prefix);
        it != data_.end() && result.size() < static_cast<size_t>(max_results); ++it) {
        const auto& key = it->first;
        if (key.compare(0, prefix.size(), prefix) != 0) {
            break;
        }
        if (!isExpired(it->second) && it->second.entry.type != ValueType::REMOVED) {
            result.push_back(key);
        }
    }
    return result;
}

bool MemTable::forEachKeyWithPrefix(const std::string& prefix, const std::function<bool(const std::string&)>& callback) const {
    for (auto it = data_.lower_bound(prefix); it != data_.end(); ++it) {
        const auto& key = it->first;
        if (key.compare(0, prefix.size(), prefix) != 0) {
            return true; 
        }
        if (!isExpired(it->second) && it->second.entry.type != ValueType::REMOVED) {
            if (!callback(key)) {
                return false; // Stop iterating if callback returns false
            }
        }
    }
    return true;
}

bool MemTable::remove(const std::string& key) {
    auto it = data_.find(key);
    if (it != data_.end()) {
        it->second.expiration_ms = sst::datablock::EXPIRATION_DELETED;
        it->second.entry.type = ValueType::REMOVED;
        return true;
    }
    return false;
}


bool MemTable::full() const noexcept {
    return current_size_bytes_ >= max_size_bytes_;
}

size_t MemTable::count() const noexcept(noexcept(data_.size())) {
    return data_.size();
}

void MemTable::clear() noexcept(noexcept(data_.clear()))  {
    data_.clear();
    current_size_bytes_ = 0;
}

bool MemTable::isExpired(const MemEntry& entry) const {
    return Utils::isExpired(entry.expiration_ms);
}
