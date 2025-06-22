#pragma once
#include <map>
#include <string>
#include <optional>
#include <functional>
#include <chrono>
#include "types.h"
#include "ilevel.h"
#include "skiplist.h"
struct MemEntry {
    Entry entry;
    uint64_t expiration_ms = std::numeric_limits<uint64_t>::max();
};

class MemTable : public ILevel {
public:
    explicit MemTable(size_t max_size_bytes);

    void put(const std::string& key, const Entry& entry, uint64_t expiration_ms);
    std::optional<Entry> get(const std::string& key) const override;
    bool remove(const std::string& key);
    EntryStatus status(const std::string& key) const override;
    std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results) const override;
    bool forEachKeyWithPrefix(const std::string& prefix, const std::function<bool(const std::string&)>& callback) const override;

    auto begin() const noexcept(noexcept(data_.begin())) {
        return data_.begin();
    }
    auto end() const noexcept(noexcept(data_.end())) {
        return data_.end();
    }
    bool full() const noexcept;
    size_t count() const noexcept(noexcept(data_.size()));

private:
    size_t max_size_bytes_;
    std::atomic<size_t> current_size_bytes_{ 0 };
    SkipList<std::string, MemEntry> data_;

    bool isExpired(const MemEntry& entry) const;
};
