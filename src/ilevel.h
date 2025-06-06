#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include "types.h"
#include "sstfile.h"

// Abstract interface for storage levels (Level 0, Level N, etc.)
class ILevel {
public:
    virtual ~ILevel() = default;
    virtual std::optional<Entry> get(const std::string& key) const = 0;
    virtual EntryStatus status(const std::string& key) const = 0;
    virtual std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results) const = 0;

};

class IFileLevel: public ILevel {
public:
    struct MergeResult {
        std::vector<std::unique_ptr<SSTFile>>  new_files;
        std::vector<std::filesystem::path> files_to_remove;
    };
    virtual ~IFileLevel() = default;
    virtual bool remove(const std::string& key, uint64_t max_seq_num) = 0;
    virtual std::vector<std::filesystem::path> filelistToMerge(uint64_t max_seq_num) const = 0;
    virtual MergeResult mergeToTmp(const std::filesystem::path&, size_t datablock_size) const = 0;
    virtual void addSST(std::vector<std::unique_ptr<SSTFile>>  sst) = 0;
    virtual void removeSSTs(const std::vector<std::filesystem::path>& sst_paths) = 0;
    virtual uint64_t maxSeqNum() const = 0;
    virtual void clearCache() noexcept = 0;
    virtual size_t count() const = 0;
};
