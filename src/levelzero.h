#pragma once

#include "ilevel.h"

#include <filesystem>
// Implementation of Level 0. Key ranges may overlap.
class LevelZero : public IFileLevel {
public:
    LevelZero(const std::filesystem::path& path, size_t max_num_files);
    ~LevelZero() override = default;
    std::optional<Entry> get(const std::string& key) const override;
    bool remove(const std::string& key, uint64_t max_seq_num) override;
    EntryStatus status(const std::string& key) const override;
    std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results) const override;
    std::vector<std::filesystem::path> filelistToMerge(uint64_t max_seq_num) const override;
    MergeResult mergeToTmp(const std::filesystem::path&, size_t datablock_size) const override;
    void addSST(std::vector<SSTFile> sst) override;
    void removeSSTs(const std::vector<std::filesystem::path>& sst_paths) override;
    uint64_t maxSeqNum() const override {
        return sst_files_.empty() ? 0 : sst_files_.back().seqNum();
    }

private:
    std::filesystem::path path_;
    size_t max_num_files_;
    std::vector<SSTFile> sst_files_;
};
