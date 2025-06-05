#pragma once

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <filesystem>

// MergeLog is used for crash recovery during merge operations (compactions).
class MergeLog {
public:
    MergeLog(const std::filesystem::path& path);
    void addToRemove(const std::filesystem::path& file);
    void addToRegister(int levelId, const std::filesystem::path& file);
    void commit() const;
    void removeFiles();
    bool empty() const noexcept {
        return files_to_remove_.empty() && files_to_register_.empty();
    }
    const std::vector<std::filesystem::path>& filesToRemove() const;
    const std::unordered_map<int, std::vector<std::filesystem::path>>& filesToRegister() const;

private:
    std::filesystem::path path_;
    std::vector<std::filesystem::path> files_to_remove_;
    std::unordered_map<int, std::vector<std::filesystem::path>> files_to_register_;
};
