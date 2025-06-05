#include "MergeLog.h"
#include <fstream>
#include <stdexcept>
#include <nlohmann/json.hpp>


MergeLog::MergeLog(const std::filesystem::path& path) : path_(path) {
    std::ifstream in(path);
    if (!in) {
        return;
    }

    nlohmann::json j;
    in >> j;

    if (j.contains("files_to_remove")) {
        for (const auto& val : j["files_to_remove"]) {
            files_to_remove_.emplace_back(val.get<std::string>());
        }
    }

    if (j.contains("files_to_register")) {
        for (auto& [level_str, arr] : j["files_to_register"].items()) {
            int level = std::stoi(level_str);
            for (const auto& val : arr) {
                files_to_register_[level].emplace_back(val.get<std::string>());
            }
        }
    }
}

void MergeLog::addToRemove(const std::filesystem::path& path) {
    files_to_remove_.push_back(path);
}

void MergeLog::addToRegister(int levelId, const std::filesystem::path& path) {
    files_to_register_[levelId].push_back(path);
}

void MergeLog::commit() const {
    nlohmann::json j;

    j["files_to_remove"] = nlohmann::json::array();
    for (const auto& f : files_to_remove_)
        j["files_to_remove"].push_back(f.string());

    for (const auto& [level, vec] : files_to_register_) {
        for (const auto& f : vec) {
            j["files_to_register"][std::to_string(level)].push_back(f.string());
        }
    }

    auto tmp_path = path_;
    tmp_path += ".tmp";
    std::filesystem::remove(tmp_path); // Ensure the temp file does not exist before writing
    {
        std::ofstream out(tmp_path, std::ios::trunc);
        if (!out) throw std::runtime_error("Failed to write merge log: " + tmp_path.string());
        out << j.dump(2);
        out.flush();
        out.close();
    }
    std::filesystem::rename(tmp_path, path_);
}

void MergeLog::removeFiles() {
    for (const auto& path : files_to_remove_) {
        if (std::filesystem::exists(path)) {
            std::filesystem::remove(path);
        }
    }
    std::filesystem::remove(path_);
    files_to_remove_.clear();
    files_to_register_.clear();

}

const std::vector<std::filesystem::path>& MergeLog::filesToRemove() const {
    return files_to_remove_;
}

const std::unordered_map<int, std::vector<std::filesystem::path>>& MergeLog::filesToRegister() const {
    return files_to_register_;
}
