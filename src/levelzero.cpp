#include "levelzero.h"
#include "levelzero.h"
namespace {
    constexpr auto file_extension = ".vsst";
    constexpr auto file_prefix = "L0_";
}

LevelZero::LevelZero(const std::filesystem::path& path, size_t max_num_files) : path_(path), max_num_files_(max_num_files) {
    if (!std::filesystem::exists(path_)) {
        std::filesystem::create_directories(path_);
    }
    for (const auto& entry : std::filesystem::directory_iterator(path_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".vsst") {
            sst_files_.push_back(SSTFile::readAndCreate(entry.path()));
        }
    }
    std::sort(sst_files_.begin(), sst_files_.end(),
        [](const auto& a, const auto& b) {
            return a->seqNum() < b->seqNum();
        }); 
}

std::optional<Entry> LevelZero::get(const std::string& key) const {
    for (auto it = sst_files_.rbegin(); it != sst_files_.rend(); ++it) {
        auto val = (*it)->get(key);
        if (val.has_value()) {
            return val;
        }
    }
    return std::nullopt;
}

bool LevelZero::remove(const std::string& key, uint64_t max_seq_num) {
    for (auto it = sst_files_.rbegin(); it != sst_files_.rend(); ++it) {
        if ((*it)->seqNum() > max_seq_num) {
            continue; // Skip SST files with higher sequence numbers
        }
        if ((*it)->remove(key)) {
            return true;
        }
    }
    return false;
}

EntryStatus LevelZero::status(const std::string& key) const {
    for (auto it = sst_files_.rbegin(); it != sst_files_.rend(); ++it) {
        EntryStatus st = (*it)->status(key);
        if (st != EntryStatus::NOT_FOUND) {
            return st;
        }
    }
    return EntryStatus::NOT_FOUND;
}

std::vector<std::string> LevelZero::keysWithPrefix(const std::string& prefix, unsigned int max_results) const {
    std::vector<std::string> result;
    for (auto it = sst_files_.rbegin(); it != sst_files_.rend() && result.size() < max_results; ++it) {
        auto keys = (*it)->keysWithPrefix(prefix, max_results - result.size());
        result.insert(result.end(),
            std::make_move_iterator(keys.begin()),
            std::make_move_iterator(keys.end()));
        if (result.size() >= max_results)
            break;
    }
    return result;
}

bool LevelZero::forEachKeyWithPrefix(const std::string& prefix, const std::function<bool(const std::string&)>& callback) const {
    for (auto it = sst_files_.rbegin(); it != sst_files_.rend(); ++it) {
        if (!(*it)->forEachKeyWithPrefix(prefix, callback)) {
            return false; // Stop if callback returns false
        }
    }
    return true;
}


std::vector<std::filesystem::path> LevelZero::filelistToMerge(uint64_t max_seq_num) const {
    if (sst_files_.size() < max_num_files_) {
        return {};
    }
    std::vector<std::filesystem::path> ret;
    for (const auto& sst : sst_files_) {
        if (sst->seqNum() <= max_seq_num) {
            ret.push_back(sst->path());
        }
    }
    return ret;
}

IFileLevel::MergeResult LevelZero::mergeToTmp(const std::filesystem::path&, size_t) const {
    throw std::logic_error("Level 0 does not support merging to temporary files. Use Level 1 or higher for merging.");
}

void LevelZero::addSST(std::vector<std::unique_ptr<SSTFile>>  ssts) {
    for (auto& sst : ssts) {
        auto fname = file_prefix + std::to_string(sst->seqNum()) + file_extension;
        sst->rename(path_ / fname);
        sst_files_.push_back(std::move(sst));
    }
}

void LevelZero::removeSSTs(const std::vector<std::filesystem::path>& sst_paths) {
    for (const auto& path : sst_paths) {
        auto it = std::remove_if(sst_files_.begin(), sst_files_.end(),
            [&path](const std::unique_ptr<SSTFile>& sst) { return sst->path() == path; });
        if (it != sst_files_.end()) {
            sst_files_.erase(it, sst_files_.end());
        }
    }
}

void LevelZero::clearCache() noexcept {
    for (auto& sst : sst_files_) {
        sst->clearCache();
    }
}

size_t LevelZero::count() const {
    return sst_files_.size();
}
