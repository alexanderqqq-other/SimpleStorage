#include "generallevel.h"
#include <regex>
namespace {
    constexpr auto file_extension = ".vsst";
    constexpr auto file_prefix = "general_";

    uint64_t extractSecondNumber(const std::string& filename) {
        std::regex pattern(R"(_\d+_(\d+)\.)");
        std::smatch match;

        if (std::regex_search(filename, match, pattern)) {
            return std::stoi(match[1]);
        }

        return 0;
    }

    template<typename Map, typename LRU, typename LruIt>
    static LruIt findSSTImpl(Map& sst_file_map, LRU& lru_sst_files, const std::string& key, LruIt lru_end) {
        if (sst_file_map.empty()) {
            return lru_end;
        }
        auto it = sst_file_map.upper_bound(key);
        if (it == sst_file_map.begin() || (it != sst_file_map.end() && it->second->maxKey() < key)) {
            return lru_end;
        }
        --it;
        lru_sst_files.splice(lru_end, lru_sst_files, it->second); // Move to the end of the list
        return it->second;
    }
}



auto GeneralLevel::findSST(const std::string& key) -> decltype(lru_sst_files_)::iterator {
    return findSSTImpl(sst_file_map_, lru_sst_files_, key, lru_sst_files_.end());
}

auto GeneralLevel::findSST(const std::string& key) const -> decltype(lru_sst_files_)::const_iterator {
    return findSSTImpl(sst_file_map_, lru_sst_files_, key, lru_sst_files_.end());
}


GeneralLevel::GeneralLevel(const std::filesystem::path& path, size_t max_file_size, size_t max_num_files, bool is_last) :
    path_(path), max_file_size_(max_file_size), max_num_files_(max_num_files), is_last_(is_last){
    if (!std::filesystem::exists(path_)) {
        std::filesystem::create_directories(path_);
    }
    std::vector<SSTFile> sst_files;
    for (const auto& entry : std::filesystem::directory_iterator(path_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".vsst") {
            auto sst = SSTFile::readAndCreate(entry.path());
            sst_files.push_back(std::move(sst));
            max_file_index_ = std::max(max_file_index_, extractSecondNumber(entry.path().filename().string()));
        }
    }
    addSST(std::move(sst_files));
}


std::optional<Entry> GeneralLevel::get(const std::string& key) const {
    auto it = findSST(key);
    if (it == lru_sst_files_.end()) {
        return std::nullopt;
    }
    return it->get(key);
}

bool GeneralLevel::remove(const std::string& key, uint64_t max_seq_num) {
    auto it = findSST(key);
    if (it == lru_sst_files_.end()) {
        return false;
    }
    return it->remove(key);
}

EntryStatus GeneralLevel::status(const std::string& key) const {
    auto it = findSST(key);
    if (it == lru_sst_files_.end()) {
        return EntryStatus::NOT_FOUND;
    }
    return it->status(key);
}

std::vector<std::string> GeneralLevel::keysWithPrefix(const std::string& prefix, unsigned int max_results) const {
    std::vector<std::string> result;
    result.reserve(max_results);
    for (auto it = sst_file_map_.lower_bound(prefix); it != sst_file_map_.end(); ++it) {
        const auto& sst = *(it->second);
        auto keys = sst.keysWithPrefix(prefix, max_results - result.size());
        result.insert(result.end(),
            std::make_move_iterator(keys.begin()),
            std::make_move_iterator(keys.end()));
        if (result.size() >= max_results) break;
    }
    return result;
}



std::vector<std::filesystem::path> GeneralLevel::filelistToMerge(uint64_t max_seq_num) const {
    std::vector<std::filesystem::path> ret;
    if (seq_num_map_.size() < max_num_files_) {
        return ret;
    }
    //merge 20% oldest files to next level
    int i = 0;
    for (auto it = seq_num_map_.begin(); it != seq_num_map_.end() && i < seq_num_map_.size() / 3; ++it) {
        ret.push_back(it->second->path());
    }
    return ret;
}

// Merge a single SST file into this level
IFileLevel::MergeResult GeneralLevel::mergeToTmp(const std::filesystem::path& sst_path, size_t datablock_size) const {
    MergeResult result;
    auto new_sst_file = SSTFile::readAndCreate(sst_path);
    auto it_upper = sst_file_map_.upper_bound(new_sst_file.minKey());

    // Check the case if first file is overlaped
    if (it_upper != sst_file_map_.begin() && !sst_file_map_.empty()) {
        auto it_prev = std::prev(it_upper);
        // If file's maxKey >= min_key, then it overlaps
        if (it_prev->second->maxKey() >= new_sst_file.minKey()) {
            result.files_to_remove.push_back(it_prev->second->path());
        }
    }

    for (auto it = it_upper; it != sst_file_map_.end() && it->first <= new_sst_file.maxKey(); ++it) {
        result.files_to_remove.push_back(it->second->path());
    }

    //merge with empty file is OK will just copy new file to .tmp file
    result.new_files = SSTFile::merge(
        sst_path,
        result.files_to_remove,
        path_,
        max_file_size_,
        datablock_size,
        !is_last_
    );
    // Remove paths from files_to_remove that exist in new_files
    return result;
}

void GeneralLevel::addSST(std::vector<SSTFile> ssts) {
    for (auto& sst : ssts) {
        std::filesystem::path fpath;
        auto num_str =  std::to_string(sst.seqNum()) + "_" + std::to_string(max_file_index_);
        auto fname = file_prefix + num_str + file_extension;
        fpath = path_ / fname;

        sst.rename(fpath);
        lru_sst_files_.push_back(std::move(sst));
        auto back = std::prev(lru_sst_files_.end());
        sst_file_map_[back->minKey()] = back;
        seq_num_map_[back->seqNum()] = back;
        file_path_map_[back->path().string()] = back;
        ++max_file_index_;
    }
}

void GeneralLevel::removeSSTs(const std::vector<std::filesystem::path>& sst_paths) {
    for (const auto& sst_path : sst_paths) {
        auto it = file_path_map_.find(sst_path.string());
        if (it != file_path_map_.end()) {
            sst_file_map_.erase(it->second->minKey());
            seq_num_map_.erase(it->second->seqNum());
            lru_sst_files_.erase(it->second);
            file_path_map_.erase(it);
        }
    }
}

IFileLevel::MergeResult GeneralLevel::shrink(uint32_t datablock_size) {
    MergeResult result;
    for (const auto& file : lru_sst_files_) {
        result.new_files.push_back(file.shrink(datablock_size));
        result.files_to_remove.push_back(file.path());
    }
    return result;
}
