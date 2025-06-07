#include "sstfile.h"
#include "utils.h"
#include <array>
namespace iblock = sst::indexblock;
int SSTFile::max_cached_files_ = 10; // Maximum number of cached datablocks

SSTFile::SSTFile(const std::filesystem::path& path, sst::indexblock::OffsetFieldType index_block_offset,
    uint64_t seq_num, const std::string max_key,
    std::vector <std::pair<std::string, iblock::OffsetFieldType>> index_block) :
    path_(path), index_block_offset_(index_block_offset), index_block_(std::move(index_block)), seq_num_(seq_num), max_key_(max_key) {}

void SSTFile::openIfNeeded() const {
    if (!ifs_.is_open()) {
        ifs_.open(path_, std::ios::binary);
    }
}

std::vector<uint8_t> SSTFile::readDatablock(iblock::OffsetFieldType block_offset, iblock::OffsetFieldType block_size) const {
    // SSTFile is not thread-safe, but this is const operation so it can be called concurrently
    std::lock_guard lock(cache_mutex_);
    if (datablock_cache_.find(block_offset) != datablock_cache_.end()) {
        return datablock_cache_[block_offset];
    }
    if (datablock_cache_.size() >= max_cached_files_) {
        // Remove random cached block if we exceed the limit
        datablock_cache_.erase(datablock_cache_.begin());
    }
    openIfNeeded();
    if (!ifs_) {
        return {};
    }
    ifs_.seekg(block_offset, std::ios::beg);
    std::vector<uint8_t> data(block_size);
    ifs_.read(reinterpret_cast<char*>(data.data()), block_size);
    if (ifs_.gcount() != block_size) {
        return {};
    }
    datablock_cache_[block_offset] = std::move(data);
    return datablock_cache_[block_offset];
}

std::vector<uint8_t> SSTFile::readDatablock(const std::filesystem::path path, sst::indexblock::OffsetFieldType block_offset, sst::indexblock::OffsetFieldType block_size)
{
    std::vector<uint8_t> ret;
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) {
        return ret;
    }
    ifs.seekg(block_offset, std::ios::beg);
    ret.resize(block_size);
    ifs.read(reinterpret_cast<char*>(ret.data()), block_size);
    if (ifs.gcount() != block_size) {
        return ret;
    }
    return ret;
}

void SSTFile::writeDatablock(const DataBlock& block, sst::indexblock::OffsetFieldType offsetIndex) const {
    datablock_cache_[offsetIndex] = block.data();
    std::fstream ofs(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Failed to open SST file for writing: " + path_.string());
    }
    ofs.seekp(offsetIndex, std::ios::beg);
    ofs.write(reinterpret_cast<const char*>(block.data().data()), block.data().size());
    if (!ofs) {
        throw std::runtime_error("Failed to write datablock to SST file: " + path_.string());
    }
}

auto SSTFile::findDBlockOffset(const std::string& min_key) const {
    auto it = std::upper_bound(index_block_.begin(), index_block_.end(), min_key,
        [](const std::string& lhs, const std::pair<std::string, iblock::OffsetFieldType>& rhs) {
            return lhs < rhs.first;
        });
    return it == index_block_.begin() ? index_block_.end() : std::prev(it);
}

sst::indexblock::OffsetFieldType SSTFile::getDatablockSize(decltype(index_block_)::const_iterator it) const
{
    sst::indexblock::OffsetFieldType block_size = 0;
    auto next_it = std::next(it);
    if (next_it == index_block_.end()) {
        block_size = index_block_offset_ - it->second;
    }
    else {
        block_size = next_it->second - it->second;
    }
    return block_size;
}


std::optional<Entry> SSTFile::get(const std::string& key) const {
    auto it = findDBlockOffset(key);
    if (it == index_block_.end()) {
        return std::nullopt;
    }
    auto data = readDatablock(it->second, getDatablockSize(it));
    if (data.empty()) {
        return std::nullopt; // No data block found for the key
    }
    DataBlock data_block(std::move(data));
    return data_block.get(key);
}
bool SSTFile::remove(const std::string& key)
{
    auto it = findDBlockOffset(key);
    if (it == index_block_.end()) {
        return false;
    }
    auto datablock_size = getDatablockSize(it);
    auto data = readDatablock(it->second, datablock_size);
    if (data.empty()) {
        return false;
    }
    // We don't protect the cache by mutex because remove is not thread-safe operation
    auto find_it = datablock_cache_.find(it->second);
    if (find_it != datablock_cache_.end()) {
        find_it->second = data; // Update the cached block with the latest data
    }
    DataBlock data_block(std::move(data));
    if (data_block.remove(key)) {
        writeDatablock(data_block, it->second);
        return true;
    }
    return false;
}
EntryStatus SSTFile::status(const std::string& key) const
{
    auto it = findDBlockOffset(key);
    if (it == index_block_.end()) {
        return EntryStatus::NOT_FOUND;
    }
    auto data = readDatablock(it->second, getDatablockSize(it));
    if (data.empty()) {
        return EntryStatus::NOT_FOUND;
    }
    DataBlock data_block(std::move(data));
    return data_block.status(key);
}
void SSTFile::rename(const std::filesystem::path& new_path) {
    if (ifs_.is_open()) {
        ifs_.close();
    }
    std::filesystem::rename(path_, new_path);
    path_ = new_path;
}

std::string SSTFile::minKey() const {
    if (index_block_.empty()) {
        throw std::runtime_error("Index block is empty, cannot retrieve minimum key.");
    }
    return index_block_.front().first;
}

std::string SSTFile::maxKey() const {
    return max_key_;
}

std::unique_ptr<SSTFile> SSTFile::readAndCreate(const std::filesystem::path& sst_path) {
    std::ifstream ifs(sst_path, std::ios::binary | std::ios::ate);
    if (!ifs) throw std::runtime_error("Failed to open SST file for reading: " + sst_path.string());

    uint64_t filesize = ifs.tellg();
    ifs.seekg(0, std::ios::beg);
    if (filesize < iblock::INDEX_BLOCK_COUNT_SIZE + sst::header::SST_HEADER_SIZE)
        throw std::runtime_error("File too small for SST structure");

    char signature[5] = { 0 };
    ifs.read(signature, sst::header::SST_SIGNATURE_SIZE);
    if (std::string(signature, sst::header::SST_SIGNATURE_SIZE) != sst::header::SST_SIGNATURE)
        throw std::runtime_error("Invalid SST signature");
    uint8_t version;
    ifs.read(reinterpret_cast<char*>(&version), sst::header::SST_VERSION_SIZE);
    std::array<uint8_t, sst::header::SST_SEQUENCE_SIZE> sequence_bytes;
    ifs.read(reinterpret_cast<char*>(sequence_bytes.data()), sst::header::SST_SEQUENCE_SIZE);
    uint64_t seq_num = Utils::deserializeLE<uint64_t>(sequence_bytes.data());

    ifs.seekg(filesize - iblock::INDEX_BLOCK_COUNT_SIZE, std::ios::beg);
    iblock::CountFieldType indexblock_size = 0;
    ifs.read(reinterpret_cast<char*>(&indexblock_size), sizeof(indexblock_size));
    indexblock_size = Utils::deserializeLE<iblock::CountFieldType>(reinterpret_cast<uint8_t*>(&indexblock_size));
    if (filesize < indexblock_size + iblock::INDEX_BLOCK_COUNT_SIZE + sst::header::SST_HEADER_SIZE)
        throw std::runtime_error("File too small for SST index block");
    auto indexblock_offset = filesize
        - static_cast<std::streamoff>(indexblock_size)
        - static_cast<std::streamoff>(sizeof(indexblock_size));

    ifs.seekg(indexblock_offset, std::ios::beg);
    std::vector<uint8_t> indexblock_buf(indexblock_size);
    ifs.read(reinterpret_cast<char*>(indexblock_buf.data()), indexblock_size);

    std::vector<std::pair<std::string, iblock::OffsetFieldType>> index_block;
    uint64_t pos = 0;
    iblock::OffsetFieldType offset = 0;
    while (pos + sizeof(iblock::IndexKeyLengthFieldType) < indexblock_buf.size()) {
        auto key_len = Utils::deserializeLE<iblock::IndexKeyLengthFieldType>(&indexblock_buf[pos]);
        if (key_len == 0 || pos + iblock::INDEX_KEY_LEN + iblock::BLOCK_OFFSET_SIZE + key_len > indexblock_buf.size()) {
            throw std::runtime_error("Invalid key length in index block");
        }
        pos += sizeof(iblock::IndexKeyLengthFieldType);
        auto min_key = Utils::deserializeLE<std::string>(&indexblock_buf[pos], key_len);
        pos += key_len;
        offset = Utils::deserializeLE<iblock::OffsetFieldType>(&indexblock_buf[pos]);
        pos += sizeof(iblock::OffsetFieldType);
        index_block.emplace_back(std::move(min_key), offset);
    }

    auto db = DataBlock(readDatablock(sst_path, offset, indexblock_offset - index_block.back().second));
    auto max_key = db.get(db.count() - 1).first;
    return std::unique_ptr<SSTFile>(new SSTFile(sst_path, indexblock_offset, seq_num, max_key, std::move(index_block)));
}



std::vector<std::string> SSTFile::keysWithPrefix(const std::string& prefix, unsigned int max_results) const {
    std::vector<std::string> result;
    if (prefix > maxKey()) {
        return result;
    }
    auto min_key = minKey();
    if (prefix < min_key && min_key.rfind(prefix, 0) != 0) {
        return result;
    }
    result.reserve(max_results);
    auto it = findDBlockOffset(prefix);
    if (it == index_block_.end()) {
        it = index_block_.begin(); // Key is out of the block, but prefix might be less then min_key
    }
    for (; it != index_block_.end() &&
        result.size() < static_cast<size_t>(max_results);
        ++it) {
        if (prefix < it->first && it->first.rfind(prefix, 0) != 0) {
            break;
        }
        auto block_data = readDatablock(it->second, getDatablockSize(it));
        DataBlock block(std::move(block_data));
        auto keys = block.keysWithPrefix(prefix, max_results - static_cast<int>(result.size()));
        result.insert(result.end(), keys.begin(), keys.end());
        if (result.size() >= static_cast<size_t>(max_results)) break;
    }

    return result;
}

std::unique_ptr<SSTFile> SSTFile::shrink(uint32_t datablock_size) const {
    auto first_out_path = path_.string() + std::string("_cleaned_.tmp");
    return SSTFile::writeAndCreate(first_out_path, datablock_size, seqNum(),
        false, begin(), end());
}

void SSTFile::clearCache() noexcept {
    //This operation is not thread-safe
    datablock_cache_.clear();
}

std::vector<std::unique_ptr<SSTFile>>  SSTFile::merge(
    const std::filesystem::path& sst1_path,
    const std::vector<std::filesystem::path>& dst_file_paths,
    const std::filesystem::path& out_dir,
    uint64_t max_file_size,
    uint32_t datablock_size,
    bool keep_removed)
{
    // Read input files
    auto sst1 = SSTFile::readAndCreate(sst1_path);

    std::vector<std::unique_ptr<SSTFile>>  dst_files;
    if (dst_file_paths.empty()) {
        auto first_out_path = out_dir / ("merged_" + std::to_string(sst1->seqNum()) + ".tmp");
        dst_files.push_back(SSTFile::writeAndCreate(first_out_path, datablock_size, sst1->seqNum(),
            keep_removed, sst1->begin(), sst1->end()));
        return dst_files;
    }


    dst_files.reserve(dst_file_paths.size());
    for (const auto& dst_file_path : dst_file_paths) {
        dst_files.push_back(SSTFile::readAndCreate(dst_file_path));
    }

    bool sst1_before = !dst_files.empty() && sst1->maxKey() < dst_files.front()->minKey();
    bool sst1_after = !dst_files.empty() && sst1->minKey() > dst_files.back()->maxKey();
    if (dst_files.size() == 1 && (sst1_before || sst1_after)) {
        uint64_t seq_num = std::min(sst1->seqNum(), dst_files.front()->seqNum());
        SSTBuilder builder(out_dir / ("merged_" + std::to_string(seq_num) + ".tmp"), datablock_size, seq_num);
        auto copyFile = [&](const std::unique_ptr<SSTFile>& file) {
            int i = 0;
            for (auto it = file->index_block_.begin(); it != file->index_block_.end(); ++it, ++i) {
                auto block_data = file->readDatablock(it->second, file->getDatablockSize(it));
                std::string max_key;
                if (i == file->index_block_.size() - 1) {
                    DataBlock db(block_data);
                    max_key = db.get(db.count() - 1).first;
                }
                builder.addDatablock(it->first, block_data, max_key);
            }
        };
        if (sst1_before) {
            copyFile(sst1);
            copyFile(dst_files.front());
        } else {
            copyFile(dst_files.front());
            copyFile(sst1);
        }
        std::vector<std::unique_ptr<SSTFile>> res;
        res.push_back(builder.finalize());
        return res;
    }
    std::vector<std::unique_ptr<SSTFile>>  result;
    std::vector<uint64_t> seq_nums;
    seq_nums.reserve(dst_files.size() + 1);
    seq_nums.push_back(sst1->seqNum());
    for (const auto& sst : dst_files) {
        seq_nums.push_back(sst->seqNum());
    }
    std::sort(seq_nums.begin(), seq_nums.end());
    auto seq_num = seq_nums.front();

    SSTBuilder builder(out_dir / ("merged_" + std::to_string(seq_num) + ".tmp"),
        datablock_size, seq_num);

    auto it1 = sst1->begin();
    auto it2 = dst_files.front()->begin();
    int i = 0;
    int current_seq_index = 0;
    while (it1 != sst1->end() && i < dst_files.size()) {
        if (it2 == dst_files[i]->end()) {
            ++i;
            it2 = (i < dst_files.size()) ? dst_files[i]->begin() : dst_files.back()->end();
            continue;
        }

        if (builder.currentSize() >= max_file_size - datablock_size) {
            result.push_back(builder.finalize());
            ++current_seq_index;
            if (current_seq_index >= seq_nums.size()) {
                throw std::runtime_error("Merge result can not exceed dsestanation file numbers + 1");
            }
            auto p = out_dir / ("merged_" + std::to_string(seq_nums[current_seq_index]) + ".tmp");

            builder = SSTBuilder(p, datablock_size, seq_nums[current_seq_index]);
        }
        auto [key1, stt_entry1] = *it1;
        if (!keep_removed && stt_entry1.entry.type == ValueType::REMOVED) {
            ++it1;
            continue;
        }
        auto [key2, stt_entry2] = *it2;
        if (!keep_removed && stt_entry2.entry.type == ValueType::REMOVED) {
            ++it2;
            continue;
        }
        if (key1 < key2) {
            builder.addEntry(key1, stt_entry1.entry, stt_entry1.expiration_ms);
            ++it1;
        }
        else if (key1 > key2) {
            builder.addEntry(key2, stt_entry2.entry, stt_entry2.expiration_ms);
            ++it2;
        }
        else {
            if (sst1->seqNum() >= dst_files[i]->seqNum()) {
                builder.addEntry(key1, stt_entry1.entry, stt_entry1.expiration_ms);
            }
            else {
                builder.addEntry(key2, stt_entry2.entry, stt_entry2.expiration_ms);
            }
            ++it1;
            ++it2;
        }
    }
    while (it1 != sst1->end()) {
        auto [key1, stt_entry1] = *it1;
        if (keep_removed || stt_entry1.entry.type != ValueType::REMOVED) {
            builder.addEntry(key1, stt_entry1.entry, stt_entry1.expiration_ms);
        }
        ++it1;
    }
    while (i < dst_files.size()) {
        if (it2 == dst_files[i]->end()) {
            ++i;
            continue;
        }

        auto [key2, stt_entry2] = *it2;
        if (keep_removed || stt_entry2.entry.type != ValueType::REMOVED) {
            builder.addEntry(key2, stt_entry2.entry, stt_entry2.expiration_ms);
        }
        ++it2;
    }
    result.push_back(builder.finalize());
    return result;
}
