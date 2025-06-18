#include "datablock.h"
#include "datablock.h"
#include "utils.h"

namespace dblock = sst::datablock;

DataBlockBuilder::DataBlockBuilder(uint32_t max_block_size) : max_block_size_(max_block_size) {
    raw_data_.reserve(max_block_size);
}

bool DataBlockBuilder::addEntry(const std::string& key, const Entry& entry, uint64_t expiration_ms) {
    uint32_t value_size = Utils::onDiskSize(entry.value);
    auto offset_table_size = offset_table_.size() * sizeof(decltype(offset_table_)::value_type);

    //Current block size + new record size + current offset table size + new offset entry + offset_table_size
    uint64_t new_size = raw_data_.size() + Utils::onDiskEntrySize(key, entry.value) +
        offset_table_size + dblock::DATABLOCK_COUNT_SIZE + sst::datablock::OFFSET_ENTRY_SIZE;
    if (new_size > max_block_size_) {
        return false;
    }

    offset_table_.push_back(static_cast<uint32_t>(raw_data_.size()));
    raw_data_.reserve(raw_data_.size() + key.size() + sizeof(uint16_t) + value_size);
    auto key_size = static_cast<uint16_t>(key.size());
    Utils::serializeLE(key_size, raw_data_);
    raw_data_.insert(raw_data_.end(), key.begin(), key.end());
    Utils::serializeLE(expiration_ms, raw_data_);
    static_assert(sizeof(ValueType) == sizeof(uint8_t));
    Utils::serializeLE(static_cast<uint8_t>(entry.type), raw_data_);
    std::visit([this](const auto& val) {
        using T = std::decay_t<decltype(val)>;
        if constexpr (SupportedInteger<T> || SupportedReal<T>) {
            Utils::serializeLE(val, raw_data_);
        }
        else if constexpr (SupportedBlob<T>) {
            dblock::ValueLengthFieldType value_len = val.size() * sizeof(typename T::value_type);
            Utils::serializeLE(value_len, raw_data_);
            raw_data_.insert(raw_data_.end(), val.begin(), val.end());
        }
        else {
            static_assert(always_false<T>::value, "Unsupported type for serialization in DataBlockBuilder");
        }
        }, entry.value);
    ++count_;
    return true;
}

bool DataBlockBuilder::empty() const noexcept {
    return count_ == 0;
}

uint64_t DataBlockBuilder::size() const noexcept {
    return raw_data_.size() + offset_table_.size() * sizeof(decltype(offset_table_)::value_type)
        +sizeof(count_);
}

std::vector<uint8_t> DataBlockBuilder::build() {
    for (const auto& offset : offset_table_) {
        Utils::serializeLE(offset, raw_data_);
    }
    Utils::serializeLE(count_, raw_data_);
    offset_table_.clear();
    count_ = 0;
    std::vector<uint8_t> ret;
    ret.swap(raw_data_);
    return ret;
}


DataBlock::DataBlock(std::vector<uint8_t> data) : data_(std::move(data)) {
    if (data_.size() < sizeof(count_)) {
        throw std::runtime_error("DataBlock corrupted: Data size is too small to contain a valid block.");
    }
    count_ = Utils::deserializeLE<dblock::CountFieldType>(data_.data() + data_.size() - sizeof(count_));
    if (count_ == 0) {
        throw std::runtime_error("DataBlock corrupted: Block contains no entries.");
    }
    auto offset_table_size = count_ * sizeof(dblock::OffsetEntryFieldType);
    if (data_.size() < sizeof(count_) + offset_table_size) {
        throw std::runtime_error("DataBlock corrupted: Data size is too small to contain a valid offset table.");
    }
    offset_table_pos_ = static_cast<sst::datablock::OffsetEntryFieldType>(data_.size() - sizeof(count_) - offset_table_size);
    max_entry_ptr_ = static_cast<uint32_t>(data_.size() - sizeof(count_) - offset_table_size);
}

std::optional<Entry> DataBlock::get(const std::string& key) const {
    auto offset = lowerBoundOffset(key);
    if (offset >= count_) {
        return std::nullopt;
    }
    auto cursor = posByOffset(offset);
    std::string entry_key = parseKey(cursor);
    if (entry_key != key) {
        return std::nullopt;
    }
    ValueType type = parseValueType(cursor, key.size());
    if (type == ValueType::REMOVED) {
        return Entry{ type, {} };
    }
    return Entry{ type, parseValue(cursor, key.size(), type) };
}

std::pair<std::string, DataBlock::DataBlockEntry> DataBlock::get(sst::datablock::CountFieldType offsetIdx) const
{
    auto cursor = posByOffset(offsetIdx);
    auto key = parseKey(cursor);
    ValueType type = parseValueType(cursor, key.size());
    uint64_t expiration_pos = cursor + key.size() + dblock::KEY_LEN_SIZE;
    auto expiration_ms = Utils::deserializeLE<dblock::ExpirationFieldType>(&data_[expiration_pos]);
    if (type == ValueType::REMOVED) {
        return { key, DataBlockEntry{{ type, {}}, expiration_ms } };
    }
    return { key, DataBlockEntry{{ type, parseValue(cursor, key.size(), type) }, expiration_ms} };
}

std::vector<std::string> DataBlock::keysWithPrefix(const std::string& prefix, unsigned int max_results) const {
    std::vector<std::string> result;
    if (count_ == 0) return result;
    result.reserve(std::min(static_cast<uint32_t>(max_results), count_));
    auto offset_idx = lowerBoundOffset(prefix);
    for (sst::datablock::CountFieldType i = offset_idx; i < count_ && result.size() < static_cast<size_t>(max_results); ++i) {
        uint64_t pos = posByOffset(i);
        std::string entry_key = parseKey(pos);
        if (entry_key.compare(0, prefix.size(), prefix) == 0) {
            ValueType type = parseValueType(pos, entry_key.size());
            if (type == ValueType::REMOVED) {
                continue;
            }
            result.push_back(entry_key);
        }
        else {
            break;
        }
    }
    return result;
}

bool DataBlock::forEachKeyWithPrefix(const std::string& prefix,
    const std::function<bool(const std::string&)>& callback) const {
    if (count_ == 0) return true; // No entries to iterate over
    auto offset_idx = lowerBoundOffset(prefix);
    for (sst::datablock::CountFieldType i = offset_idx; i < count_; ++i) {
        uint64_t pos = posByOffset(i);
        std::string entry_key = parseKey(pos);
        if (entry_key.compare(0, prefix.size(), prefix) == 0) {
            ValueType type = parseValueType(pos, entry_key.size());
            if (type == ValueType::REMOVED) {
                continue;
            }
            if (!callback(entry_key)) {
                return false; // Stop iteration if callback returns false
            }
        }
        else {
            return true; 
        }
    }
    return true;
    
}

bool DataBlock::remove(const std::string& key) {
    auto offset = lowerBoundOffset(key);
    if (offset >= count_) {
        return false;
    }
    auto pos = posByOffset(offset);
    std::string entry_key = parseKey(pos);
    if (entry_key != key) {
        return false;
    }
    ValueType type = parseValueType(pos, key.size());
    if (type == ValueType::REMOVED) {
        return true;
    }
    // don't need to serialize one byte
    static_assert(sizeof(ValueType) == sizeof(uint8_t));
    data_[pos + key.size() + dblock::KEY_LEN_SIZE + dblock::EXPIRATION_SIZE] = static_cast<uint8_t>(ValueType::REMOVED);
    return true;  
}

EntryStatus DataBlock::status(const std::string& key) const {
    auto offset = lowerBoundOffset(key);
    if (offset >= count_) {
        return EntryStatus::NOT_FOUND; // Key not found
    }
    auto pos = posByOffset(offset);
    std::string entry_key = parseKey(pos);
    if (entry_key != key) {
        return EntryStatus::NOT_FOUND;
    }
    ValueType type = parseValueType(pos, key.size());
    if (type == ValueType::REMOVED) {
        return EntryStatus::REMOVED;
    }
    return EntryStatus::EXISTS;
}

uint64_t DataBlock::posByOffset(sst::datablock::CountFieldType offsetIdx) const
{
    auto offset_table_ptr_ = reinterpret_cast<const dblock::OffsetEntryFieldType*>(&data_[offset_table_pos_]);
    return Utils::deserializeLE<dblock::OffsetEntryFieldType>(reinterpret_cast<const uint8_t*>(&offset_table_ptr_[offsetIdx]));
}

ValueType DataBlock::parseValueType(uint64_t pos, sst::datablock::KeyLengthFieldType key_size) const {
    uint64_t cursor = pos + key_size + dblock::KEY_LEN_SIZE;
    if (cursor + dblock::EXPIRATION_SIZE + dblock::VALUE_TYPE_SIZE > max_entry_ptr_) {
        throw std::runtime_error("DataBlock corrupted: Offset points outside of data bounds.");
    }
    auto expiration_ms = Utils::deserializeLE<dblock::ExpirationFieldType>(&data_[cursor]);
    if (Utils::isExpired(expiration_ms)) {
        return ValueType::REMOVED;
    }
    cursor += sizeof(expiration_ms);
    return static_cast<ValueType>(Utils::deserializeLE<dblock::ValueTypeFieldType>(&data_[cursor]));
}

std::string DataBlock::parseKey(uint64_t pos) const
{
    if (pos + dblock::KEY_LEN_SIZE > max_entry_ptr_) {
        throw std::runtime_error("DataBlock corrupted: Offset points outside of data bounds.");
    }
    auto key_len = Utils::deserializeLE<dblock::KeyLengthFieldType>(&data_[pos]);
    if (key_len > dblock::MAX_KEY_LENGTH || pos + key_len > max_entry_ptr_) {
        throw std::runtime_error("DataBlock corrupted: Key length is invalid or exceeds maximum allowed length.");
    }
    return Utils::deserializeLE<std::string>(&data_[pos + dblock::KEY_LEN_SIZE], key_len);
}

Value DataBlock::parseValue(uint64_t entry_start_pos, sst::datablock::KeyLengthFieldType key_size, ValueType type) const
{
    uint64_t cursor = entry_start_pos + key_size + dblock::KEY_LEN_SIZE + dblock::EXPIRATION_SIZE + dblock::VALUE_TYPE_SIZE;
    dblock::ValueLengthFieldType value_len;
    if (type == ValueType::BLOB || type == ValueType::STRING || type == ValueType::U8STRING) {
        value_len = Utils::deserializeLE<sst::datablock::ValueLengthFieldType>(&data_[cursor]);
        if (cursor + value_len > max_entry_ptr_) {
            throw std::runtime_error("DataBlock corrupted: Value length exceeds data bounds.");
        }
        if (value_len == 0) {
            throw std::runtime_error("DataBlock corrupted: Value length is zero.");
        }
        cursor += sizeof(value_len);
    }
    switch (type) {
    case ValueType::UINT8: {
        return Utils::deserializeLE<uint8_t>(&data_[cursor]);
    }
    case ValueType::INT8: {
        return  Utils::deserializeLE<int8_t>(&data_[cursor]);
    }
    case ValueType::UINT16: {
        return Utils::deserializeLE<uint16_t>(&data_[cursor]);
    }
    case ValueType::INT16: {
        return Utils::deserializeLE<int16_t>(&data_[cursor]);
    }
    case ValueType::UINT32: {
        return Utils::deserializeLE<uint32_t>(&data_[cursor]);
    }
    case ValueType::INT32: {
        return  Utils::deserializeLE<int32_t>(&data_[cursor]);
    }
    case ValueType::UINT64: {
        return  Utils::deserializeLE<uint64_t>(&data_[cursor]);
    }
    case ValueType::INT64: {
        return Utils::deserializeLE<int64_t>(&data_[cursor]);
    }
    case ValueType::FLOAT: {
        return Utils::deserializeLE<float>(&data_[cursor]);
    }
    case ValueType::DOUBLE: {
        return Utils::deserializeLE<double>(&data_[cursor]);
    }
    case ValueType::STRING: {
        return Utils::deserializeLE<std::string>(&data_[cursor], value_len);
    }
    case ValueType::U8STRING: {
        return Utils::deserializeLE<std::u8string>(&data_[cursor], value_len);
    }
    case ValueType::BLOB: {
        return Utils::deserializeLE<std::vector<uint8_t>>(&data_[cursor], value_len);
    }
    default: {
        throw std::runtime_error("DataBlock corrupted: Unsupported value type.");
    }
    }
}

sst::datablock::CountFieldType DataBlock::lowerBoundOffset(const std::string& key) const {
    int left = 0;
    int right = static_cast<int>(count_);
    while (left < right) {
        int mid = left + (right - left) / 2;
        uint64_t pos = posByOffset(static_cast<sst::datablock::CountFieldType>(mid));
        std::string entry_key = parseKey(pos);
        if (key.compare(entry_key) <= 0) {
            right = mid;
        }
        else {
            left = mid + 1;
        }
    }
    return static_cast<sst::datablock::CountFieldType>(left);
}
