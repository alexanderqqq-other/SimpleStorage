#include "manifest.h"
#include <nlohmann/json.hpp>
#include <filesystem>
#include <fstream>

constexpr std::string_view manifest_filename = "manifest.json";
constexpr std::string_view expected_type = "SimpleStorage";
Manifest::Manifest(const std::filesystem::path& path, const Config& config):
    data_dir_(path), config_(config) {
    validateConfig(config_);
    if (!std::filesystem::exists(data_dir_)) {
        std::filesystem::create_directories(data_dir_);
    }
    // Build the full path to manifest.json
    std::filesystem::path manifest_path = data_dir_ / manifest_filename;

    // Ignore config if storage already exists
    if (std::filesystem::exists(manifest_path)) {
        // If manifest.json already exists, read and parse it
        std::ifstream in(manifest_path);
        if (!in.is_open()) {
            throw std::runtime_error("Failed to open existing manifest file: " + manifest_path.string());
        }

        nlohmann::json j;
        in >> j;

        if (!j.contains("type") || !j["type"].is_string()) {
            throw std::runtime_error("Manifest is not SimpleStorage manifest");
        }
        std::string type_in_file = j["type"].get<std::string>();
        if (type_in_file != expected_type) {
            throw std::runtime_error("Manifest is not SimpleStorage manifest");
        }
        if (j.contains("memtable_size_bytes") && j["memtable_size_bytes"].is_number_unsigned()) {
            config_.memtable_size_bytes = j["memtable_size_bytes"].get<size_t>();
        }
        if (j.contains("l0_max_files") && j["l0_max_files"].is_number_unsigned()) {
            config_.l0_max_files = j["l0_max_files"].get<size_t>();
        }
        if (j.contains("block_size") && j["block_size"].is_number_unsigned()) {
            config_.block_size = j["block_size"].get<size_t>();
        }
        if (j.contains("shrink_timer_minutes") && j["shrink_timer_minutes"].is_number_unsigned()) {
            config_.shrink_timer_minutes = j["shrink_timer_minutes"].get<uint32_t>();
        }

    }
    else {
        nlohmann::json j;
        j["type"] = std::string(expected_type);
        j["memtable_size_bytes"] = config_.memtable_size_bytes;
        j["l0_max_files"] = config_.l0_max_files;
        j["block_size"] = config_.block_size;
        j["shrink_timer_minutes"] = config_.shrink_timer_minutes;

        std::ofstream out(manifest_path);
        if (!out.is_open()) {
            throw std::runtime_error("Failed to create manifest file: " + manifest_path.string());
        }
        out << j.dump(4) << std::endl; // Pretty print with 4 spaces indentation
    }
}

const Config& Manifest::getConfig() const {
    return config_;
}

const std::filesystem::path& Manifest::getPath() const {
    return data_dir_;
}

void Manifest::validateConfig(const Config& config) const {
    if (config.memtable_size_bytes < sst::MIN_MEMTABLE_SIZE || config.memtable_size_bytes > sst::MAX_MEMTABLE_SIZE) {
        throw std::invalid_argument("Invalid memtable size: " + std::to_string(config.memtable_size_bytes) +
            ". Must be between " + std::to_string(sst::MIN_MEMTABLE_SIZE) + " and " + std::to_string(sst::MAX_MEMTABLE_SIZE));
    }
    if (config.l0_max_files < sst::MIN_L0_NUM_FILES ) {
        throw std::invalid_argument("Invalid L0 max files: " + std::to_string(config.l0_max_files) +
            ". Must be between greater than" + std::to_string(sst::MIN_L0_NUM_FILES));
    }
    if (config.block_size < sst::MIN_BLOCK_SIZE || config.block_size > sst::MAX_BLOCK_SIZE) {
        throw std::invalid_argument("Invalid block size: " + std::to_string(config.block_size) +
            ". Must be between " + std::to_string(sst::MIN_BLOCK_SIZE) + " and " + std::to_string(sst::MAX_BLOCK_SIZE));
    }
}
