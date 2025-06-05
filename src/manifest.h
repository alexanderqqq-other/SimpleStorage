#pragma once
#include "types.h"
#include <string>
#include <filesystem>

class Manifest {
public:
    Manifest(const std::filesystem::path& path, const Config& config_);
    const Config& getConfig() const;
private:
    const std::filesystem::path& getPath() const;
    void validateConfig() const;
    Config config_;
    std::filesystem::path data_dir_;
};
