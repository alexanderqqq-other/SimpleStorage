#include <gtest/gtest.h>
#include "../src/simplestorage.h"
#include "test_utils.h"
#include <filesystem>
#include <chrono>
#include <thread>
#include <atomic>
#include <iostream>
#include <random>
#include <optional>

using namespace std::chrono;

std::string get_directory_size_mb_str(const std::filesystem::path& dir_path) {
    std::uintmax_t size = 0;
    if (std::filesystem::exists(dir_path) && std::filesystem::is_directory(dir_path)) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir_path)) {
            if (std::filesystem::is_regular_file(entry.status())) {
                std::error_code ec;
                auto sz = std::filesystem::file_size(entry.path(), ec);
                if (!ec) {
                    size += sz;
                }
            }
        }
    }
    double size_mb = static_cast<double>(size) / (1024.0 * 1024.0);

    std::ostringstream out;
    out << std::fixed << std::setprecision(2) << size_mb << " MB";
    return out.str();
}

namespace {

    size_t envToMb(const char* var, size_t defMb) {
        const char* v = std::getenv(var);
        if (!v) return defMb;
        try { return std::stoull(v); }
        catch (...) { return defMb; }
    }

    size_t envToSizeT(const char* var, size_t defValue) {
        const char* v = std::getenv(var);
        if (!v) return defValue;
        try { return std::stoull(v); }
        catch (...) { return defValue; }
    }
    std::string long_value(50, 'x');        // minimum 50 bytes long value for testing
    std::string getKeyById(size_t id) {
        return getStringFromIndex(id) + pseudo_random_string(id) + long_value + std::to_string(id);
    }
}



TEST(PerformanceTest, HighLoadMultiThread) {
    // Configure total data volume in MB via PERF_TOTAL_SIZE_MB env variable
    size_t total_mb = envToMb("PERF_TOTAL_SIZE_MB", 100); // default 100MB
    uint64_t total_bytes_target = total_mb * 1024ull * 1024ull;

    // Configure number of worker threads via PERF_THREADS env variable
    size_t num_threads = envToSizeT("PERF_THREADS", std::thread::hardware_concurrency());
    if (num_threads == 0) num_threads = 4;
    size_t block_size = envToSizeT("PERF_BLOCK_SIZE_KB", 32 * 1024);
    uint64_t memtable_size = envToSizeT("PERF_MEMTABLE_SIZE_MB", 64);

    Config config;
    config.memtable_size_bytes = memtable_size * 1024 * 1024; // default 64MB
    config.l0_max_files = 4;
    config.block_size = block_size;
    config.shrink_timer_minutes = 0;

    std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "perf_db";
    std::filesystem::remove_all(temp_dir);

    auto db = std::make_shared<SimpleStorage>(temp_dir, config);

    std::atomic<uint64_t> bytes_written{ 0 };
    std::atomic<size_t> entries_written{ 0 };
    std::atomic<size_t> id_counter{ 0 };

    auto write_start = steady_clock::now();
    std::vector<std::thread> workers;
    std::vector<std::exception_ptr> exceptions(num_threads);
    std::cout << "Starting write with " << num_threads << " threads, target size: " << total_mb << " MB\n";

    for (size_t t = 0; t < num_threads; ++t) {

        workers.emplace_back([&, t]() {
            try {
                while (true) {
                    size_t id = id_counter.fetch_add(1);
                    std::string key = getKeyById(id);
                    auto entry = pseudo_random_value(id);
                    size_t size = Utils::onDiskEntrySize(key, entry.value);
                    uint64_t current = bytes_written.fetch_add(size);
                    if (current >= total_bytes_target) {
                        break;
                    }

                    std::visit([&db, &key, id](auto&& arg) mutable {
                        using T = std::decay_t<decltype(arg)>;
                        db->put(key, arg, id % 7 == 0 ? std::optional<uint32_t>(1) : std::nullopt);
                        }, entry.value);


                    ++entries_written;
                }
            }
            catch (...) {
                exceptions[t] = std::current_exception();
            }
            });
    }
    for (auto& th : workers) {
        th.join();
    }
    for (const auto& ex : exceptions) {
        if (ex) {
            try {
                std::rethrow_exception(ex);
            }
            catch (const std::exception& e) {
                FAIL() << "Worker thread exception: " << e.what();
            }
        }
    }
    db->flush();

    auto write_end = steady_clock::now();

    double write_seconds = duration<double>(write_end - write_start).count();
    std::cout << "Written " << entries_written.load() << " entries with total " << bytes_written.load() << " bytes in " << write_seconds << " seconds using " << num_threads << " threads\n";
    std::cout << "Total size on disk: " << get_directory_size_mb_str(temp_dir) << "\n";
    std::cout << "Merge still in progress\n";
    std::cout << "Starting read while merging...\n";

    size_t total_entries = entries_written.load();

    std::atomic<size_t> read_counter{ 0 };
    auto read_start = steady_clock::now();
    workers.clear();
    for (size_t t = 0; t < num_threads; ++t) {
        workers.emplace_back([&, t]() {
            try{
            while (true) {
                size_t id = read_counter.fetch_add(1);
                if (id >= total_entries) break;
                std::string key = getKeyById(id);
                volatile auto val = db->get(key);
            }
            }
            catch (...) {
                exceptions[t] = std::current_exception();
            }
            });
    }
    for (auto& th : workers) {
        th.join();
    }
    for (const auto& ex : exceptions) {
        if (ex) {
            try {
                std::rethrow_exception(ex);
            }
            catch (const std::exception& e) {
                FAIL() << "Worker thread exception: " << e.what();
            }
        }
    }
    auto read_end = steady_clock::now();

    double read_seconds = duration<double>(read_end - read_start).count();

    std::cout << "Read while merging " << total_entries << " entries in " << read_seconds << " seconds using " << num_threads << " threads\n";
    std::cout << "Total size on disk after read: " << get_directory_size_mb_str(temp_dir) << "\n";
    std::cout << "Reading values by prefix while merging...\n";
    // Prefix search timings for a few ranges
    auto prefix_start = steady_clock::now();
    uint64_t total_found=0;
    for (int i = 0; i < 10000; i++) {
        std::string prefix = getStringFromIndex(i).substr(0, 4);
        std::vector<std::string> keys;
        db->forEachKeyWithPrefix(prefix, [&](const std::string& k) {
            keys.push_back(k);
            if (keys.size() >= 100) {
                return false; // Limit to 100 results
            }
            return true;
        });
        total_found += keys.size();
    }

    auto prefix_end = steady_clock::now();
    double prefix_seconds = duration<double>(prefix_end - prefix_start).count();
    std::cout << "10000 prefix search with 100 limitation completed in " << prefix_seconds 
        << " seconds, total found: " << total_found << "\n";
    std::cout << "Removing every second entries while merging...\n";
    std::atomic<size_t> remove_counter{ 0 };
    auto remove_start = steady_clock::now();
    workers.clear();
    for (size_t t = 0; t < num_threads; ++t) {
        workers.emplace_back([&, t]() {
            try {
                while (true) {
                    size_t id = remove_counter.fetch_add(2);
                    if (id >= total_entries) break;
                    std::string key = getKeyById(id);
                    db->remove(key);
                }
            }
            catch (...) {
            exceptions[t] = std::current_exception();
        }
            });
    }
    for (auto& th : workers) {
        th.join();
    }
    for (const auto& ex : exceptions) {
        if (ex) {
            try {
                std::rethrow_exception(ex);
            }
            catch (const std::exception& e) {
                FAIL() << "Worker thread exception: " << e.what();
            }
        }
    }

    auto remove_end = steady_clock::now();

    double remove_seconds = duration<double>(remove_end - remove_start).count();
    std::cout << "Removed " << total_entries << " entries in " << remove_seconds << " seconds using " << num_threads << " threads\n";
    std::cout << "\n\n";
    std::cout << "Total test time:" << duration<double>(steady_clock::now() - write_start).count() << " seconds\n";
    std::cout << "\n\n";
    std::cout << "Waiting for merge to complete... It might take significant time\n";
    db->flush();
    auto meger_start = steady_clock::now();
    db->waitAllAsync();
    std::cout << "Rest merge time: " << duration<double>(steady_clock::now() - meger_start).count() << " seconds\n";
    std::cout << "Total size on disk after merge: " << get_directory_size_mb_str(temp_dir) << "\n";
    std::cout << "shrinking database\n";
    auto shrink_start = steady_clock::now();
    db->shrink();
    db->waitAllAsync();
    auto shrink_end = steady_clock::now();
    double shrink_seconds = duration<double>(shrink_end - shrink_start).count();
    std::cout << "Shrink completed in " << shrink_seconds << " seconds\n";
    std::cout << "Final size on disk: " << get_directory_size_mb_str(temp_dir) << "\n";
    std::cout << "Total test time:" << duration<double>(steady_clock::now() - write_start).count() << " seconds\n";
    SUCCEED();
}

