#include <boost/interprocess/sync/file_lock.hpp>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

class StorageLockFile {
public:
    // Constructor: tries to create (if needed) and lock the given file.
    // Throws std::runtime_error if the lock cannot be acquired.
    explicit StorageLockFile(const std::filesystem::path& path)
    {
        
        // Ensure parent directories exist (optional; only if you want automatic directory creation)
        if (path.has_parent_path()) {
            std::filesystem::create_directories(path.parent_path());
        }

        // Create the file if it does not already exist
        if (!std::filesystem::exists(path)) {
            std::ofstream ofs(path);
            if (!ofs) {
                throw std::runtime_error("Cannot create lock file: " + path.string());
            }
        }
        file_lock_ = boost::interprocess::file_lock(path.c_str());
        if (!file_lock_.try_lock()) {
            throw std::runtime_error("Unable to acquire lock on file: " + path.string());
        }
    }

    // Delete copy constructor and copy assignment: locking resource should not be duplicated.
    StorageLockFile(const StorageLockFile&) = delete;
    StorageLockFile& operator=(const StorageLockFile&) = delete;

    // Delete move constructor and move assignment to keep things simple
    StorageLockFile(StorageLockFile&&) = delete;
    StorageLockFile& operator=(StorageLockFile&&) = delete;


private:
    boost::interprocess::file_lock file_lock_;     
};
