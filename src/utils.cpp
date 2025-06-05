#include "utils.h"
#include "constants.h"

#include <chrono>
using namespace std::chrono;

uint64_t Utils::getNow()
{
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

bool Utils::isExpired(uint64_t timestamp)
{
    if (timestamp == sst::datablock::EXPIRATION_NOT_SET)
        return false;
    if (timestamp == sst::datablock::EXPIRATION_DELETED) // Special case for deleted entries
        return true;
    uint64_t now = getNow();
    return timestamp <= now;
}
