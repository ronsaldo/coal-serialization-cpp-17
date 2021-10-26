#include "coal-serialization/coal.hpp"
#include <iostream>

#define assertEquals(expected, obtained) { \
    auto expectedValue = expected; \
    auto obtainedValue = obtained; \
    if(!(expectedValue == obtainedValue)) { \
        std::cout << __FILE__ << ":" << __LINE__ << ": Assertion failure: " << obtainedValue << " is not equal to the expected value of " << expectedValue << std::endl; \
        ++testErrorCount; \
    } } \

int main()
{
    int testErrorCount = 0;
    // Primitive values
    {
        assertEquals(false, coal::deserialize<bool> (coal::serialize(false)));
        assertEquals(true, coal::deserialize<bool> (coal::serialize(true)));

        assertEquals(42u, (uint16_t)coal::deserialize<uint8_t> (coal::serialize<uint8_t> (42)));
        assertEquals(42u, coal::deserialize<uint16_t> (coal::serialize<uint16_t> (42)));
        assertEquals(42u, coal::deserialize<uint32_t> (coal::serialize<uint32_t> (42)));
        assertEquals(42u, coal::deserialize<uint64_t> (coal::serialize<uint64_t> (42)));

        assertEquals(-42, (int)coal::deserialize<int8_t> (coal::serialize<int8_t> (-42)));
        assertEquals(-42, coal::deserialize<int16_t> (coal::serialize<int16_t> (-42)));
        assertEquals(-42, coal::deserialize<int32_t> (coal::serialize<int32_t> (-42)));
        assertEquals(-42, coal::deserialize<int64_t> (coal::serialize<int64_t> (-42)));


        assertEquals(42.5f, coal::deserialize<float> (coal::serialize<float> (42.5f)));
        assertEquals(42.5, coal::deserialize<double> (coal::serialize<double> (42.5)));
    }

    if(testErrorCount > 0)
        std::cerr << testErrorCount << " test failures" << std::endl;
    return testErrorCount > 0 ? 1 : 0;
}