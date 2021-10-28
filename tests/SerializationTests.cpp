#include "coal-serialization/coal.hpp"
#include <stdexcept>
#include <iostream>

#define guardException(block) try block \
    catch(std::exception &e) { \
        std::cout << __FILE__ << ":" << __LINE__ << ": Exception caught: " << e.what() << std::endl; \
        ++testErrorCount; \
    }

#define assertEquals(expected, obtained) guardException({ \
    auto expectedValue = expected; \
    auto obtainedValue = obtained; \
    if(!(expectedValue == obtainedValue)) { \
        std::cout << __FILE__ << ":" << __LINE__ << ": Assertion failure: " << obtainedValue << " is not equal to the expected value of " << expectedValue << std::endl; \
        ++testErrorCount; \
    } }) \

/**
 * Sample structure with inline Coal serialization specs.
 */
struct TestStructure : public coal::SerializableStructureTag
{
    typedef TestStructure SelfType;

    static constexpr char const __coal_name__[] = "TestStructure";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"booleanField", &SelfType::booleanField},
            {"integerField", &SelfType::integerField},
            {"floatField", &SelfType::floatField},
        };
    }

    bool booleanField = false;
    int integerField = 0;
    float floatField = 0;

    bool operator==(const TestStructure &other) const
    {
        return booleanField == other.booleanField
            && integerField == other.integerField
            && floatField == other.floatField;
    }

    friend std::ostream &operator<<(std::ostream &out, const TestStructure &value)
    {
        out << '{' << value.booleanField << ", " << value.integerField << ", " << value.floatField << "}";
        return out;
    }
};

struct TestStructureWithDifferentOrder
{
    int integerField = 0;
    float floatField = 0;
    bool booleanField = false;

    bool operator==(const TestStructureWithDifferentOrder &other) const
    {
        return booleanField == other.booleanField
            && integerField == other.integerField
            && floatField == other.floatField;
    }

    friend std::ostream &operator<<(std::ostream &out, const TestStructureWithDifferentOrder &value)
    {
        out << '{' << value.booleanField << ", " << value.integerField << ", " << value.floatField << "}";
        return out;
    }
};

int main()
{
    int testErrorCount = 0;
    // Primitive values
    {
        assertEquals(false, coal::deserialize<bool> (coal::serialize(false)).value());
        assertEquals(true, coal::deserialize<bool> (coal::serialize(true)).value());

        assertEquals(42u, (uint16_t)coal::deserialize<uint8_t> (coal::serialize<uint8_t> (42)).value());
        assertEquals(42u, coal::deserialize<uint16_t> (coal::serialize<uint16_t> (42)).value());
        assertEquals(42u, coal::deserialize<uint32_t> (coal::serialize<uint32_t> (42)).value());
        assertEquals(42u, coal::deserialize<uint64_t> (coal::serialize<uint64_t> (42)).value());

        assertEquals(-42, (int)coal::deserialize<int8_t> (coal::serialize<int8_t> (-42)).value());
        assertEquals(-42, coal::deserialize<int16_t> (coal::serialize<int16_t> (-42)).value());
        assertEquals(-42, coal::deserialize<int32_t> (coal::serialize<int32_t> (-42)).value());
        assertEquals(-42, coal::deserialize<int64_t> (coal::serialize<int64_t> (-42)).value());


        assertEquals(42.5f, coal::deserialize<float> (coal::serialize<float> (42.5f)).value());
        assertEquals(42.5, coal::deserialize<double> (coal::serialize<double> (42.5)).value());
    }

    // Structure
    {
        // Empty
        assertEquals(TestStructure{}, coal::deserialize<TestStructure> (coal::serialize<TestStructure> (TestStructure{})).value());
    }

    if(testErrorCount > 0)
        std::cerr << testErrorCount << " test failures" << std::endl;
    return testErrorCount > 0 ? 1 : 0;
}