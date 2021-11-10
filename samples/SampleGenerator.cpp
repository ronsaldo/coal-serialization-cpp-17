#include "coal-serialization/coal.hpp"
#include "coal-serialization/coal-std-bindings.hpp"

#include <fstream>

void writeDataToFile(const std::string &filename, const std::vector<uint8_t> &data)
{
    std::ofstream out(filename, std::ios::binary | std::ios::out | std::ios::trunc);
    out.write(reinterpret_cast<const char*> (data.data()), data.size());
}

/**
 * Sample structure with inline Coal serialization specs.
 */
struct SampleStructure : public coal::SerializableStructureTag
{
    typedef SampleStructure SelfType;

    static constexpr char const __coal_typename__[] = "SampleStructure";

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
};

/**
 * Sample nested structure with inline Coal serialization specs.
 */
struct SampleNestedStructure : public coal::SerializableStructureTag
{
    typedef SampleNestedStructure SelfType;

    static constexpr char const __coal_typename__[] = "SampleNestedStructure";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"innerStruct", &SelfType::innerStruct},
            {"integerField", &SelfType::integerField},
        };
    }

    SampleStructure innerStruct = {};
    int integerField = 0;
};

/**
 * SampleObject
 */
class SampleObject : public coal::SerializableSharedObjectClassTag
{
public:
    typedef SampleObject SelfType;

    static constexpr char const __coal_typename__[] = "SampleObject";

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
};

/**
 * SampleObjectOuter
 */
class SampleObjectOuter : public coal::SerializableSharedObjectClassTag
{
public:
    typedef SampleObjectOuter SelfType;

    static constexpr char const __coal_typename__[] = "SampleObjectOuter";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"innerObject", &SelfType::innerObject},
        };
    }

    std::shared_ptr<SampleObject> innerObject;
};

/**
 * SampleCyclicObject
 */
class SampleCyclicObject : public coal::SerializableSharedObjectClassTag
{
public:
    typedef SampleCyclicObject SelfType;

    static constexpr char const __coal_typename__[] = "SampleCyclicObject";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"potentiallyCyclicReference", &SelfType::potentiallyCyclicReference},
            {"potentiallyCyclicReference2", &SelfType::potentiallyCyclicReference2},
        };
    }

    std::shared_ptr<SampleCyclicObject> potentiallyCyclicReference;
    std::shared_ptr<SampleCyclicObject> potentiallyCyclicReference2;
};

/**
 * SampleSharedObjectWithCollection
 */
class SampleObjectWithCollection : public coal::SerializableSharedObjectClassTag
{
public:
    typedef SampleObjectWithCollection SelfType;

    static constexpr char const __coal_typename__[] = "SampleObjectWithCollection";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"list", &SelfType::list},
            {"set", &SelfType::set},
            {"map", &SelfType::map},
        };
    }

    std::vector<std::shared_ptr<SampleObject>> list;
    std::unordered_set<std::shared_ptr<SampleObject>> set;
    std::unordered_map<std::string, std::shared_ptr<SampleObject>> map;
};


int main()
{
    // Primitive values
    {
        writeDataToFile("boolean8-true.coal", coal::serialize(true));
        writeDataToFile("boolean8-false.coal", coal::serialize(false));

        writeDataToFile("uint8-42.coal", coal::serialize<uint8_t> (42));
        writeDataToFile("uint16-42.coal", coal::serialize<uint16_t> (42));
        writeDataToFile("uint32-42.coal", coal::serialize<uint32_t> (42));
        writeDataToFile("uint64-42.coal", coal::serialize<uint64_t> (42));

        writeDataToFile("int8-m42.coal", coal::serialize<int8_t> (-42));
        writeDataToFile("int16-m42.coal", coal::serialize<int16_t> (-42));
        writeDataToFile("int32-m42.coal", coal::serialize<int32_t> (-42));
        writeDataToFile("int64-m42.coal", coal::serialize<int64_t> (-42));

        writeDataToFile("float32-42.5.coal", coal::serialize<float> (42.5f));
        writeDataToFile("float64-42.5.coal", coal::serialize<double> (42.5));

        writeDataToFile("utf8_32_32-hello.coal", coal::serialize<std::string> ("Hello World\r\n"));

        writeDataToFile("array32-1-2-3-3-42.coal", coal::serialize(std::vector<int>{1, 2, 3, 3, 42}));
        writeDataToFile("array32-Hello-World-crlf.coal", coal::serialize(std::vector<std::string>{"Hello", "World", "\r\n"}));

        writeDataToFile("set32-1-2-3-42.coal", coal::serialize(std::unordered_set<int>{1, 2, 3, 3, 42}));
        writeDataToFile("set32-Hello-World-crlf.coal", coal::serialize(std::unordered_set<std::string>{"Hello", "World", "\r\n"}));

        writeDataToFile("map32-First-1-Second-2-Third-3.coal", coal::serialize(std::map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}}));
    }

    // Structure
    {
        writeDataToFile("sample-structure-empty.coal", coal::serialize(SampleStructure{}));
        writeDataToFile("sample-structure-non-empty.coal", coal::serialize(SampleStructure{{}, true, -42, 42.5f}));

        writeDataToFile("sample-nested-structure-empty.coal", coal::serialize(SampleNestedStructure{}));
        writeDataToFile("sample-nested-structure-non-empty.coal", coal::serialize(SampleNestedStructure{{}, {{}, true, -42, 42.5f}, 13}));
    }

    return 0;
}