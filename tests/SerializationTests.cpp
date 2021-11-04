#include "coal-serialization/coal.hpp"
#include <stdexcept>
#include <iostream>

#define guardException(block) try block \
    catch(std::exception &e) { \
        std::cout << __FILE__ << ":" << __LINE__ << ": Exception caught: " << e.what() << std::endl; \
        ++testErrorCount; \
    }

#define assertEquals(expected, obtained) guardException({ \
    const auto expectedValue = expected; \
    const auto obtainedValue = obtained; \
    if(!(expectedValue == obtainedValue)) { \
        std::cout << __FILE__ << ":" << __LINE__ << ": Assertion failure: " << obtainedValue << " is not equal to the expected value of " << expectedValue << std::endl; \
        ++testErrorCount; \
    } }) \

template<typename T>
std::ostream &operator<<(std::ostream &out, const std::vector<T> &v)
{
    out << '{';
    bool first = true;
    for(const auto &e : v)
    {
        if(first)
            first = false;
        else
            out << ", ";
        out << '"' << e << '"';
    }
    out << '}';
    return out;
}

template<typename T>
std::ostream &operator<<(std::ostream &out, const std::set<T> &v)
{
    out << '{';
    bool first = true;
    for(const auto &e : v)
    {
        if(first)
            first = false;
        else
            out << ", ";
        out << '"' << e << '"';
    }
    out << '}';
    return out;
}

template<typename T>
std::ostream &operator<<(std::ostream &out, const std::unordered_set<T> &v)
{
    out << '{';
    bool first = true;
    for(const auto &e : v)
    {
        if(first)
            first = false;
        else
            out << ", ";
        out << '"' << e << '"';
    }
    out << '}';
    return out;
}

template<typename K, typename V>
std::ostream &operator<<(std::ostream &out, const std::map<K, V> &v)
{
    out << '{';
    bool first = true;
    for(const auto &e : v)
    {
        if(first)
            first = false;
        else
            out << ", ";
        out << "{'" << e.first << "', '" << e.second << "'}";
    }
    out << '}';
    return out;
}

template<typename K, typename V>
std::ostream &operator<<(std::ostream &out, const std::unordered_map<K, V> &v)
{
    out << '{';
    bool first = true;
    for(const auto &e : v)
    {
        if(first)
            first = false;
        else
            out << ", ";
        out << "{'" << e.first << "', '" << e.second << "'}";
    }
    out << '}';
    return out;
}

/**
 * Sample structure with inline Coal serialization specs.
 */
struct TestStructure : public coal::SerializableStructureTag
{
    typedef TestStructure SelfType;

    static constexpr char const __coal_typename__[] = "TestStructure";

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

/**
 * Sample nested structure with inline Coal serialization specs.
 */
struct TestNestedStructure : public coal::SerializableStructureTag
{
    typedef TestNestedStructure SelfType;

    static constexpr char const __coal_typename__[] = "TestNestedStructure";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"innerStruct", &SelfType::innerStruct},
            {"integerField", &SelfType::integerField},
        };
    }

    TestStructure innerStruct = {};
    int integerField = 0;

    bool operator==(const TestNestedStructure &other) const
    {
        return innerStruct == other.innerStruct
            && integerField == other.integerField;
    }

    friend std::ostream &operator<<(std::ostream &out, const TestNestedStructure &value)
    {
        out << '{' << value.innerStruct << ", " << value.integerField << "}";
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

/**
 * Sample nested structure with inline Coal serialization specs.
 */
struct TestNestedStructureWithDifferentOrder
{
    int integerField = 0;
    TestStructureWithDifferentOrder innerStruct = {};

    bool operator==(const TestNestedStructureWithDifferentOrder &other) const
    {
        return innerStruct == other.innerStruct
            && integerField == other.integerField;
    }

    friend std::ostream &operator<<(std::ostream &out, const TestNestedStructureWithDifferentOrder &value)
    {
        out << '{' << value.integerField << ", " << value.innerStruct << "}";
        return out;
    }
};

/**
 * TestSharedObject
 */
class TestSharedObject : public coal::SerializableSharedObjectClassTag
{
public:
    typedef TestSharedObject SelfType;

    static constexpr char const __coal_typename__[] = "TestSharedObject";

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

    bool operator==(const TestSharedObject &other) const
    {
        return booleanField == other.booleanField
            && integerField == other.integerField
            && floatField == other.floatField;
    }

    friend std::ostream &operator<<(std::ostream &out, const TestSharedObject &object)
    {
        out << '{' << object.booleanField << ", " << object.integerField << ", " << object.floatField << "}";
        return out;
    }
};

/**
 * TestSharedObjectOuter
 */
class TestSharedObjectOuter : public coal::SerializableSharedObjectClassTag
{
public:
    typedef TestSharedObjectOuter SelfType;

    static constexpr char const __coal_typename__[] = "TestSharedObjectOuter";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"innerObject", &SelfType::innerObject},
        };
    }

    std::shared_ptr<TestSharedObject> innerObject;

    bool operator==(const TestSharedObjectOuter &other) const
    {
        return bool(innerObject) == bool(other.innerObject) && (!innerObject || *innerObject == *other.innerObject);
    }

    friend std::ostream &operator<<(std::ostream &out, const TestSharedObjectOuter &object)
    {
        out << '{';
        if(object.innerObject)
            out << *object.innerObject;
        else
            out << "nullptr";
        out << "}";
        return out;
    }
};

namespace coal
{
template<>
struct StructureTypeMetadataFor<TestStructureWithDifferentOrder>
{
    typedef void type;

    static FieldDescriptions getFields()
    {
        return {
            {"booleanField", &TestStructureWithDifferentOrder::booleanField},
            {"integerField", &TestStructureWithDifferentOrder::integerField},
            {"floatField", &TestStructureWithDifferentOrder::floatField},
        };
    }

    static std::string getTypeName()
    {
        return "TestStructure";
    }
};

template<>
struct StructureTypeMetadataFor<TestNestedStructureWithDifferentOrder>
{
    typedef void type;

    static FieldDescriptions getFields()
    {
        return {
            {"integerField", &TestNestedStructureWithDifferentOrder::integerField},
            {"innerStruct", &TestNestedStructureWithDifferentOrder::innerStruct},
        };
    }

    static std::string getTypeName()
    {
        return "TestNestedStructure";
    }
};

}

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

        assertEquals("Hello World\n\r", coal::deserialize<std::string> (coal::serialize<std::string> ("Hello World\n\r")).value());

        assertEquals((std::vector<int>{1, 2, 3, 3, 42}), coal::deserialize<std::vector<int>> (coal::serialize(std::vector<int>{1, 2, 3, 3, 42})).value());
        assertEquals((std::vector<std::string>{"Hello", "World", "\r\n"}), coal::deserialize<std::vector<std::string>> (coal::serialize(std::vector<std::string>{"Hello", "World", "\r\n"})).value());

        assertEquals((std::set<int>{1, 2, 3, 42}), coal::deserialize<std::set<int>> (coal::serialize(std::set<int>{1, 2, 3, 42})).value());
        assertEquals((std::set<std::string>{"Hello", "World", "\r\n"}), coal::deserialize<std::set<std::string>> (coal::serialize(std::set<std::string>{"Hello", "World", "\r\n"})).value());
        assertEquals((std::unordered_set<int>{1, 2, 3, 42}), coal::deserialize<std::unordered_set<int>> (coal::serialize(std::unordered_set<int>{1, 2, 3, 42})).value());
        assertEquals((std::unordered_set<std::string>{"Hello", "World", "\r\n"}), coal::deserialize<std::unordered_set<std::string>> (coal::serialize(std::unordered_set<std::string>{"Hello", "World", "\r\n"})).value());

        assertEquals((std::set<int>{1, 2, 3, 42}), coal::deserialize<std::set<int>> (coal::serialize(std::unordered_set<int>{1, 2, 3, 42})).value());
        assertEquals((std::set<std::string>{"Hello", "World", "\r\n"}), coal::deserialize<std::set<std::string>> (coal::serialize(std::unordered_set<std::string>{"Hello", "World", "\r\n"})).value());
        assertEquals((std::unordered_set<int>{1, 2, 3, 42}), coal::deserialize<std::unordered_set<int>> (coal::serialize(std::set<int>{1, 2, 3, 42})).value());
        assertEquals((std::unordered_set<std::string>{"Hello", "World", "\r\n"}), coal::deserialize<std::unordered_set<std::string>> (coal::serialize(std::set<std::string>{"Hello", "World", "\r\n"})).value());

        assertEquals((std::map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}}), (coal::deserialize<std::map<std::string, int>> (coal::serialize(std::map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}})).value()));
        assertEquals((std::unordered_map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}}), (coal::deserialize<std::unordered_map<std::string, int>> (coal::serialize(std::unordered_map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}})).value()));

        assertEquals((std::map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}}), (coal::deserialize<std::map<std::string, int>> (coal::serialize(std::unordered_map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}})).value()));
        assertEquals((std::unordered_map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}}), (coal::deserialize<std::unordered_map<std::string, int>> (coal::serialize(std::map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}})).value()));
    }

    // Structure
    {
        assertEquals(TestStructure{}, coal::deserialize<TestStructure> (coal::serialize(TestStructure{})).value());
        assertEquals(TestStructureWithDifferentOrder{}, coal::deserialize<TestStructureWithDifferentOrder> (coal::serialize(TestStructureWithDifferentOrder{})).value());
        assertEquals(TestNestedStructure{}, coal::deserialize<TestNestedStructure> (coal::serialize(TestNestedStructure{})).value());
        assertEquals(TestNestedStructureWithDifferentOrder{}, coal::deserialize<TestNestedStructureWithDifferentOrder> (coal::serialize(TestNestedStructureWithDifferentOrder{})).value());

        assertEquals((TestStructure{{}, true, -42, 42.5f}), coal::deserialize<TestStructure> (coal::serialize(TestStructure{{}, true, -42, 42.5f})).value());
        assertEquals((TestStructureWithDifferentOrder{-42, 42.5f, true}), coal::deserialize<TestStructureWithDifferentOrder> (coal::serialize(TestStructureWithDifferentOrder{-42, 42.5f, true})).value());
        assertEquals((TestNestedStructure{{}, {{}, true, -42, 42.5f}, 13}), coal::deserialize<TestNestedStructure> (coal::serialize(TestNestedStructure{{}, {{}, true, -42, 42.5f}, 13})).value());
        assertEquals((TestNestedStructureWithDifferentOrder{13, {-42, 42.5f, true}}), coal::deserialize<TestNestedStructureWithDifferentOrder> (coal::serialize(TestNestedStructureWithDifferentOrder{13, {-42, 42.5f, true}})).value());

        assertEquals((TestStructureWithDifferentOrder{-42, 42.5, true}), coal::deserialize<TestStructureWithDifferentOrder> (coal::serialize(TestStructure{{}, true, -42, 42.5f})).value());
        assertEquals((TestStructure{{}, true, -42, 42.5f}), coal::deserialize<TestStructure> (coal::serialize(TestStructureWithDifferentOrder{-42, 42.5, true})).value());

        assertEquals((TestNestedStructureWithDifferentOrder{13, {-42, 42.5f, true}}), coal::deserialize<TestNestedStructureWithDifferentOrder> (coal::serialize(TestNestedStructure{{}, {{}, true, -42, 42.5f}, 13})).value());
        assertEquals((TestNestedStructure{{}, {{}, true, -42, 42.5f}, 13}), coal::deserialize<TestNestedStructure> (coal::serialize(TestNestedStructureWithDifferentOrder{13, {-42, 42.5f, true}})).value());
    }

    // TestSharedObject empty
    {
        auto object = std::make_shared<TestSharedObject> ();
        auto serialized = coal::serialize(object);
        auto materializedObject = coal::deserialize<std::shared_ptr<TestSharedObject>> (serialized).value();
        assertEquals(*object, *materializedObject);
    }

    // TestSharedObject non-empty
    {
        auto object = std::make_shared<TestSharedObject> ();
        object->booleanField = true;
        object->integerField = -42;
        object->floatField = 42.5f;
        
        auto serialized = coal::serialize(object);
        auto materializedObject = coal::deserialize<std::shared_ptr<TestSharedObject>> (serialized).value();
        assertEquals(*object, *materializedObject);
    }

    // TestSharedObjectOuter empty
    {
        auto object = std::make_shared<TestSharedObjectOuter> ();
        auto serialized = coal::serialize(object);
        auto materializedObject = coal::deserialize<std::shared_ptr<TestSharedObjectOuter>> (serialized).value();
        assertEquals(*object, *materializedObject);
    }

    // TestSharedObjectOuter non empty
    {
        auto innerObject = std::make_shared<TestSharedObject> ();
        innerObject->booleanField = true;
        innerObject->integerField = -42;
        innerObject->floatField = 42.5f;

        auto object = std::make_shared<TestSharedObjectOuter> ();
        object->innerObject = innerObject;
        auto serialized = coal::serialize(object);
        auto materializedObject = coal::deserialize<std::shared_ptr<TestSharedObjectOuter>> (serialized).value();
        assertEquals(*object, *materializedObject);
    }

    if(testErrorCount > 0)
        std::cerr << testErrorCount << " test failures" << std::endl;
    return testErrorCount > 0 ? 1 : 0;
}