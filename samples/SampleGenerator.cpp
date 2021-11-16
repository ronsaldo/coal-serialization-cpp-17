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
class SampleObject : public coal::MakeSerializableSharedSubclassOf<SampleObject, void>
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
class SampleObjectOuter : public coal::MakeSerializableSharedSubclassOf<SampleObjectOuter, void>
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
class SampleCyclicObject : public coal::MakeSerializableSharedSubclassOf<SampleCyclicObject, void>
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
class SampleObjectWithCollection : public coal::MakeSerializableSharedSubclassOf<SampleObjectWithCollection, void>
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

class SampleShape : public coal::MakeSerializableSharedSubclassOf<SampleShape, void>
{
public:
    static constexpr char const __coal_typename__[] = "Shape";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"centerX", &SelfType::centerX},
            {"centerY", &SelfType::centerY},
        };
    }

    virtual bool isCircle() const
    {
        return false;
    }

    virtual bool isBox() const
    {
        return false;
    }

    float centerX;
    float centerY;
};

class SampleCircle : public coal::MakeSerializableSharedSubclassOf<SampleCircle, SampleShape>
{
public:
    static constexpr char const __coal_typename__[] = "Circle";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"radius", &SelfType::radius}
        };
    }

    virtual bool isCircle() const override
    {
        return true;
    }

    float radius;
};

class SampleBox : public coal::MakeSerializableSharedSubclassOf<SampleBox, SampleShape>
{
public:
    static constexpr char const __coal_typename__[] = "Box";

    static coal::FieldDescriptions __coal_fields__()
    {
        return {
            {"width", &SelfType::width},
            {"height", &SelfType::height}
        };
    }

    virtual bool isBox() const
    {
        return true;
    }

    float width;
    float height;
};

typedef std::shared_ptr<SampleShape> SampleShapePtr;
typedef std::vector<SampleShapePtr> SampleShapePtrList;


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

    // SampleObject empty
    {
        auto object = std::make_shared<SampleObject> ();
        writeDataToFile("sample-object-empty.coal", coal::serialize(object));
    }

    // SampleObject non-empty
    {
        auto object = std::make_shared<SampleObject> ();
        object->booleanField = true;
        object->integerField = -42;
        object->floatField = 42.5f;
        writeDataToFile("sample-object-non-empty.coal", coal::serialize(object));
    }

    // SampleObjectOuter empty
    {
        auto object = std::make_shared<SampleObjectOuter> ();
        writeDataToFile("sample-object-outer-empty.coal", coal::serialize(object));
    }

    // SampleObjectOuter non empty
    {
        auto innerObject = std::make_shared<SampleObject> ();
        innerObject->booleanField = true;
        innerObject->integerField = -42;
        innerObject->floatField = 42.5f;

        auto object = std::make_shared<SampleObjectOuter> ();
        object->innerObject = innerObject;
        writeDataToFile("sample-object-outer-non-empty.coal", coal::serialize(object));
    }

    // SampleCyclicObject no cycle
    {
        auto noCycle = std::make_shared<SampleCyclicObject> ();
        writeDataToFile("sample-cyclic-object-no-cycle.coal", coal::serialize(noCycle));
    }

    // SampleCyclicObject self cycle
    {
        auto selfCycle = std::make_shared<SampleCyclicObject> ();
        selfCycle->potentiallyCyclicReference = selfCycle;
        writeDataToFile("sample-cyclic-object-self-cycle.coal", coal::serialize(selfCycle));

        selfCycle->potentiallyCyclicReference.reset();
    }

    // SampleCyclicObject indirect cycle
    {
        auto first = std::make_shared<SampleCyclicObject> ();
        auto second = std::make_shared<SampleCyclicObject> ();
        first->potentiallyCyclicReference = second;
        second->potentiallyCyclicReference = first;
        second->potentiallyCyclicReference2 = second;
        writeDataToFile("sample-cyclic-object-indirect.coal", coal::serialize(first));
        second->potentiallyCyclicReference.reset();
        second->potentiallyCyclicReference2.reset();
    }
    
    // TestSharedObjectWithCollections empty
    {
        auto root = std::make_shared<SampleObjectWithCollection> ();
        writeDataToFile("sample-object-with-collections-empty.coal", coal::serialize(root));
    }

    // SampleObjectWithCollection
    {
        auto root = std::make_shared<SampleObjectWithCollection> ();
        auto firstObject = std::make_shared<SampleObject> ();
        firstObject->integerField = 1;
        firstObject->floatField = 1;

        auto secondObject = std::make_shared<SampleObject> ();
        secondObject->integerField = 2;
        secondObject->floatField = 2;

        auto thirdObject = std::make_shared<SampleObject> ();
        thirdObject->integerField = 3;
        thirdObject->floatField = 3;

        root->list.push_back(firstObject);
        root->list.push_back(secondObject);
        root->list.push_back(secondObject);
        root->list.push_back(thirdObject);

        root->set.insert(firstObject);
        root->set.insert(secondObject);
        root->set.insert(thirdObject);

        root->map.insert({"First", firstObject});
        root->map.insert({"Second", secondObject});
        root->map.insert({"Third", thirdObject});
        writeDataToFile("sample-object-with-collections-non-empty.coal", coal::serialize(root));
    }

    // Empty shape list
    {
        writeDataToFile("sample-shape-list-empty.coal", coal::serialize(SampleShapePtrList{}));
    }

    // Non empty shape list
    {
        SampleShapePtrList shapeList;

        {
            auto shape = std::make_shared<SampleBox> ();
            shape->centerX = -2;
            shape->centerY = -3;
            shape->width = 1;
            shape->height = 2;
            shapeList.push_back(shape);
        }

        {
            auto shape = std::make_shared<SampleCircle> ();
            shape->centerX = -5;
            shape->centerY = -6;
            shape->radius = 3;
            shapeList.push_back(shape);
        }

        writeDataToFile("sample-shape-list-non-empty.coal", coal::serialize(shapeList));
    }

    return 0;
}