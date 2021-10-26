/**
 * The MIT License (MIT)
 * Copyright (c) 2021 Desarrollo de Software Ronie Salgado Faila E.I.R.L.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef COAL_SERIALIZATION_COAL_HPP
#define COAL_SERIALIZATION_COAL_HPP

#pragma once

#include <cstdint>
#include <memory>
#include <array>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <functional>
#include <iostream>

namespace coal
{

static constexpr uint32_t CoalMagicNumber = 0x4C414F43;
static constexpr uint8_t CoalVersionMajor = 1;
static constexpr uint8_t CoalVersionMinor = 0;

class StructType;
typedef std::shared_ptr<StructType> StructTypePtr;

class TypeDescriptor;
typedef std::shared_ptr<TypeDescriptor> TypeDescriptorPtr;

class ClusterDescription;
typedef std::shared_ptr<ClusterDescription> ClusterDescriptionPtr;

class ObjectMapper;
typedef std::shared_ptr<ObjectMapper> ObjectMapperPtr;

class SerializationCluster;
typedef std::shared_ptr<SerializationCluster> SerializationClusterPtr;

class FieldAccessor;
typedef std::shared_ptr<FieldAccessor> FieldAccessorPtr;

struct WriteStream;
struct ReadStream;

/**
 * The kind of a coal type descriptor.
 */
enum class TypeDescriptorKind : uint8_t
{
    Object = 0x00,
    Boolean8 = 0x01,
    Boolean16 = 0x02,
    Boolean32 = 0x03,
    Boolean64 = 0x04,
    UInt8 = 0x05,
    UInt16 = 0x06,
    UInt32 = 0x07,
    UInt64 = 0x08,
    UInt128 = 0x09,
    Int8 = 0x0A,
    Int16 = 0x0B,
    Int32 = 0x0C,
    Int64 = 0x0D,
    Int128 = 0x0E,
    Float16 = 0x0F,
    Float32 = 0x10,
    Float64 = 0x11,
    Float128 = 0x12,
    Float256 = 0x13,
    Decimal32 = 0x14,
    Decimal64 = 0x15,
    Decimal128 = 0x16,
    Binary_32_8 = 0x17,
    Binary_32_16 = 0x18,
    Binary_32_32 = 0x19,
    UTF8_32_8 = 0x1A,
    UTF8_32_16 = 0x1B,
    UTF8_32_32 = 0x1C,
    UTF16_32_8 = 0x1D,
    UTF16_32_16 = 0x1E,
    UTF16_32_32 = 0x1F,
    UTF32_32_8 = 0x20,
    UTF32_32_16 = 0x21,
    UTF32_32_32 = 0x22,
    BigInt_32_8 = 0x23,
    BigInt_32_16 = 0x24,
    BigInt_32_32 = 0x25,
    Char8 = 0x26,
    Char16 = 0x27,
    Char32 = 0x28,

    Struct = 0x80,
    FixedArray = 0x81,
    Array8 = 0x82,
    Array16 = 0x83,
    Array32 = 0x84,
    Set8 = 0x85,
    Set16 = 0x86,
    Set32 = 0x87,
    Map8 = 0x88,
    Map16 = 0x89,
    Map32 = 0x8A,

    PrimitiveTypeDescriptorCount = Char32 + 1,
};

/**
 * Binary blob builder
 */
class BinaryBlobBuilder
{
public:
    std::vector<uint8_t> data;

    size_t pushBytes(const uint8_t *bytes, size_t dataSize)
    {
        if(bytes == 0)
            return 0;
        
        // TODO: Use a proper hash table for this case.
        auto result = data.size();
        data.insert(data.end(), bytes, bytes + dataSize);
        return result;
    }

    size_t internString(const std::string &string)
    {
        if(string.empty())
            return 0;

        auto it = internedStrings.find(string);
        if(it != internedStrings.end())
            return it->second;
        
        auto result = pushBytes(reinterpret_cast<const uint8_t*> (string.data()), string.size());
        internedStrings[string] = result;
        return result;
    }

private:
    std::unordered_map<std::string, size_t> internedStrings;
};

/**
 * Interface for a write stream.
 */
class WriteStream
{
public:
    virtual ~WriteStream() {}

    virtual void writeBytes(const uint8_t *data, size_t size) = 0;

    void writeUInt8(uint8_t value)
    {
        writeBytes(&value, 1);
    }
    
    void writeUInt16(uint16_t value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 2);
    }
    
    void writeUInt32(uint32_t value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 4);
    }

    void writeUInt64(uint64_t value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 8);
    }

    void writeInt8(int8_t value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 1);
    }
    
    void writeInt16(int16_t value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 2);
    }
    
    void writeInt32(int32_t value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 4);
    }

    void writeInt64(int64_t value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 8);
    }

    void writeFloat32(float value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 4);
    }

    void writeFloat64(float value)
    {
        writeBytes(reinterpret_cast<uint8_t*> (&value), 8);
    }

    void writeBlob(const BinaryBlobBuilder *theBlob)
    {
        blob = theBlob;
        writeBytes(blob->data.data(), blob->data.size());
    }

private:
    const BinaryBlobBuilder *blob = nullptr;
};

/**
 * Interface for a read stream.
 */
struct ReadStream
{
    virtual ~ReadStream() {}
};

/**
 * Memory write stream
 */
class MemoryWriteStream : public WriteStream
{
public:
    MemoryWriteStream(std::vector<uint8_t> &initialOutput)
        : output(initialOutput) {}

    void writeBytes(const uint8_t *data, size_t size)
    {
        output.insert(output.end(), data, data + size);
    }
private:
    std::vector<uint8_t> &output;
};

/**
 * Type descriptor
 */
class TypeDescriptor
{
public:
    virtual ~TypeDescriptor() {}

    TypeDescriptorKind kind;
};

/**
 * Structure type descriptor
 */
class StructTypeDescriptor : public TypeDescriptor
{
public:
    StructTypePtr structure;
};

/**
 * Fixed array type descriptor
 */
class FixedArrayTypeDescriptor : public TypeDescriptor
{
public:
    uint32_t size;
    TypeDescriptorPtr element;
};

/**
 * Array type descriptor
 */
class ArrayTypeDescriptor : public TypeDescriptor
{
public:
    TypeDescriptorPtr element;
};

/**
 * Set type descriptor
 */
class SetTypeDescriptor : public TypeDescriptor
{
public:
    TypeDescriptorPtr element;
};

/**
 * Map type descriptor
 */
class MapTypeDescriptor : public TypeDescriptor
{
public:
    TypeDescriptorPtr key;
    TypeDescriptorPtr value;
};

/**
 * TypeDescriptorContext
 */
class TypeDescriptorContext
{
public:
    TypeDescriptorPtr getOrCreatePrimitiveTypeDescriptor(TypeDescriptorKind kind)
    {
        auto &descriptor = primitiveTypeDescriptors[uint8_t(kind)];
        if(!descriptor)
        {
            descriptor = std::make_shared<TypeDescriptor> ();
            descriptor->kind = kind;
        }

        return descriptor;
    }

    template<typename T>
    TypeDescriptorPtr getForType();

private:
    std::array<TypeDescriptorPtr, uint8_t(TypeDescriptorKind::PrimitiveTypeDescriptorCount)> primitiveTypeDescriptors;
};

template<typename T, typename C = void>
struct TypeDescriptorFor;

template<TypeDescriptorKind Kind >
struct PrimitiveTypeDescriptorForKind
{
    static TypeDescriptorPtr apply(TypeDescriptorContext *context)
    {
        return context->getOrCreatePrimitiveTypeDescriptor(Kind);
    }
};

template<>
struct TypeDescriptorFor<bool> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Boolean8> {};

template<>
struct TypeDescriptorFor<uint8_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::UInt8> {};

template<>
struct TypeDescriptorFor<uint16_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::UInt16> {};

template<>
struct TypeDescriptorFor<uint32_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::UInt32> {};

template<>
struct TypeDescriptorFor<uint64_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::UInt64> {};

template<>
struct TypeDescriptorFor<int8_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Int8> {};

template<>
struct TypeDescriptorFor<int16_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Int16> {};

template<>
struct TypeDescriptorFor<int32_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Int32> {};

template<>
struct TypeDescriptorFor<int64_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Int64> {};

template<>
struct TypeDescriptorFor<char> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Char8> {};

template<>
struct TypeDescriptorFor<char16_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Char16> {};

template<>
struct TypeDescriptorFor<char32_t> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Char32> {};

template<>
struct TypeDescriptorFor<float> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Float32> {};

template<>
struct TypeDescriptorFor<double> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::Float64> {};

template<>
struct TypeDescriptorFor<std::string> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::UTF8_32_32> {};

template<>
struct TypeDescriptorFor<std::u16string> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::UTF16_32_32> {};

template<>
struct TypeDescriptorFor<std::u32string> : PrimitiveTypeDescriptorForKind<TypeDescriptorKind::UTF32_32_32> {};

template<typename T>
inline TypeDescriptorPtr TypeDescriptorContext::getForType()
{
    return TypeDescriptorFor<T>::apply(this);
}

/**
 * Field desriptor
 */
struct FieldDescriptor
{
    std::string name;
    TypeDescriptorPtr typeDescriptor;
    FieldAccessorPtr fieldAccessor;

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
    {
        binaryBlobBuilder.internString(name);
    }
};

typedef std::function<void (const FieldDescriptor &)> FieldDescriptorIterationBlock;

/**
 * Object mapper interface.
 * I am an interface used for gluing different kinds of objects with serializer and deserializer.
 */
class ObjectMapper
{
public:
    virtual ~ObjectMapper() {};

    virtual std::string getObjectTypeName() const = 0;
    virtual void fieldDescriptorsDo(TypeDescriptorContext *typeDescriptorContext, const FieldDescriptorIterationBlock &aBlock) = 0;
};

/**
 * I am an accessor for a field.
 */
class FieldAccessor
{
public:
    virtual ~FieldAccessor() {}
};

/**
 * I am an accessor for a member field.
 */
template<typename CT, typename MT>
class MemberFieldAccessor : public FieldAccessor
{
public:
    typedef CT ClassType;
    typedef MT MemberType;
    typedef MemberType ClassType::*MemberPointerType;

    MemberFieldAccessor(MemberPointerType initialMemberPointer)
        : memberPointer(initialMemberPointer) {}

    MemberPointerType memberPointer;
};

template<typename CT, typename MT>
FieldAccessorPtr memberFieldAccessorFor(MT CT::*fieldPointer)
{
    return std::make_shared<MemberFieldAccessor<CT, MT>> (fieldPointer);
};

/**
 * I am a default value type box object.
 */
template<typename VT>
class ValueBoxObject : public ObjectMapper
{
public:
    typedef VT ValueType;
    typedef ValueBoxObject<VT> ThisType;

    ValueBoxObject(const VT &initialValue = VT())
        : value(initialValue) {}

    std::string getObjectTypeName() const override
    {
        return "ValueBox";
    }

    void fieldDescriptorsDo(TypeDescriptorContext *typeDescriptorContext, const FieldDescriptorIterationBlock &aBlock) override
    {
        aBlock(FieldDescriptor{
            "value",
            typeDescriptorContext->getForType<VT> (),
            memberFieldAccessorFor(&ThisType::value)
        });
    }

    VT value;
};

/**
 * Tag for marking a serializable structure.
 */
struct SerializableStructureTag {};

/**
 * Tag for marking a serializable object class.
 */
struct SerializableObjectClassTag {};

/**
 * Makes an object mapper interface for the specified object.
 */
template<typename T, typename C=void>
struct ObjectMapperFor;

template<typename T>
struct ValueBoxObjectMapperFor
{
    static ObjectMapperPtr apply(const T &value)
    {
        return std::make_shared<ValueBoxObject<T>> (value);
    }    
};

template<>
struct ObjectMapperFor<bool> : ValueBoxObjectMapperFor<bool> {};

template<>
struct ObjectMapperFor<uint8_t> : ValueBoxObjectMapperFor<uint8_t> {};

template<>
struct ObjectMapperFor<uint16_t> : ValueBoxObjectMapperFor<uint16_t> {};

template<>
struct ObjectMapperFor<uint32_t> : ValueBoxObjectMapperFor<uint32_t> {};

template<>
struct ObjectMapperFor<uint64_t> : ValueBoxObjectMapperFor<uint64_t> {};

template<>
struct ObjectMapperFor<int8_t> : ValueBoxObjectMapperFor<int8_t> {};

template<>
struct ObjectMapperFor<int16_t> : ValueBoxObjectMapperFor<int16_t> {};

template<>
struct ObjectMapperFor<int32_t> : ValueBoxObjectMapperFor<int32_t> {};

template<>
struct ObjectMapperFor<int64_t> : ValueBoxObjectMapperFor<int64_t> {};

template<>
struct ObjectMapperFor<char> : ValueBoxObjectMapperFor<char> {};

template<>
struct ObjectMapperFor<char16_t> : ValueBoxObjectMapperFor<char16_t> {};

template<>
struct ObjectMapperFor<char32_t> : ValueBoxObjectMapperFor<char32_t> {};

template<>
struct ObjectMapperFor<float> : ValueBoxObjectMapperFor<float> {};

template<>
struct ObjectMapperFor<double> : ValueBoxObjectMapperFor<double> {};

template<>
struct ObjectMapperFor<std::string> : ValueBoxObjectMapperFor<std::string> {};

/**
 * Serialization cluster
 */
class SerializationCluster
{
public:
    std::string name;
    std::vector<ObjectMapperPtr> instances;
    std::vector<FieldDescriptor> fieldDescriptors;
    std::vector<size_t> objectFieldDescriptors;

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
    {
        binaryBlobBuilder.internString(name);
        for(auto &descriptor : fieldDescriptors)
            descriptor.pushDataIntoBinaryBlob(binaryBlobBuilder);
    }

    void addFieldDescriptor(const FieldDescriptor &descriptor)
    {
        fieldDescriptors.push_back(descriptor);
    }

    void addObject(const ObjectMapperPtr &object)
    {
        instances.push_back(object);
    }
};

/**
 * Coal serializer.
 */
class Serializer
{
public:
    Serializer(WriteStream *initialOutput)
        : output(initialOutput)
    {
    }

    template<typename ROT>
    void serializeRootObjectOrValue(ROT &&root)
    {
        serializeRootObject(ObjectMapperFor<ROT>::apply(root));
    }

    void serializeRootObject(const ObjectMapperPtr &object)
    {
        addPendingObject(object);
        tracePendingObjects();

        prepareForWriting();

        writeHeader();
        writeBlob();
        writeValueTypeLayouts();
        writeClusterDescriptions();
        writeClusterInstances();
    }

private:
    void addPendingObject(const ObjectMapperPtr &object)
    {
        if(seenSet.find(object) != seenSet.end())
            return;
        
        tracingStack.push_back(object);
        seenSet.insert(object);
    }

    void tracePendingObjects()
    {
        while(!tracingStack.empty())
        {
            auto pendingObject = tracingStack.back();
            tracingStack.pop_back();
            tracePendingObject(pendingObject);
        }
    }

    void tracePendingObject(const ObjectMapperPtr &object)
    {
        auto cluster = getOrCreateClusterFor(object);
        cluster->addObject(object);
    }

    SerializationClusterPtr getOrCreateClusterFor(const ObjectMapperPtr &object)
    {
        auto typeName = object->getObjectTypeName();
        auto it = objectTypeNameToClusters.find(typeName);
        if(it != objectTypeNameToClusters.end())
            return it->second;

        auto newCluster = std::make_shared<SerializationCluster> ();
        newCluster->name = typeName;
        object->fieldDescriptorsDo(&typeDescriptorContext, [&](const FieldDescriptor &fieldDescriptor) {
            newCluster->addFieldDescriptor(fieldDescriptor);
        });

        clusters.push_back(newCluster);
        objectTypeNameToClusters.insert(std::make_pair(typeName, newCluster));
        return newCluster;
    }

    void writeHeader()
    {
        output->writeUInt32(CoalMagicNumber);
        output->writeUInt8(CoalVersionMajor);
        output->writeUInt8(CoalVersionMinor);
        output->writeUInt16(0); // Reserved

        output->writeUInt32(binaryBlobBuilder.data.size()); // Blob size
        output->writeUInt32(0); // TODO: Value type layouts size
        output->writeUInt32(clusters.size()); // Cluster Count
        output->writeUInt32(objectCount); // Cluster Count
    }

    void writeBlob()
    {
        output->writeBlob(&binaryBlobBuilder);
    }

    void writeValueTypeLayouts()
    {
    }

    void writeClusterDescriptions()
    {
    }

    void writeClusterInstances()
    {
    }

    void prepareForWriting()
    {
        objectCount = 0;
        for(auto & cluster : clusters)
        {
            cluster->pushDataIntoBinaryBlob(binaryBlobBuilder);
            for(auto instance : cluster->instances)
                objectToInstanceIndexTable[instance] = objectCount++;
        }
    }

    WriteStream *output;

    TypeDescriptorContext typeDescriptorContext;
    BinaryBlobBuilder binaryBlobBuilder;
    size_t objectCount;
    std::vector<SerializationClusterPtr> clusters;
    std::unordered_map<std::string, SerializationClusterPtr> objectTypeNameToClusters;

    std::vector<ObjectMapperPtr> tracingStack;
    std::unordered_set<ObjectMapperPtr> seenSet;
    std::unordered_map<ObjectMapperPtr, size_t> objectToInstanceIndexTable;
};

/**
 * Coal deserializer.
 */
class Deserializer
{
public:
};

/**
 * Convenience method for serializing Coal objects and values.
 */
template<typename VT>
std::vector<uint8_t> serialize(VT &&value)
{
    std::vector<uint8_t> result;
    MemoryWriteStream output(result);
    Serializer serializer(&output);
    serializer.serializeRootObjectOrValue(std::forward<VT> (value));
    return result;
}

/**
 * Convenience method for deserializing Coal objects and values.
 */
template<typename RT>
RT deserialize(const std::vector<uint8_t> &data)
{
    (void)data;
    return RT();
}

} // End of namespace coal

#endif //COAL_SERIALIZATION_COAL_HPP