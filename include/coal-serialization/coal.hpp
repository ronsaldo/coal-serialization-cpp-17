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
#include <cstring>
#include <cassert>
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>
#include <map> // for std::pair key
#include <unordered_set>
#include <algorithm>

#include <mutex>
#include <functional>

namespace coal
{

static constexpr uint32_t CoalMagicNumber = 0x4C414F43;
static constexpr uint8_t CoalVersionMajor = 1;
static constexpr uint8_t CoalVersionMinor = 0;

class TypeDescriptor;
typedef std::shared_ptr<TypeDescriptor> TypeDescriptorPtr;

class TypeDescriptorContext;

class TypeMapper;
typedef std::shared_ptr<TypeMapper> TypeMapperPtr;
typedef std::weak_ptr<TypeMapper> TypeMapperWeakPtr;

class TypeMapperRegistry;
typedef std::shared_ptr<TypeMapperRegistry> TypeMapperRegistryPtr;
typedef std::weak_ptr<TypeMapperRegistry> TypeMapperRegistryWeakPtr;

class ObjectMaterializationTypeMapper;
typedef std::shared_ptr<ObjectMaterializationTypeMapper> ObjectMaterializationTypeMapperPtr;
typedef std::weak_ptr<ObjectMaterializationTypeMapper> ObjectMaterializationTypeMapperWeakPtr;

class ObjectMapper;
typedef std::shared_ptr<ObjectMapper> ObjectMapperPtr;

class SerializationCluster;
typedef std::shared_ptr<SerializationCluster> SerializationClusterPtr;
typedef std::weak_ptr<SerializationCluster> SerializationClusterWeakPtr;

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
    Fixed16_16 = 0x29,
    Fixed16_16_Sat = 0x2A,

    Struct = 0x80,
    TypedObject = 0x81,
    FixedArray = 0x82,
    Array8 = 0x83,
    Array16 = 0x84,
    Array32 = 0x85,
    Set8 = 0x86,
    Set16 = 0x87,
    Set32 = 0x88,
    Map8 = 0x89,
    Map16 = 0x8A,
    Map32 = 0x8B,

    PrimitiveTypeDescriptorCount = Char32 + 1,
};

inline const char *typeDescriptorKindToString(TypeDescriptorKind kind)
{
    switch(kind)
    {
    case TypeDescriptorKind::Object: return "Object";
    case TypeDescriptorKind::Boolean8: return "Boolean8";
    case TypeDescriptorKind::Boolean16: return "Boolean16";
    case TypeDescriptorKind::Boolean32: return "Boolean32";
    case TypeDescriptorKind::Boolean64: return "Boolean64";
    case TypeDescriptorKind::UInt8: return "UInt8";
    case TypeDescriptorKind::UInt16: return "UInt16";
    case TypeDescriptorKind::UInt32: return "UInt32";
    case TypeDescriptorKind::UInt64: return "UInt64";
    case TypeDescriptorKind::UInt128: return "UInt128";
    case TypeDescriptorKind::Int8: return "Int8";
    case TypeDescriptorKind::Int16: return "Int16";
    case TypeDescriptorKind::Int32: return "Int32";
    case TypeDescriptorKind::Int64: return "Int64";
    case TypeDescriptorKind::Int128: return "Int128";
    case TypeDescriptorKind::Float16: return "Float16";
    case TypeDescriptorKind::Float32: return "Float32";
    case TypeDescriptorKind::Float64: return "Float64";
    case TypeDescriptorKind::Float128: return "Float128";
    case TypeDescriptorKind::Float256: return "Float256";
    case TypeDescriptorKind::Decimal32: return "Decimal32";
    case TypeDescriptorKind::Decimal64: return "Decimal64";
    case TypeDescriptorKind::Decimal128: return "Decimal128";
    case TypeDescriptorKind::Binary_32_8: return "Binary_32_8";
    case TypeDescriptorKind::Binary_32_16: return "Binary_32_16";
    case TypeDescriptorKind::Binary_32_32: return "Binary_32_32";
    case TypeDescriptorKind::UTF8_32_8: return "UTF8_32_8";
    case TypeDescriptorKind::UTF8_32_16: return "UTF8_32_16";
    case TypeDescriptorKind::UTF8_32_32: return "UTF8_32_32";
    case TypeDescriptorKind::UTF16_32_8: return "UTF16_32_8";
    case TypeDescriptorKind::UTF16_32_16: return "UTF16_32_16";
    case TypeDescriptorKind::UTF16_32_32: return "UTF16_32_32";
    case TypeDescriptorKind::UTF32_32_8: return "UTF32_32_8";
    case TypeDescriptorKind::UTF32_32_16: return "UTF32_32_16";
    case TypeDescriptorKind::UTF32_32_32: return "UTF32_32_32";
    case TypeDescriptorKind::BigInt_32_8: return "BigInt_32_8";
    case TypeDescriptorKind::BigInt_32_16: return "BigInt_32_16";
    case TypeDescriptorKind::BigInt_32_32: return "BigInt_32_32";
    case TypeDescriptorKind::Char8: return "Char8";
    case TypeDescriptorKind::Char16: return "Char16";
    case TypeDescriptorKind::Char32: return "Char32";
    case TypeDescriptorKind::Fixed16_16: return "Fixed16_16";
    case TypeDescriptorKind::Fixed16_16_Sat: return "Fixed16_16_Sat";
    case TypeDescriptorKind::Struct: return "Struct";
    case TypeDescriptorKind::TypedObject: return "TypedObject";
    case TypeDescriptorKind::FixedArray: return "FixedArray";
    case TypeDescriptorKind::Array8: return "Array8";
    case TypeDescriptorKind::Array16: return "Array16";
    case TypeDescriptorKind::Array32: return "Array32";
    case TypeDescriptorKind::Set8: return "Set8";
    case TypeDescriptorKind::Set16: return "Set16";
    case TypeDescriptorKind::Set32: return "Set32";
    case TypeDescriptorKind::Map8: return "Map8";
    case TypeDescriptorKind::Map16: return "Map16";
    case TypeDescriptorKind::Map32: return "Map32";
    default: abort();
    }
}
/**
 * Binary blob builder
 */
class BinaryBlobBuilder
{
public:
    std::vector<uint8_t> data;

    uint32_t getOffsetForBytes(const uint8_t *bytes, size_t dataSize) const
    {
        if(dataSize == 0)
            return 0;
        
        auto &bucket = hashTable[hashForBytes(bytes, dataSize) % HashTableCapacity];
        for(auto [entryOffset, entrySize] : bucket)
        {
            if(entrySize != dataSize)
                continue;

            if(memcmp(&data[entryOffset], bytes, dataSize) == 0)
                return entryOffset;
        }

        // Entry not found.
        abort();
    }

    void pushBytes(const uint8_t *bytes, size_t dataSize)
    {
        if(dataSize == 0)
            return;
        
        auto &bucket = hashTable[hashForBytes(bytes, dataSize) % HashTableCapacity];
        for(auto [entryOffset, entrySize] : bucket)
        {
            if(entrySize != dataSize)
                continue;

            if(memcmp(&data[entryOffset], bytes, dataSize) == 0)
                return;
        }

        auto result = data.size();
        data.insert(data.end(), bytes, bytes + dataSize);
        bucket.push_back({result, dataSize});
    }

    void internString8(const std::string &string)
    {
        if(string.empty())
            return;

        return pushBytes(reinterpret_cast<const uint8_t*> (string.data()), std::min(string.size(), size_t(0xFF)));
    }

    void internString16(const std::string &string)
    {
        if(string.empty())
            return;

        pushBytes(reinterpret_cast<const uint8_t*> (string.data()), std::min(string.size(), size_t(0xFFFF)));
    }

    void internString32(const std::string &string)
    {
        if(string.empty())
            return;

        pushBytes(reinterpret_cast<const uint8_t*> (string.data()), std::min(string.size(), size_t(0xFFFFFFFF)));
    }

private:
    static constexpr size_t HashTableCapacity = 4096;
    static uint32_t hashForBytes(const uint8_t *bytes, size_t dataSize)
    {
        // FIXME: Use a better hash function.
        uint32_t result = 0;
        for(size_t i = 0; i < dataSize; ++i)
        {
            result = result*33 + bytes[i];
        }

        return result;
    }

    std::vector<std::pair<size_t, size_t>> hashTable[HashTableCapacity];
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

    void writeUTF8_32_8(const std::string &string)
    {
        assert(blob);
        auto dataSize = uint8_t(std::min(string.size(), size_t(0xFF)));
        writeUInt32(blob->getOffsetForBytes(reinterpret_cast<const uint8_t*> (string.data()), dataSize));
        writeUInt8(dataSize);
    }

    void writeUTF8_32_16(const std::string &string)
    {
        assert(blob);
        auto dataSize = uint16_t(std::min(string.size(), size_t(0xFFFF)));
        writeUInt32(blob->getOffsetForBytes(reinterpret_cast<const uint8_t*> (string.data()), dataSize));
        writeUInt16(dataSize);
    }

    void writeUTF8_32_32(const std::string &string)
    {
        assert(blob);
        auto dataSize = uint32_t(std::min(string.size(), size_t(0xFFFFFFFF)));
        writeUInt32(blob->getOffsetForBytes(reinterpret_cast<const uint8_t*> (string.data()), dataSize));
        writeUInt32(dataSize);
    }

    void setTypeDescriptorContext(TypeDescriptorContext *context)
    {
        typeDescriptorContext = context;
    }

    void writeTypeDescriptorForTypeMapper(const TypeMapperPtr &typeMapper);

    void setObjectPointerToIndexMap(const std::unordered_map<const void*, uint32_t> *map)
    {
        objectPointerToIndexMap = map;
    }

    void writeObjectPointerAsReference(const void *pointer)
    {
        auto it = objectPointerToIndexMap->find(pointer);
        if(it != objectPointerToIndexMap->end())
            writeUInt32(it->second + 1);
        else
            writeUInt32(0);
    }

private:
    const BinaryBlobBuilder *blob = nullptr;
    TypeDescriptorContext *typeDescriptorContext = nullptr;
    const std::unordered_map<const void*, uint32_t> *objectPointerToIndexMap = nullptr;
};

/**
 * Interface for a read stream.
 */
struct ReadStream
{
    virtual ~ReadStream() {}

    virtual bool readBytes(uint8_t *buffer, size_t size) = 0;
    virtual bool skipBytes(size_t size) = 0;

    bool readUInt8(uint8_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 1);
    }

    bool readUInt16(uint16_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 2);
    }

    bool readUInt32(uint32_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 4);
    }

    bool readUInt64(uint64_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 8);
    }

    bool readInt8(int8_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 1);
    }

    bool readInt16(int16_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 2);
    }

    bool readInt32(int32_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 4);
    }

    bool readInt64(int64_t &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 8);
    }

    bool readFloat32(float &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 4);
    }

    bool readFloat64(double &destination)
    {
        return readBytes(reinterpret_cast<uint8_t*> (&destination), 8);
    }

    bool readUTF8_32_8(std::string &output)
    {
        uint32_t offset = 0;
        uint8_t size = 0;
        if(!readUInt32(offset) || !readUInt8(size) || offset + size > binaryBlobSize)
            return false;

        output.resize(size);
        memcpy(output.data(), binaryBlobData + offset, size);
        return true;
    }

    bool readUTF8_32_16(std::string &output)
    {
        uint32_t offset = 0;
        uint16_t size = 0;
        if(!readUInt32(offset) || !readUInt16(size) || offset + size > binaryBlobSize)
            return false;

        output.resize(size);
        memcpy(output.data(), binaryBlobData + offset, size);
        return true;
    }

    bool readUTF8_32_32(std::string &output)
    {
        uint32_t offset = 0;
        uint32_t size = 0;
        if(!readUInt32(offset) || !readUInt32(size) || offset + size > binaryBlobSize)
            return false;

        output.resize(size);
        memcpy(output.data(), binaryBlobData + offset, size);
        return true;
    }

    bool readTypeDescriptor(TypeDescriptorPtr &typeDescriptor);

    void setTypeDescriptorContext(TypeDescriptorContext *context)
    {
        typeDescriptorContext = context;
    }

    void setBinaryBlob(const uint8_t *data, size_t size)
    {
        binaryBlobData = data;
        binaryBlobSize = size;
    }

    void setInstances(const std::vector<ObjectMapperPtr> *theInstances)
    {
        instances = theInstances;
    }

    bool readInstanceReference(ObjectMapperPtr &destination)
    {
        uint32_t index = 0;
        if(!readUInt32(index) || index > instances->size())
            return false;

        if(index == 0)
            destination.reset();
        else
            destination = (*instances)[index - 1];
        return true;
    }

private:
    size_t binaryBlobSize;
    const uint8_t *binaryBlobData;
    TypeDescriptorContext *typeDescriptorContext = nullptr;
    const std::vector<ObjectMapperPtr> *instances = nullptr;
};

/**
 * Memory write stream
 */
class MemoryWriteStream : public WriteStream
{
public:
    MemoryWriteStream(std::vector<uint8_t> &initialOutput)
        : output(initialOutput) {}

    virtual void writeBytes(const uint8_t *data, size_t size) override
    {
        output.insert(output.end(), data, data + size);
    }
private:
    std::vector<uint8_t> &output;
};

/**
 * Memory read stream
 */
class MemoryReadStream : public ReadStream
{
public:
    MemoryReadStream(const uint8_t *initialData, size_t initialDataSize)
        : data(initialData), dataSize(initialDataSize) {}

    virtual bool readBytes(uint8_t *buffer, size_t size) override
    {
        if(position + size > dataSize)
            return false;

        memcpy(buffer, data + position, size);
        position += size;
        return true;
    }

    virtual bool skipBytes(size_t size) override
    {
        if(position + size > dataSize)
            return false;

        position += size;
        return true;
    }

private:
    size_t position = 0;
    const uint8_t *data = nullptr;
    size_t dataSize;
};

/**
 * Type descriptor
 */
class TypeDescriptor
{
public:
    virtual ~TypeDescriptor() {}

    virtual void writeDescriptionWith(WriteStream *output)
    {
        output->writeUInt8(uint8_t(kind));
    }

    virtual bool skipDataWith(ReadStream *input)
    {
        switch(kind)
        {
        case TypeDescriptorKind::Object: return input->skipBytes(4);
        case TypeDescriptorKind::Boolean8: return input->skipBytes(1);
        case TypeDescriptorKind::Boolean16: return input->skipBytes(2);
        case TypeDescriptorKind::Boolean32: return input->skipBytes(4);
        case TypeDescriptorKind::Boolean64: return input->skipBytes(8);
        case TypeDescriptorKind::UInt8: return input->skipBytes(1);
        case TypeDescriptorKind::UInt16: return input->skipBytes(2);
        case TypeDescriptorKind::UInt32: return input->skipBytes(4);
        case TypeDescriptorKind::UInt64: return input->skipBytes(8);
        case TypeDescriptorKind::UInt128: return input->skipBytes(16);
        case TypeDescriptorKind::Int8: return input->skipBytes(1);
        case TypeDescriptorKind::Int16: return input->skipBytes(2);
        case TypeDescriptorKind::Int32: return input->skipBytes(4);
        case TypeDescriptorKind::Int64: return input->skipBytes(8);
        case TypeDescriptorKind::Int128: return input->skipBytes(16);
        case TypeDescriptorKind::Float16: return input->skipBytes(2);
        case TypeDescriptorKind::Float32: return input->skipBytes(4);
        case TypeDescriptorKind::Float64: return input->skipBytes(8);
        case TypeDescriptorKind::Float128: return input->skipBytes(16);
        case TypeDescriptorKind::Float256: return input->skipBytes(32);
        case TypeDescriptorKind::Decimal32: return input->skipBytes(4);
        case TypeDescriptorKind::Decimal64: return input->skipBytes(8);
        case TypeDescriptorKind::Decimal128: return input->skipBytes(16);
        case TypeDescriptorKind::Binary_32_8: return input->skipBytes(5);
        case TypeDescriptorKind::Binary_32_16: return input->skipBytes(6);
        case TypeDescriptorKind::Binary_32_32: return input->skipBytes(8);
        case TypeDescriptorKind::UTF8_32_8: return input->skipBytes(5);
        case TypeDescriptorKind::UTF8_32_16: return input->skipBytes(6);
        case TypeDescriptorKind::UTF8_32_32: return input->skipBytes(8);
        case TypeDescriptorKind::UTF16_32_8: return input->skipBytes(5);
        case TypeDescriptorKind::UTF16_32_16: return input->skipBytes(6);
        case TypeDescriptorKind::UTF16_32_32: return input->skipBytes(8);
        case TypeDescriptorKind::UTF32_32_8: return input->skipBytes(5);
        case TypeDescriptorKind::UTF32_32_16: return input->skipBytes(6);
        case TypeDescriptorKind::UTF32_32_32: return input->skipBytes(8);
        case TypeDescriptorKind::BigInt_32_8: return input->skipBytes(5);
        case TypeDescriptorKind::BigInt_32_16: return input->skipBytes(6);
        case TypeDescriptorKind::BigInt_32_32: return input->skipBytes(8);
        case TypeDescriptorKind::Char8: return input->skipBytes(1);
        case TypeDescriptorKind::Char16: return input->skipBytes(2);
        case TypeDescriptorKind::Char32: return input->skipBytes(4);
        case TypeDescriptorKind::TypedObject: return input->skipBytes(4);
        default: return false;
        }
    }

    TypeDescriptorKind kind;
};

/**
 * Structure type descriptor
 */
class StructTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override
    {
        output->writeUInt8(uint8_t(kind));
        output->writeUInt32(index);
    }

    virtual bool skipDataWith(ReadStream *input);

    uint32_t index = 0;
    TypeMapperPtr typeMapper;
};

/**
 * Fixed array type descriptor
 */
class FixedArrayTypeDescriptor : public TypeDescriptor
{
public:

    virtual void writeDescriptionWith(WriteStream *output) override
    {
        output->writeUInt8(uint8_t(kind));
        output->writeUInt32(size);
        element->writeDescriptionWith(output);
    }

    virtual bool skipDataWith(ReadStream *input) override
    {
        for(uint32_t i = 0; i < size; ++i)
        {
            if(!element->skipDataWith(input))
                return false;
        }

        return true;
    }

    uint32_t size;
    TypeDescriptorPtr element;
};

/**
 * Array type descriptor
 */
class ArrayTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override
    {
        output->writeUInt8(uint8_t(kind));
        element->writeDescriptionWith(output);
    }

    virtual bool skipDataWith(ReadStream *input) override
    {
        size_t size = 0;
        switch(kind)
        {
        case TypeDescriptorKind::Array8:
            {
                uint8_t count = 0;
                if(!input->readUInt8(count))
                    return false;
                size = count;
            }
            break;
        case TypeDescriptorKind::Array16:
            {
                uint16_t count = 0;
                if(!input->readUInt16(count))
                    return false;
                size = count;
            }
            break;
        case TypeDescriptorKind::Array32:
            {
                uint32_t count = 0;
                if(!input->readUInt32(count))
                    return false;
                size = count;
            }
            break;
        default:
            return false;
        }

        for(size_t i = 0; i < size; ++i)
        {
            if(!element->skipDataWith(input))
                return false;
        }

        return true;
    }

    TypeDescriptorPtr element;
};

/**
 * Set type descriptor
 */
class SetTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override
    {
        output->writeUInt8(uint8_t(kind));
        element->writeDescriptionWith(output);
    }

    virtual bool skipDataWith(ReadStream *input) override
    {
        size_t size = 0;
        switch(kind)
        {
        case TypeDescriptorKind::Set8:
            {
                uint8_t count = 0;
                if(!input->readUInt8(count))
                    return false;
                size = count;
            }
            break;
        case TypeDescriptorKind::Set16:
            {
                uint16_t count = 0;
                if(!input->readUInt16(count))
                    return false;
                size = count;
            }
            break;
        case TypeDescriptorKind::Set32:
            {
                uint32_t count = 0;
                if(!input->readUInt32(count))
                    return false;
                size = count;
            }
            break;
        default:
            return false;
        }

        for(size_t i = 0; i < size; ++i)
        {
            if(!element->skipDataWith(input))
                return false;
        }

        return true;
    }

    TypeDescriptorPtr element;
};

/**
 * Map type descriptor
 */
class MapTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override
    {
        output->writeUInt8(uint8_t(kind));
        key->writeDescriptionWith(output);
        value->writeDescriptionWith(output);
    }

    virtual bool skipDataWith(ReadStream *input) override
    {
        size_t size = 0;
        switch(kind)
        {
        case TypeDescriptorKind::Map8:
            {
                uint8_t count = 0;
                if(!input->readUInt8(count))
                    return false;
                size = count;
            }
            break;
        case TypeDescriptorKind::Map16:
            {
                uint16_t count = 0;
                if(!input->readUInt16(count))
                    return false;
                size = count;
            }
            break;
        case TypeDescriptorKind::Map32:
            {
                uint32_t count = 0;
                if(!input->readUInt32(count))
                    return false;
                size = count;
            }
            break;
        default:
            return false;
        }

        for(size_t i = 0; i < size; ++i)
        {
            if(!key->skipDataWith(input) || !value->skipDataWith(input))
                return false;
        }

        return true;
    }

    TypeDescriptorPtr key;
    TypeDescriptorPtr value;
};

/**
 * Structure type descriptor
 */
class ObjectReferenceTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override
    {
        output->writeUInt8(uint8_t(kind));
        output->writeUInt32(index);
    }

    uint32_t index = 0;
    TypeMapperWeakPtr typeMapper;
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

    TypeDescriptorPtr getForTypeMapper(const TypeMapperPtr &mapper);

    uint32_t getValueTypeCount()
    {
        return uint32_t(valueTypes.size());
    }

    TypeDescriptorPtr addValueType(const TypeMapperPtr &mapper)
    {
        assert(mapperToDescriptorMap.find(mapper) == mapperToDescriptorMap.end());

        auto descriptor = std::make_shared<StructTypeDescriptor> ();
        descriptor->kind = TypeDescriptorKind::Struct;
        descriptor->index = valueTypes.size();
        descriptor->typeMapper = mapper;

        valueTypes.push_back(mapper);
        valueTypeDescriptors.push_back(descriptor);
        mapperToDescriptorMap.insert({mapper, descriptor});
        return descriptor;
    }

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder);

    void writeValueTypeLayoutsWith(WriteStream *output);

    bool readTypeDescriptorWith(TypeDescriptorPtr &descriptor, ReadStream *input)
    {
        uint8_t kindByte = 0;
        if(!input->readUInt8(kindByte))
            return false;

        // Is this a single byte primitive type descriptor?
        auto kind = TypeDescriptorKind(kindByte);
        if(kindByte < uint8_t(TypeDescriptorKind::PrimitiveTypeDescriptorCount))
        {
            descriptor = getOrCreatePrimitiveTypeDescriptor(kind);
            return true;
        }

        switch(kind)
        {
        case TypeDescriptorKind::Struct:
            {
                uint32_t structureIndex = 0;
                if(!input->readUInt32(structureIndex) || structureIndex >= valueTypes.size())
                    return false;

                descriptor = valueTypeDescriptors[structureIndex];
                return true;
            }
        
        case TypeDescriptorKind::TypedObject:
            {
                uint32_t clusterIndex = 0;
                if(!input->readUInt32(clusterIndex) || clusterIndex >= clusterTypes.size())
                    return false;

                descriptor = getOrCreateForTypedObjectReference(clusterTypes[clusterIndex]);
                return true;
            }

        case TypeDescriptorKind::Array8:
        case TypeDescriptorKind::Array16:
        case TypeDescriptorKind::Array32:
            {
                TypeDescriptorPtr elementTypeDescriptor;
                if(!readTypeDescriptorWith(elementTypeDescriptor, input))
                    return false;

                descriptor = getOrCreateArrayTypeDescriptor(kind, elementTypeDescriptor);
                return true;
            }

        case TypeDescriptorKind::Set8:
        case TypeDescriptorKind::Set16:
        case TypeDescriptorKind::Set32:
            {
                TypeDescriptorPtr elementTypeDescriptor;
                if(!readTypeDescriptorWith(elementTypeDescriptor, input))
                    return false;

                descriptor = getOrCreateSetTypeDescriptor(kind, elementTypeDescriptor);
                return true;
            }

        case TypeDescriptorKind::Map8:
        case TypeDescriptorKind::Map16:
        case TypeDescriptorKind::Map32:
            {
                TypeDescriptorPtr keyTypeDescriptor;
                TypeDescriptorPtr valueTypeDescriptor;
                if(!readTypeDescriptorWith(keyTypeDescriptor, input) || !readTypeDescriptorWith(valueTypeDescriptor, input))
                    return false;

                descriptor = getOrCreateMapTypeDescriptor(kind, keyTypeDescriptor, valueTypeDescriptor);
                return true;
            }
        default:
            // Unsupported type descriptor kind.
            return false;
        }
    }

    void addObjectTypeMapper(const TypeMapperPtr &typeMapper)
    {
        objectTypeToClusterIndexMap.insert({typeMapper, clusterTypes.size()});
        clusterTypes.push_back(typeMapper);
    }

    TypeDescriptorPtr getOrCreateForTypedObjectReference(const TypeMapperPtr &objectType)
    {
        auto it = typedObjectReferenceCache.find(objectType);
        if(it != typedObjectReferenceCache.end())
            return it->second;
        
        auto descriptor = std::make_shared<ObjectReferenceTypeDescriptor> ();
        descriptor->kind = TypeDescriptorKind::TypedObject;
        descriptor->index = objectTypeToClusterIndexMap.at(objectType);
        descriptor->typeMapper = objectType;
        return descriptor;
    }

    TypeDescriptorPtr getOrCreateArrayTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &elementType)
    {
        auto it = arrayTypeDescriptorCache.find({kind, elementType});
        if(it != arrayTypeDescriptorCache.end())
            return it->second;
        
        auto descriptor = std::make_shared<ArrayTypeDescriptor> ();
        descriptor->kind = kind;
        descriptor->element = elementType;
        arrayTypeDescriptorCache.insert({{kind, elementType}, descriptor});
        return descriptor;
    }

    TypeDescriptorPtr getOrCreateSetTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &elementType)
    {
        auto it = setTypeDescriptorCache.find({kind, elementType});
        if(it != setTypeDescriptorCache.end())
            return it->second;
        
        auto descriptor = std::make_shared<SetTypeDescriptor> ();
        descriptor->kind = kind;
        descriptor->element = elementType;
        setTypeDescriptorCache.insert({{kind, elementType}, descriptor});
        return descriptor;
    }

    TypeDescriptorPtr getOrCreateMapTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &keyType, const TypeDescriptorPtr &valueType)
    {
        auto it = mapTypeDescriptorCache.find({kind, {keyType, valueType}});
        if(it != mapTypeDescriptorCache.end())
            return it->second;
        
        auto descriptor = std::make_shared<MapTypeDescriptor> ();
        descriptor->kind = kind;
        descriptor->key = keyType;
        descriptor->value = valueType;
        mapTypeDescriptorCache.insert({{kind, {keyType, valueType}}, descriptor});
        return descriptor;
    }

private:
    std::array<TypeDescriptorPtr, uint8_t(TypeDescriptorKind::PrimitiveTypeDescriptorCount)> primitiveTypeDescriptors;

    std::vector<TypeMapperPtr> valueTypes;
    std::vector<TypeDescriptorPtr> valueTypeDescriptors;

    std::vector<TypeMapperPtr> clusterTypes;
    std::unordered_map<TypeMapperPtr, uint32_t> objectTypeToClusterIndexMap;
    std::unordered_map<TypeMapperPtr, TypeDescriptorPtr> mapperToDescriptorMap;
    std::unordered_map<TypeMapperPtr, TypeDescriptorPtr> typedObjectReferenceCache;
    std::map<std::pair<TypeDescriptorKind, TypeDescriptorPtr>, TypeDescriptorPtr> arrayTypeDescriptorCache;
    std::map<std::pair<TypeDescriptorKind, TypeDescriptorPtr>, TypeDescriptorPtr> setTypeDescriptorCache;
    std::map<std::pair<TypeDescriptorKind, std::pair<TypeDescriptorPtr, TypeDescriptorPtr>>, TypeDescriptorPtr> mapTypeDescriptorCache;
};

inline void WriteStream::writeTypeDescriptorForTypeMapper(const TypeMapperPtr &typeMapper)
{
    typeDescriptorContext->getForTypeMapper(typeMapper)->writeDescriptionWith(this);
}

inline bool ReadStream::readTypeDescriptor(TypeDescriptorPtr &descriptor)
{
    return typeDescriptorContext->readTypeDescriptorWith(descriptor, this);
}

/**
 * Aggregate field description.
 */
struct FieldDescription
{
    FieldDescription() = default;

    FieldDescription(const std::string &initialName, const TypeMapperPtr &initialTypeMapper, const FieldAccessorPtr &initialAccessor)
        : name(initialName), typeMapper(initialTypeMapper), accessor(initialAccessor) {}

    template<typename CT, typename MT>
    FieldDescription(const std::string &initialName, MT CT::*fieldPointer);

    std::string name;
    TypeMapperWeakPtr typeMapper;
    FieldAccessorPtr accessor;

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder) const
    {
        binaryBlobBuilder.internString16(name);
    }

    void writeDescriptionWith(WriteStream *output) const
    {
        output->writeUTF8_32_16(name);
        output->writeTypeDescriptorForTypeMapper(typeMapper.lock());
    }
};

typedef std::vector<FieldDescription> FieldDescriptions;

/**
 * Materialization field description
 */
struct MaterializationFieldDescription
{
    std::string name;
    TypeDescriptorPtr encoding;
    FieldDescription *targetField = nullptr;
    TypeMapperPtr targetTypeMapper;

    bool readDescriptionWith(ReadStream *input)
    {
        return input->readUTF8_32_16(name) && input->readTypeDescriptor(encoding);
    }
};

typedef std::function<void (const TypeMapperPtr &)> TypeMapperIterationBlock;
typedef std::function<void (const ObjectMapperPtr &)> ObjectReferenceIterationBlock;

/**
 * Type mapper interface.
 * I am an interface used for describing and serializing the contents of specific types.
 */
class TypeMapper : public std::enable_shared_from_this<TypeMapper>
{
public:
    virtual ~TypeMapper() {};

    virtual bool isMaterializationAdaptationType() const
    {
        return false;
    }

    virtual bool isSerializationDependencyType() const
    {
        return false;
    }

    virtual bool isAggregateType() const
    {
        return false;
    }

    virtual bool isObjectType() const
    {
        return false;
    }

    virtual bool isReferenceType() const
    {
        return false;
    }

    virtual const std::string &getName() const = 0;

    virtual void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
    {
        (void)binaryBlobBuilder;
    }

    virtual TypeMapperPtr getResolvedType() const
    {
        return nullptr;
    }

    virtual uint16_t getFieldCount() const
    {
        return 0;
    }

    virtual FieldDescription *getFieldNamed(const std::string &fieldName)
    {
        (void)fieldName;
        return nullptr;
    }

    virtual void writeFieldDescriptionsWith(WriteStream *output) const
    {
        (void)output;
        abort();
    }

    virtual void writeInstanceWith(void *basePointer, WriteStream *output)
    {
        (void)basePointer;
        (void)output;
        abort();
    }

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output)
    {
        (void)fieldPointer;
        (void)output;
        abort();
    }

    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder)
    {
        (void)fieldPointer;
        (void)binaryBlobBuilder;
    }

    virtual void pushInstanceDataIntoBinaryBlob(void *instancePointer, BinaryBlobBuilder &binaryBlobBuilder)
    {
        (void)instancePointer;
        (void)binaryBlobBuilder;
    }

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
    {
        (void)encoding;
        return false;
    }

    virtual bool readInstanceWith(void *basePointer, ReadStream *input)
    {
        (void)basePointer;
        (void)input;
        abort();
    }

    virtual bool skipInstanceWith(ReadStream *input)
    {
        (void)input;
        abort();
    }

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
    {
        (void)fieldPointer;
        (void)fieldEncoding;
        (void)input;
        abort();
    }
    
    virtual bool skipFieldWith(ReadStream *input)
    {
        (void)input;
        abort();
    }

    virtual ObjectMapperPtr makeInstance()
    {
        abort();
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) = 0;

    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock)
    {
        (void)aBlock;
    }

    virtual void withTypeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock)
    {
        if(isSerializationDependencyType())
            aBlock(shared_from_this());
        typeMapperDependenciesDo(aBlock);
    }

    virtual void objectReferencesInInstanceDo(void *instancePointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock)
    {
        (void)instancePointer;
        (void)cache;
        (void)aBlock;
    }

    virtual void objectReferencesInFieldDo(void *fieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock)
    {
        (void)fieldPointer;
        (void)cache;
        (void)aBlock;
    }
};

inline TypeDescriptorPtr TypeDescriptorContext::getForTypeMapper(const TypeMapperPtr &mapper)
{
    auto it = mapperToDescriptorMap.find(mapper);
    if(it != mapperToDescriptorMap.end())
        return it->second;

    auto descriptor = mapper->getOrCreateTypeDescriptor(this);
    mapperToDescriptorMap.insert({mapper, descriptor});
    return descriptor;
}

inline void TypeDescriptorContext::pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
{
    for(auto &typeMapper : valueTypes)
    {
        binaryBlobBuilder.internString16(typeMapper->getName());
        typeMapper->pushDataIntoBinaryBlob(binaryBlobBuilder);
    }
}

inline void TypeDescriptorContext::writeValueTypeLayoutsWith(WriteStream *output)
{
    for(auto &typeMapper : valueTypes)
    {
        output->writeUTF8_32_16(typeMapper->getName());
        output->writeUInt16(typeMapper->getFieldCount());
        typeMapper->writeFieldDescriptionsWith(output);
    }
}

inline bool StructTypeDescriptor::skipDataWith(ReadStream *input)
{
    return typeMapper && typeMapper->skipFieldWith(input);
}

/**
 * Object mapper interface.
 * I am an interface used for gluing different kinds of objects with serializer and deserializer.
 */
class ObjectMapper
{
public:
    virtual ~ObjectMapper() {};

    virtual TypeMapperPtr getTypeMapper() const = 0;
    virtual void *getObjectBasePointer() = 0;
    
    virtual std::shared_ptr<void> asObjectSharedPointer()
    {
        return nullptr;
    }
};

/**
 * Type mapper registry interface.
 * I am an interface used for looking up type mappers by name. I am typically used for customizing the materialization process
 */
class TypeMapperRegistry
{
public:
    virtual ~TypeMapperRegistry() {}

    virtual TypeMapperPtr getTypeMapperWithName(const std::string &name)
    {
        (void)name;
        return nullptr;
    }

    static TypeMapperRegistryPtr getOrCreateForTransitiveClosureOf(const TypeMapperPtr &rootTypeMapper);
};

/**
 * Transitive closure type mapper registry
 * I am a type mapper registry that is built by using the transitive closure that starts with some specified types.
 */
class TransitiveClosureTypeMapperRegistry : public TypeMapperRegistry
{
public:
   
    virtual TypeMapperPtr getTypeMapperWithName(const std::string &name)
    {
        auto it = nameMap.find(name);
        return it != nameMap.end() ? it->second : nullptr;
    }

    void addWithDependencies(const TypeMapperPtr &typeMapper)
    {
        if(!typeMapper)
            return;

        if(addedTypes.find(typeMapper) != addedTypes.end())
            return;

        addedTypes.insert(typeMapper);
        nameMap.insert({typeMapper->getName(), typeMapper});
        typeMapper->typeMapperDependenciesDo([&](const TypeMapperPtr &dependency) {
            addWithDependencies(dependency);
        });
    }

private:
    std::unordered_set<TypeMapperPtr> addedTypes;
    std::unordered_map<std::string, TypeMapperPtr> nameMap;
};

inline TypeMapperRegistryPtr TypeMapperRegistry::getOrCreateForTransitiveClosureOf(const TypeMapperPtr &rootTypeMapper)
{
    static std::unordered_map<TypeMapperPtr, TypeMapperRegistryPtr> cachedRegistries;
    static std::mutex cachedRegistriesMutex;

    std::unique_lock<std::mutex> l(cachedRegistriesMutex);

    auto it = cachedRegistries.find(rootTypeMapper);
    if(it != cachedRegistries.end())
        return it->second;

    auto transitiveClosureRegistry = std::make_shared<TransitiveClosureTypeMapperRegistry> ();
    transitiveClosureRegistry->addWithDependencies(rootTypeMapper);
    cachedRegistries.insert({rootTypeMapper, transitiveClosureRegistry});
    return transitiveClosureRegistry;
}

/**
 * I am an accessor for a field.
 */
class FieldAccessor
{
public:
    virtual ~FieldAccessor() {}

    virtual void *getPointerForBasePointer(void *basePointer) = 0;
};

/**
 * Aggregate type mapper
 * I am object type mapper
 */
class PrimitiveTypeMapper : public TypeMapper
{
public:

    virtual const std::string &getName() const override
    {
        return name;
    }

    std::string name;
};

/**
 * Aggregate type mapper
 * I am object type mapper
 */
class AggregateTypeMapper : public TypeMapper
{
public:
    virtual bool isAggregateType() const override
    {
        return true;
    }

    virtual bool isSerializationDependencyType() const override
    {
        return true;
    }

    virtual const std::string &getName() const override
    {
        return name;
    }

    virtual void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder) override
    {
        binaryBlobBuilder.internString16(name);
        for(auto &field : fields)
            binaryBlobBuilder.internString16(field.name);
    }

    virtual uint16_t getFieldCount() const override
    {
        return uint16_t(fields.size());
    }

    virtual FieldDescription *getFieldNamed(const std::string &name) override
    {
        auto it = fieldNameMap.find(name);
        return it != fieldNameMap.end() ? &fields[it->second] : nullptr;
    }

    virtual void writeFieldDescriptionsWith(WriteStream *output) const override
    {
        for(auto &field : fields)
            field.writeDescriptionWith(output);
    }

    virtual void writeInstanceWith(void *basePointer, WriteStream *output) override
    {
        for(auto &field : fields)
        {
            auto fieldPointer = field.accessor->getPointerForBasePointer(basePointer);
            field.typeMapper.lock()->writeFieldWith(fieldPointer, output);
        }
    }

    virtual void pushInstanceDataIntoBinaryBlob(void *instancePointer, BinaryBlobBuilder &binaryBlobBuilder)
    {
        for(auto &field : fields)
        {
            auto fieldPointer = field.accessor->getPointerForBasePointer(instancePointer);
            field.typeMapper.lock()->pushFieldDataIntoBinaryBlob(fieldPointer, binaryBlobBuilder);
        }
    }

    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock) override
    {
        for(auto &field : fields)
        {
            auto fieldTypeMapper = field.typeMapper.lock();
            if(fieldTypeMapper)
                fieldTypeMapper->withTypeMapperDependenciesDo(aBlock);
        }
    }

protected:

    void addFields(const std::vector<FieldDescription> &newFields)
    {
        fields.reserve(newFields.size());
        for(auto &field : newFields)
        {
            fieldNameMap.insert({field.name, fields.size()});
            fields.push_back(field);
        }
    }

    std::string name;
    std::vector<FieldDescription> fields;
    std::unordered_map<std::string, size_t> fieldNameMap;

};

typedef std::function<ObjectMapperPtr ()> ObjectMapperFactory;

/**
 * Structure type mapper
 * I am object type mapper
 */
class ObjectTypeMapper : public AggregateTypeMapper
{
public:

    virtual bool isObjectType() const
    {
        return true;
    }

    virtual ObjectMapperPtr makeInstance() override
    {
        return factory();
    }

    static TypeMapperPtr makeWithFields(const std::string &name, const TypeMapperPtr &superType, const ObjectMapperFactory &factory, const std::vector<FieldDescription> &fields)
    {
        auto result = std::make_shared<ObjectTypeMapper> ();
        result->name = name;
        result->superType = superType;
        result->addFields(fields);
        result->factory = factory;
        return result;
    }

    virtual void writeFieldWith(void *, WriteStream *)
    {
        // This is implemented in a separate location.
        abort();
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *)
    {
        abort();
    }

    void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock)
    {
        auto st = superType.lock();
        if(st)
            st->withTypeMapperDependenciesDo(aBlock);
        AggregateTypeMapper::typeMapperDependenciesDo(aBlock);
    }

    virtual void objectReferencesInInstanceDo(void *instancePointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override
    {
        auto st = superType.lock();
        if(st)
            st->objectReferencesInInstanceDo(instancePointer, cache, aBlock);

        for(auto &field : fields)
        {
            auto fieldType = field.typeMapper.lock();
            if(fieldType)
            {
                auto fieldPointer = field.accessor->getPointerForBasePointer(instancePointer);
                fieldType->objectReferencesInFieldDo(fieldPointer, cache, aBlock);
            }
        }
    }

    TypeMapperWeakPtr superType;
    ObjectMapperFactory factory;
};

/**
 * Structure type mapper
 * I am object type mapper
 */
class StructureTypeMapper : public AggregateTypeMapper
{
public:
    static TypeMapperPtr makeWithFields(const std::string &name, const std::vector<FieldDescription> &fields)
    {
        auto result = std::make_shared<StructureTypeMapper> ();
        result->name = name;
        result->addFields(fields);
        return result;
    }

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output)
    {
        writeInstanceWith(fieldPointer, output);
    }

    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder)
    {
        pushInstanceDataIntoBinaryBlob(fieldPointer, binaryBlobBuilder);
    }

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
    {
        if(encoding->kind != TypeDescriptorKind::Struct)
            return false;
        
        auto materializationTypeMapper = std::static_pointer_cast<StructTypeDescriptor> (encoding)->typeMapper;
        return materializationTypeMapper
            && materializationTypeMapper->getResolvedType() == shared_from_this()
            && materializationTypeMapper->isMaterializationAdaptationType()
            && materializationTypeMapper->isAggregateType()
            && !materializationTypeMapper->isObjectType();
    }

    virtual bool readFieldWith(void *basePointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
    {
        assert(canReadFieldWithTypeDescriptor(fieldEncoding));
        return std::static_pointer_cast<StructTypeDescriptor> (fieldEncoding)->typeMapper->readFieldWith(basePointer, fieldEncoding, input);
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *)
    {
        // This should not be reached.
        abort();
    }

    virtual void objectReferencesInFieldDo(void *baseFieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override
    {
        for(auto &field : fields)
        {
            auto fieldType = field.typeMapper.lock();
            if(fieldType)
            {
                auto fieldPointer = field.accessor->getPointerForBasePointer(baseFieldPointer);
                fieldType->objectReferencesInFieldDo(fieldPointer, cache, aBlock);
            }
        }
    }
};

/**
 * Type mapper used to resolve differences between in memory and disk types.
 */
class MaterializationTypeMapper : public TypeMapper
{
public:

    std::string name;
    std::vector<MaterializationFieldDescription> fields;
    TypeMapperPtr resolvedType;

    virtual bool isAggregateType() const
    {
        return true;
    }

    virtual bool isMaterializationAdaptationType() const
    {
        return true;
    }

    virtual const std::string &getName() const
    {
        return name;
    }

    virtual TypeMapperPtr getResolvedType() const
    {
        return resolvedType;
    }

    virtual void pushDataIntoBinaryBlob(BinaryBlobBuilder &)
    {
        abort();
    }

    virtual uint16_t getFieldCount() const
    {
        return uint16_t(fields.size());
    }

    virtual void writeFieldDescriptionsWith(WriteStream *) const
    {
        abort();
    }

    virtual void writeFieldWith(void *, WriteStream *)
    {
        abort();
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *)
    {
        abort();
    }

    void resolveTypeUsing(const TypeMapperPtr &newResolveType)
    {
        if(!newResolveType)
            return;

        // Match the type kind.
        if(newResolveType->isObjectType() != isObjectType())
            return;

        // Set the resolved type. 
        resolvedType = newResolveType;

    }

    void resolveTypeFields()
    {
        if(!resolvedType)
            return;

        // Attempt to match the different fields.
        for(auto &serializedField : fields)
        {
            // Fetch the target field.
            auto targetField = resolvedType->getFieldNamed(serializedField.name);
            if(!targetField)
                continue;

            // Fetch the target type mapper.
            auto targetFieldTypeMapper = targetField->typeMapper.lock();
            if(!targetFieldTypeMapper)
                continue;

            // Check that the target type mapper can read the target field.
            if(!targetFieldTypeMapper->canReadFieldWithTypeDescriptor(serializedField.encoding))
                continue;

            serializedField.targetField = targetField;
            serializedField.targetTypeMapper = targetFieldTypeMapper;
        }
    }

};

/**
 * Type mapper used to resolve differences in structure types.
 */
class StructureMaterializationTypeMapper : public MaterializationTypeMapper
{
public:

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
    {
        return encoding->kind == TypeDescriptorKind::Struct && std::static_pointer_cast<StructTypeDescriptor> (encoding)->typeMapper == shared_from_this();
    }

    virtual bool readFieldWith(void *basePointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
    {
        assert(canReadFieldWithTypeDescriptor(fieldEncoding));

        for(auto &field : fields)
        {
            if(field.targetField && field.targetTypeMapper)
            {
                auto targetFieldPointer = field.targetField->accessor->getPointerForBasePointer(basePointer);
                if(!field.targetTypeMapper->readFieldWith(targetFieldPointer, field.encoding, input))
                    return false;
            }
            else
            {
                if(!field.encoding->skipDataWith(input))
                    return false;
            }
        }

        return true;
    }

};

/**
 * Type mapper used to resolve differences in structure types.
 */
class ObjectMaterializationTypeMapper : public MaterializationTypeMapper
{
public:

    virtual bool isObjectType() const
    {
        return true;
    }

    virtual ObjectMapperPtr makeInstance() override
    {
        return resolvedType ? resolvedType->makeInstance() : nullptr;
    }

    virtual bool readInstanceWith(void *basePointer, ReadStream *input) override
    {
        auto s = supertype.lock();
        if(s)
        {
            if(!s->readInstanceWith(basePointer, input))
                return false;
        }

        for(auto &field : fields)
        {
            if(field.targetField && field.targetTypeMapper)
            {
                auto targetFieldPointer = field.targetField->accessor->getPointerForBasePointer(basePointer);
                if(!field.targetTypeMapper->readFieldWith(targetFieldPointer, field.encoding, input))
                    return false;
            }
            else
            {
                if(!field.encoding->skipDataWith(input))
                    return false;
            }
        }

        return true;
    }

    virtual bool skipInstanceWith(ReadStream *input) override
    {
        auto s = supertype.lock();
        if(s)
        {
            if(!s->skipInstanceWith(input))
                return false;
        }

        for(auto &field : fields)
        {
            if(!field.encoding->skipDataWith(input))
                return false;
        }

        return true;
    }

    ObjectMaterializationTypeMapperWeakPtr supertype;
};

/**
 * Makes a type mapper for the specified type.
 */
template<typename T, typename C=void>
struct TypeMapperFor;

template<typename T>
inline TypeMapperPtr typeMapperForType()
{
    return TypeMapperFor<T>::apply();
}

template<typename T>
struct SingletonTypeMapperFor
{
    static constexpr bool IsObjectType = T::IsObjectType;
    static constexpr bool IsReferenceType = T::IsReferenceType;
    static constexpr bool IsValueType = !IsObjectType && !IsReferenceType;

    static TypeMapperPtr apply()
    {
        return T::uniqueInstance();
    }
};

/**
 * Numeric primitive type mapper.
 */
template<typename FT, TypeDescriptorKind TDK>
class NumericPrimitiveTypeMapper : public PrimitiveTypeMapper
{
public:
    typedef FT FieldType;
    typedef NumericPrimitiveTypeMapper<FT, TDK> ThisType;

    static constexpr TypeDescriptorKind EncodingDescriptorKind = TDK;
    static constexpr bool IsObjectType = false;
    static constexpr bool IsReferenceType = false;

    static TypeMapperPtr uniqueInstance()
    {
        static auto singleton = std::make_shared<ThisType> ();
        return singleton;
    }

    NumericPrimitiveTypeMapper()
    {
        name = typeDescriptorKindToString(EncodingDescriptorKind);
    }

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output) override
    {
        output->writeBytes(reinterpret_cast<const uint8_t*> (fieldPointer), sizeof(FieldType));
    }

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
    {
        switch(encoding->kind)
        {
        case TypeDescriptorKind::Boolean8:
        case TypeDescriptorKind::Boolean16:
        case TypeDescriptorKind::Boolean32:
        case TypeDescriptorKind::Boolean64:
        case TypeDescriptorKind::UInt8:
        case TypeDescriptorKind::UInt16:
        case TypeDescriptorKind::UInt32:
        case TypeDescriptorKind::UInt64:
        case TypeDescriptorKind::UInt128:
        case TypeDescriptorKind::Int8:
        case TypeDescriptorKind::Int16:
        case TypeDescriptorKind::Int32:
        case TypeDescriptorKind::Int64:
        case TypeDescriptorKind::Int128:
        case TypeDescriptorKind::Float32:
        case TypeDescriptorKind::Float64:
        case TypeDescriptorKind::Char8:
        case TypeDescriptorKind::Char16:
        case TypeDescriptorKind::Char32:
                return true;

        default:
            return false;
        }
    }

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
    {
        auto destination = reinterpret_cast<FieldType*> (fieldPointer);

        switch(fieldEncoding->kind)
        {
        case TypeDescriptorKind::Boolean8:
        case TypeDescriptorKind::UInt8:
        case TypeDescriptorKind::Char8:
            {
                uint8_t readedValue = 0;
                if(!input->readUInt8(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Boolean16:
        case TypeDescriptorKind::UInt16:
        case TypeDescriptorKind::Char16:
            {
                uint16_t readedValue = 0;
                if(!input->readUInt16(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Boolean32:
        case TypeDescriptorKind::UInt32:
        case TypeDescriptorKind::Char32:
            {
                uint32_t readedValue = 0;
                if(!input->readUInt32(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Boolean64:
        case TypeDescriptorKind::UInt64:
            {
                uint64_t readedValue = 0;
                if(!input->readUInt64(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::UInt128:
            {
                uint64_t readedLow = 0;
                uint64_t readedHigh = 0;
                if(!input->readUInt64(readedLow) || !input->readUInt64(readedHigh))
                    return false;

                *destination = FieldType(readedLow);
                return true;
            }

        case TypeDescriptorKind::Int8:
            {
                int8_t readedValue = 0;
                if(!input->readInt8(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Int16:
            {
                int16_t readedValue = 0;
                if(!input->readInt16(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Int32:
            {
                int32_t readedValue = 0;
                if(!input->readInt32(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Int64:
            {
                int64_t readedValue = 0;
                if(!input->readInt64(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Int128:
            {
                int64_t readedLow = 0;
                int64_t readedHigh = 0;
                if(!input->readInt64(readedLow) || !input->readInt64(readedHigh))
                    return false;

                *destination = FieldType(readedLow);
                return true;
            }

        case TypeDescriptorKind::Float32:
            {
                float readedValue = 0;
                if(!input->readFloat32(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        case TypeDescriptorKind::Float64:
            {
                double readedValue = 0;
                if(!input->readFloat64(readedValue))
                    return false;
                *destination = FieldType(readedValue);
                return true;
            }

        default:
            return false;
        }
    }


    TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context)
    {
        return context->getOrCreatePrimitiveTypeDescriptor(EncodingDescriptorKind);
    }
};

template<>
struct TypeMapperFor<bool> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<bool, TypeDescriptorKind::Boolean8>> {};

template<>
struct TypeMapperFor<uint8_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<uint8_t, TypeDescriptorKind::UInt8>> {};

template<>
struct TypeMapperFor<uint16_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<uint16_t, TypeDescriptorKind::UInt16>> {};

template<>
struct TypeMapperFor<uint32_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<uint32_t, TypeDescriptorKind::UInt32>> {};

template<>
struct TypeMapperFor<uint64_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<uint64_t, TypeDescriptorKind::UInt64>> {};

template<>
struct TypeMapperFor<int8_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<int8_t, TypeDescriptorKind::Int8>> {};

template<>
struct TypeMapperFor<int16_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<int16_t, TypeDescriptorKind::Int16>> {};

template<>
struct TypeMapperFor<int32_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<int32_t, TypeDescriptorKind::Int32>> {};

template<>
struct TypeMapperFor<int64_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<int64_t, TypeDescriptorKind::Int64>> {};

template<>
struct TypeMapperFor<char> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<char, TypeDescriptorKind::Char8>> {};

template<>
struct TypeMapperFor<char16_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<char16_t, TypeDescriptorKind::Char16>> {};

template<>
struct TypeMapperFor<char32_t> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<char32_t, TypeDescriptorKind::Char32>> {};

template<>
struct TypeMapperFor<float> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<float, TypeDescriptorKind::Float32>> {};

template<>
struct TypeMapperFor<double> : SingletonTypeMapperFor<NumericPrimitiveTypeMapper<double, TypeDescriptorKind::Float64>> {};

/**
 * Tag for marking a serializable structure.
 */
struct SerializableStructureTag {};

/**
 * Utility for extracting the metadata for a structure type.
 */
template<typename T, typename C=void>
struct StructureTypeMetadataFor;

template<typename T>
struct StructureTypeMetadataFor<T, typename std::enable_if< std::is_base_of<SerializableStructureTag, T>::value >::type>
{
    typedef void type;

    static FieldDescriptions getFields()
    {
        return T::__coal_fields__();
    }

    static std::string getTypeName()
    {
        return T::__coal_typename__;
    }
};

template<typename T>
struct ReflectedStructureTypeMapperFor
{
    typedef StructureTypeMetadataFor<T> Metadata;

    static constexpr bool IsObjectType = false;
    static constexpr bool IsReferenceType = false;
    static constexpr bool IsValueType = true;

    static TypeMapperPtr apply()
    {
        static TypeMapperPtr singleton;
        static std::once_flag singletonCreation;
        std::call_once(singletonCreation, [&](){
            singleton = StructureTypeMapper::makeWithFields(Metadata::getTypeName(), Metadata::getFields());
        });

        return singleton;
    }
};

template<typename T>
struct TypeMapperFor<T, typename StructureTypeMetadataFor<T>::type> : ReflectedStructureTypeMapperFor<T> {};

/**
 * Utility for extracting the metadata for a class type.
 */
template<typename T, typename C=void>
struct ClassTypeMetadataFor;

template<typename T>
struct ReflectedClassTypeMapperFor
{
    typedef ClassTypeMetadataFor<T> Metadata;

    static constexpr bool IsObjectType = true;
    static constexpr bool IsReferenceType = false;
    static constexpr bool IsValueType = false;

    static TypeMapperPtr apply()
    {
        static TypeMapperPtr singleton;
        static std::once_flag singletonCreation;
        std::call_once(singletonCreation, [&](){
            singleton = ObjectTypeMapper::makeWithFields(Metadata::getTypeName(), nullptr, Metadata::newInstance, Metadata::getFields());
        });

        return singleton;
    }
};

template<typename T>
struct TypeMapperFor<T, typename ClassTypeMetadataFor<T>::type> : ReflectedClassTypeMapperFor<T> {};


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

    void *getPointerForBasePointer(void *basePointer) override
    {
        auto self = reinterpret_cast<ClassType*> (basePointer);
        return reinterpret_cast<void*> (&((*self).*memberPointer));
    }

    MemberPointerType memberPointer;
};

template<typename CT, typename MT>
FieldAccessorPtr memberFieldAccessorFor(MT CT::*fieldPointer)
{
    return std::make_shared<MemberFieldAccessor<CT, MT>> (fieldPointer);
};

template<typename CT, typename MT>
inline FieldDescription::FieldDescription(const std::string &initialName, MT CT::*fieldPointer)
    : name(initialName), typeMapper(typeMapperForType<MT> ()), accessor(memberFieldAccessorFor(fieldPointer))
{
}


/**
 * I am a default value type box object.
 */
template<typename VT>
class RootValueBox : public ObjectMapper
{
public:
    typedef VT ValueType;
    typedef RootValueBox<VT> ThisType;

    RootValueBox(const VT &initialValue = VT())
        : value(initialValue) {}

    static TypeMapperPtr typeMapperSingleton()
    {
        static auto singleton = ObjectTypeMapper::makeWithFields("RootValueBox", nullptr, 
            []() {
                return std::make_shared<ThisType> ();
            },
            {
                {"value", &ThisType::value}
            });
        return singleton;
    }

    static ObjectMapperPtr makeFor(std::unordered_map<void *, ObjectMapperPtr> *cache, const ValueType &value)
    {
        (void)cache;
        return std::make_shared<ThisType> (value);
    }

    static std::optional<ValueType> unwrapDeserializedRootObjectOrValue(const ObjectMapperPtr &deserializedRootObject)
    {
        if(!deserializedRootObject)
            return std::nullopt;

        return std::static_pointer_cast<ThisType> (deserializedRootObject)->value;
    }

    virtual TypeMapperPtr getTypeMapper() const override
    {
        return typeMapperSingleton();
    }

    virtual void *getObjectBasePointer() override
    {
        return reinterpret_cast<void*> (this);
    }

    VT value;
};

/**
 * Makes an object mapper interface for the specified object.
 */
template<typename T, typename C=void>
struct ObjectMapperClassFor;

template<typename T>
struct ObjectMapperClassFor<T, typename std::enable_if<TypeMapperFor<T>::IsValueType>::type>
{
    typedef RootValueBox<T> type;
};

template<typename T>
struct ObjectMapperClassFor<const T &> : ObjectMapperClassFor<T> {};

template<typename T>
struct ObjectMapperClassFor<T &> : ObjectMapperClassFor<T> {};

/**
 * Serialization cluster
 */
class SerializationCluster
{
public:
    size_t index = 0;
    std::string name;
    SerializationClusterWeakPtr supertype;
    TypeMapperPtr typeMapper;
    std::vector<ObjectMapperPtr> instances;
    std::vector<size_t> objectFieldDescriptions;

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
    {
        binaryBlobBuilder.internString16(name);
        typeMapper->pushDataIntoBinaryBlob(binaryBlobBuilder);
        for(auto &instance: instances)
        {
            auto instancePointer = instance->getObjectBasePointer();
            typeMapper->pushInstanceDataIntoBinaryBlob(instancePointer, binaryBlobBuilder);
        }
    }

    void addObject(const ObjectMapperPtr &object)
    {
        instances.push_back(object);
    }

    void writeDescriptionWith(WriteStream *output)
    {
        auto super = supertype.lock();
        output->writeUTF8_32_16(name);
        output->writeUInt32(super ? super->index + 1 : 0);
        output->writeUInt16(typeMapper->getFieldCount());
        output->writeUInt32(instances.size());
        typeMapper->writeFieldDescriptionsWith(output);
    }

    void writeInstancesWith(WriteStream *output)
    {
        for(const auto &instance : instances)
        {
            auto basePointer = instance->getObjectBasePointer();
            typeMapper->writeInstanceWith(basePointer, output);
        }
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
        serializeRootObject(ObjectMapperClassFor<ROT>::type::makeFor(&objectPointerToMapperMap, root));
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
        writeTrailerForObject(object);
    }

private:
    enum class ValueTypeScanColor: uint8_t
    {
        White = 0,
        Gray,
        Black
    };

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
        auto typeMapper = object->getTypeMapper();
        auto cluster = getOrCreateClusterFor(typeMapper);
        cluster->addObject(object);

        typeMapper->objectReferencesInInstanceDo(object->getObjectBasePointer(), &objectPointerToMapperMap, [&](const ObjectMapperPtr &reference) {
            addPendingObject(reference);
        });
    }

    TypeDescriptorPtr getOrCreateAggregateTypeDescriptorFor(const TypeMapperPtr &typeMapper)
    {
        auto it = valueTypeScanColorMap.find(typeMapper);
        if(it != valueTypeScanColorMap.end())
        {
            auto currentColor = it->second;
            assert(currentColor == ValueTypeScanColor::Black && "Recursive value types are not allowed.");
            if(currentColor == ValueTypeScanColor::Gray)
                abort();
            
            return typeDescriptorContext.getForTypeMapper(typeMapper);
        }

        // Scan the dependencies recursively.
        valueTypeScanColorMap[typeMapper] = ValueTypeScanColor::Gray;
        typeMapper->typeMapperDependenciesDo([&](const TypeMapperPtr &dependency) {
            scanTypeMapperDependency(dependency);
        });
        
        valueTypeScanColorMap[typeMapper] = ValueTypeScanColor::Black;

        // Add the value type.
        return typeDescriptorContext.addValueType(typeMapper);
    }

    void scanReferenceTypeDependencies(const TypeMapperPtr &typeMapper)
    {
        if(scannedReferenceType.find(typeMapper) == scannedReferenceType.end())
            return;

        scannedReferenceType.insert(typeMapper);
        typeMapper->typeMapperDependenciesDo([&](const TypeMapperPtr &dependency) {
            scanTypeMapperDependency(dependency);
        });
    }

    void scanTypeMapperDependency(const TypeMapperPtr &typeMapper)
    {
        if(typeMapper->isObjectType())
            getOrCreateClusterFor(typeMapper);
        else if(typeMapper->isAggregateType())
            getOrCreateAggregateTypeDescriptorFor(typeMapper);
        else if(typeMapper->isReferenceType())
            scanReferenceTypeDependencies(typeMapper);
    }

    SerializationClusterPtr getOrCreateClusterFor(const TypeMapperPtr &typeMapper)
    {
        assert(typeMapper->isObjectType());
        auto it = typeMapperToClustersMap.find(typeMapper);
        if(it != typeMapperToClustersMap.end())
            return it->second;

        auto newCluster = std::make_shared<SerializationCluster> ();
        newCluster->name = typeMapper->getName();
        newCluster->typeMapper = typeMapper;
        newCluster->index = clusters.size();
        clusters.push_back(newCluster);
        typeMapperToClustersMap.insert(std::make_pair(typeMapper, newCluster));

        typeMapper->typeMapperDependenciesDo([&](const TypeMapperPtr &dependency) {
            scanTypeMapperDependency(dependency);
        });

        return newCluster;
    }

    void writeHeader()
    {
        output->writeUInt32(CoalMagicNumber);
        output->writeUInt8(CoalVersionMajor);
        output->writeUInt8(CoalVersionMinor);
        output->writeUInt16(0); // Reserved

        output->writeUInt32(binaryBlobBuilder.data.size()); // Blob size
        output->writeUInt32(typeDescriptorContext.getValueTypeCount()); // Value type layouts size
        output->writeUInt32(clusters.size()); // Cluster Count
        output->writeUInt32(objectCount); // Cluster Count
    }

    void writeBlob()
    {
        output->writeBlob(&binaryBlobBuilder);
    }

    void writeValueTypeLayouts()
    {
        output->setTypeDescriptorContext(&typeDescriptorContext);
        typeDescriptorContext.writeValueTypeLayoutsWith(output);
    }

    void writeClusterDescriptions()
    {
        for(auto &cluster : clusters)
            cluster->writeDescriptionWith(output);
    }

    void writeClusterInstances()
    {
        for(auto &cluster : clusters)
            cluster->writeInstancesWith(output);
    }

    void writeTrailerForObject(const ObjectMapperPtr &rootObject)
    {
        output->writeUInt32(objectPointerToInstanceIndexTable.at(rootObject->getObjectBasePointer()) + 1);
    }

    void prepareForWriting()
    {
        objectCount = 0;
        typeDescriptorContext.pushDataIntoBinaryBlob(binaryBlobBuilder);
        for(auto & cluster : clusters)
        {
            cluster->pushDataIntoBinaryBlob(binaryBlobBuilder);
            typeDescriptorContext.addObjectTypeMapper(cluster->typeMapper);
            for(auto instance : cluster->instances)
                objectPointerToInstanceIndexTable.insert({instance->getObjectBasePointer(), objectCount++});
        }

        output->setObjectPointerToIndexMap(&objectPointerToInstanceIndexTable);
    }

    WriteStream *output;

    TypeDescriptorContext typeDescriptorContext;
    BinaryBlobBuilder binaryBlobBuilder;
    size_t objectCount;
    std::vector<SerializationClusterPtr> clusters;
    std::unordered_map<TypeMapperPtr, ValueTypeScanColor> valueTypeScanColorMap;
    std::unordered_map<TypeMapperPtr, SerializationClusterPtr> typeMapperToClustersMap;
    std::unordered_set<TypeMapperPtr> scannedReferenceType;

    std::vector<ObjectMapperPtr> tracingStack;
    std::unordered_set<ObjectMapperPtr> seenSet;
    std::unordered_map<void *, ObjectMapperPtr> objectPointerToMapperMap;
    std::unordered_map<const void *, uint32_t> objectPointerToInstanceIndexTable;
};

/**
 * Coal deserializer.
 */
class Deserializer
{
public:

    Deserializer(ReadStream *initialInput)
        : input(initialInput) {}

    template<typename T>
    std::optional<T> deserializeRootObjectOrValueOfType()
    {
        typedef typename ObjectMapperClassFor<T>::type RootObjectMapperClass;

        auto result = deserializeRootObject(RootObjectMapperClass::typeMapperSingleton());
        return RootObjectMapperClass::unwrapDeserializedRootObjectOrValue(result);
    }

    ObjectMapperPtr deserializeRootObject(const TypeMapperPtr &rootTypeMapper)
    {
        if(!typeMapperRegistry)
            typeMapperRegistry = TypeMapperRegistry::getOrCreateForTransitiveClosureOf(rootTypeMapper);

        if(!parseContent())
            return nullptr;
        return rootObject;
    }

private:
    bool parseHeaderAndReadBlob()
    {
        uint32_t magicNumber;
        uint8_t versionMajor, versionMinor;
        uint16_t reserved;
        uint32_t blobSize;

        if(!input->readUInt32(magicNumber) || magicNumber != CoalMagicNumber)
            return false;

        if(!input->readUInt8(versionMajor) || versionMajor != CoalVersionMajor)
            return false;

        if(!input->readUInt8(versionMinor) || versionMinor != CoalVersionMinor)
            return false;

        if (!input->readUInt16(reserved) ||
            !input->readUInt32(blobSize) ||
            !input->readUInt32(valueTypeCount) ||
            !input->readUInt32(clusterCount) ||
            !input->readUInt32(objectCount))
            return false;

        blobData.resize(blobSize);
        if(!input->readBytes(blobData.data(), blobSize))
            return false;
        input->setBinaryBlob(blobData.data(), blobSize);
        input->setTypeDescriptorContext(&typeDescriptorContext);

        return true;
    }

    bool parseContent()
    {
        return parseHeaderAndReadBlob() &&
            parseValueTypeDescriptors() &&
            parseClusterDescriptors() &&
            validateAndResolveTypes() &&
            parseClusterInstances() &&
            parseTrailer();
    }

    bool parseValueTypeDescriptors()
    {
        for(uint32_t i = 0; i < valueTypeCount; ++i)
        {
            auto structureType = std::make_shared<StructureMaterializationTypeMapper> ();
            uint16_t fieldCount;
            if(!input->readUTF8_32_16(structureType->name) ||
                !input->readUInt16(fieldCount))
                return false;

            structureType->fields.resize(fieldCount);
            for(uint16_t fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex)
            {
                auto &field = structureType->fields[fieldIndex];
                if(!field.readDescriptionWith(input))
                    return false;
            }

            structureType->resolveTypeUsing(typeMapperRegistry->getTypeMapperWithName(structureType->getName()));
            structureType->resolveTypeFields();
            typeDescriptorContext.addValueType(structureType);
        }
        return true;
    }

    bool parseClusterDescriptors()
    {
        // Pre-allocate the cluster types.
        clusterTypes.reserve(clusterCount);
        for(uint32_t i = 0; i < clusterCount; ++i)
        {
            clusterTypes.push_back(std::make_shared<ObjectMaterializationTypeMapper> ());
            typeDescriptorContext.addObjectTypeMapper(clusterTypes.back());
        }

        // Parse the clusters.
        clusterInstanceCount.reserve(clusterCount);
        uint32_t totalInstanceCount = 0;
        for(uint32_t clusterIndex = 0; clusterIndex < clusterCount; ++clusterIndex)
        {
            auto &clusterType = clusterTypes[clusterIndex];
            uint32_t superTypeIndex;
            uint16_t fieldCount;
            uint32_t instanceCount;
            if(!input->readUTF8_32_16(clusterType->name) ||
                !input->readUInt32(superTypeIndex) || superTypeIndex > clusterIndex ||
                !input->readUInt16(fieldCount) ||
                !input->readUInt32(instanceCount))
                return false;
            clusterInstanceCount.push_back(instanceCount);
            if(superTypeIndex > 0)
                clusterType->supertype = clusterTypes[superTypeIndex - 1];

            clusterType->fields.resize(fieldCount);
            for(uint16_t fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex)
            {
                auto &field = clusterType->fields[fieldIndex];
                if(!field.readDescriptionWith(input))
                    return false;
            }

            totalInstanceCount += instanceCount;
        }

        // Validate the total instance count.
        if(totalInstanceCount != objectCount)
            return false;

        return true;
    }

    bool validateAndResolveTypes()
    {
        for(auto &type : clusterTypes)
            type->resolveTypeUsing(typeMapperRegistry->getTypeMapperWithName(type->getName()));

        for(auto &type : clusterTypes)
            type->resolveTypeFields();

        return true;
    }

    bool parseClusterInstances()
    {
        // Make the instances.
        instances.reserve(objectCount);
        for(size_t i = 0; i < clusterTypes.size(); ++i)
        {
            auto &clusterType = clusterTypes[i];
            auto instanceCount = clusterInstanceCount[i];
            for(uint32_t j = 0; j < instanceCount; ++j)
                instances.push_back(clusterType->makeInstance());
        }
        input->setInstances(&instances);

        // Parse the instance data.
        uint32_t nextInstanceIndex = 0;
        for(size_t i = 0; i < clusterTypes.size(); ++i)
        {
            auto &clusterType = clusterTypes[i];
            auto instanceCount = clusterInstanceCount[i];
            for(uint32_t j = 0; j < instanceCount; ++j)
            {
                auto &instance = instances[nextInstanceIndex++];
                if(instance)
                {
                    auto basePointer = instance->getObjectBasePointer();
                    if(!clusterType->readInstanceWith(basePointer, input))
                        return false;
                }
                else
                {
                    if(!clusterType->skipInstanceWith(input))
                        return false;
                }
            }
        }

        return true;
    }

    bool parseTrailer()
    {
        uint32_t rootObjectIndex = 0;
        if(!input->readUInt32(rootObjectIndex) || rootObjectIndex > objectCount)
            return false;

        rootObject = rootObjectIndex > 0 ? instances[rootObjectIndex - 1] : 0;
        return true;
    }

    ReadStream *input;
    ObjectMapperPtr rootObject;
    TypeMapperRegistryPtr typeMapperRegistry;
    std::vector<uint8_t> blobData;
    TypeDescriptorContext typeDescriptorContext;

    uint32_t valueTypeCount = 0;
    uint32_t clusterCount = 0;
    uint32_t objectCount = 0;

    std::vector<ObjectMaterializationTypeMapperPtr> clusterTypes;
    std::vector<uint32_t> clusterInstanceCount;
    std::vector<ObjectMapperPtr> instances;
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
std::optional<RT> deserialize(const std::vector<uint8_t> &data)
{
    MemoryReadStream input(data.data(), data.size());
    Deserializer deserializer(&input);
    return deserializer.deserializeRootObjectOrValueOfType<RT> ();
}

} // End of namespace coal

#endif //COAL_SERIALIZATION_COAL_HPP
