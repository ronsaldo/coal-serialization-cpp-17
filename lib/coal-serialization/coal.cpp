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

#include "coal-serialization/coal.hpp"

namespace coal
{

#pragma region TypeDescriptorKind
const char *typeDescriptorKindToString(TypeDescriptorKind kind)
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

#pragma endregion TypeDescriptorKind

#pragma region BinaryBlobBuilder
uint32_t BinaryBlobBuilder::getOffsetForBytes(const uint8_t *bytes, size_t dataSize) const
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

void BinaryBlobBuilder::pushBytes(const uint8_t *bytes, size_t dataSize)
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

void BinaryBlobBuilder::internString8(const std::string &string)
{
    if(string.empty())
        return;

    return pushBytes(reinterpret_cast<const uint8_t*> (string.data()), std::min(string.size(), size_t(0xFF)));
}

void BinaryBlobBuilder::internString16(const std::string &string)
{
    if(string.empty())
        return;

    pushBytes(reinterpret_cast<const uint8_t*> (string.data()), std::min(string.size(), size_t(0xFFFF)));
}

void BinaryBlobBuilder::internString32(const std::string &string)
{
    if(string.empty())
        return;

    pushBytes(reinterpret_cast<const uint8_t*> (string.data()), std::min(string.size(), size_t(0xFFFFFFFF)));
}

uint32_t BinaryBlobBuilder::hashForBytes(const uint8_t *bytes, size_t dataSize)
{
    // FIXME: Use a better hash function.
    uint32_t result = 0;
    for(size_t i = 0; i < dataSize; ++i)
    {
        result = result*33 + bytes[i];
    }

    return result;
}

const uint8_t *BinaryBlobBuilder::getData() const
{
    return data.data();
}

size_t BinaryBlobBuilder::getDataSize() const
{
    return data.size();
}

#pragma endregion BinaryBlobBuilder

#pragma region WriteStream

WriteStream::~WriteStream() {}

void WriteStream::writeUInt8(uint8_t value)
{
    writeBytes(&value, 1);
}

void WriteStream::writeUInt16(uint16_t value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 2);
}

void WriteStream::writeUInt32(uint32_t value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 4);
}

void WriteStream::writeUInt64(uint64_t value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 8);
}

void WriteStream::writeInt8(int8_t value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 1);
}

void WriteStream::writeInt16(int16_t value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 2);
}

void WriteStream::writeInt32(int32_t value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 4);
}

void WriteStream::writeInt64(int64_t value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 8);
}

void WriteStream::writeFloat32(float value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 4);
}

void WriteStream::writeFloat64(float value)
{
    writeBytes(reinterpret_cast<uint8_t*> (&value), 8);
}

void WriteStream::writeBlob(const BinaryBlobBuilder *theBlob)
{
    blob = theBlob;
    writeBytes(blob->getData(), blob->getDataSize());
}

void WriteStream::writeUTF8_32_8(const std::string &string)
{
    assert(blob);
    auto dataSize = uint8_t(std::min(string.size(), size_t(0xFF)));
    writeUInt32(blob->getOffsetForBytes(reinterpret_cast<const uint8_t*> (string.data()), dataSize));
    writeUInt8(dataSize);
}

void WriteStream::writeUTF8_32_16(const std::string &string)
{
    assert(blob);
    auto dataSize = uint16_t(std::min(string.size(), size_t(0xFFFF)));
    writeUInt32(blob->getOffsetForBytes(reinterpret_cast<const uint8_t*> (string.data()), dataSize));
    writeUInt16(dataSize);
}

void WriteStream::writeUTF8_32_32(const std::string &string)
{
    assert(blob);
    auto dataSize = uint32_t(std::min(string.size(), size_t(0xFFFFFFFF)));
    writeUInt32(blob->getOffsetForBytes(reinterpret_cast<const uint8_t*> (string.data()), dataSize));
    writeUInt32(dataSize);
}

void WriteStream::setTypeDescriptorContext(TypeDescriptorContext *context)
{
    typeDescriptorContext = context;
}

void WriteStream::writeTypeDescriptorForTypeMapper(const TypeMapperPtr &typeMapper)
{
    typeDescriptorContext->getForTypeMapper(typeMapper)->writeDescriptionWith(this);
}

void WriteStream::setObjectPointerToIndexMap(const std::unordered_map<const void*, uint32_t> *map)
{
    objectPointerToIndexMap = map;
}

void WriteStream::writeObjectPointerAsReference(const void *pointer)
{
    auto it = objectPointerToIndexMap->find(pointer);
    if(it != objectPointerToIndexMap->end())
        writeUInt32(it->second + 1);
    else
        writeUInt32(0);
}

#pragma endregion WriteStream

#pragma region ReadStream

ReadStream::~ReadStream()
{
}

bool ReadStream::readUInt8(uint8_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 1);
}

bool ReadStream::readUInt16(uint16_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 2);
}

bool ReadStream::readUInt32(uint32_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 4);
}

bool ReadStream::readUInt64(uint64_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 8);
}

bool ReadStream::readInt8(int8_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 1);
}

bool ReadStream::readInt16(int16_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 2);
}

bool ReadStream::readInt32(int32_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 4);
}

bool ReadStream::readInt64(int64_t &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 8);
}

bool ReadStream::readFloat32(float &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 4);
}

bool ReadStream::readFloat64(double &destination)
{
    return readBytes(reinterpret_cast<uint8_t*> (&destination), 8);
}

bool ReadStream::readUTF8_32_8(std::string &output)
{
    uint32_t offset = 0;
    uint8_t size = 0;
    if(!readUInt32(offset) || !readUInt8(size) || offset + size > binaryBlobSize)
        return false;

    output.resize(size);
    memcpy(output.data(), binaryBlobData + offset, size);
    return true;
}

bool ReadStream::readUTF8_32_16(std::string &output)
{
    uint32_t offset = 0;
    uint16_t size = 0;
    if(!readUInt32(offset) || !readUInt16(size) || offset + size > binaryBlobSize)
        return false;

    output.resize(size);
    memcpy(output.data(), binaryBlobData + offset, size);
    return true;
}

bool ReadStream::readUTF8_32_32(std::string &output)
{
    uint32_t offset = 0;
    uint32_t size = 0;
    if(!readUInt32(offset) || !readUInt32(size) || offset + size > binaryBlobSize)
        return false;

    output.resize(size);
    memcpy(output.data(), binaryBlobData + offset, size);
    return true;
}

bool ReadStream::readTypeDescriptor(TypeDescriptorPtr &typeDescriptor)
{
    return typeDescriptorContext->readTypeDescriptorWith(typeDescriptor, this);
}

void ReadStream::setTypeDescriptorContext(TypeDescriptorContext *context)
{
    typeDescriptorContext = context;
}

void ReadStream::setBinaryBlob(const uint8_t *data, size_t size)
{
    binaryBlobData = data;
    binaryBlobSize = size;
}

void ReadStream::setInstances(const std::vector<ObjectMapperPtr> *theInstances)
{
    instances = theInstances;
}

bool ReadStream::readInstanceReference(ObjectMapperPtr &destination)
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

#pragma endregion ReadStream

#pragma region MemoryWriteStream

MemoryWriteStream::MemoryWriteStream(std::vector<uint8_t> &initialOutput)
    : output(initialOutput) {}

void MemoryWriteStream::writeBytes(const uint8_t *data, size_t size)
{
    output.insert(output.end(), data, data + size);
}
#pragma endregion MemoryWriteStream

#pragma region MemoryReadStream
MemoryReadStream::MemoryReadStream(const uint8_t *initialData, size_t initialDataSize)
    : data(initialData), dataSize(initialDataSize)
{
}

bool MemoryReadStream::readBytes(uint8_t *buffer, size_t size)
{
    if(position + size > dataSize)
        return false;

    memcpy(buffer, data + position, size);
    position += size;
    return true;
}

bool MemoryReadStream::skipBytes(size_t size)
{
    if(position + size > dataSize)
        return false;

    position += size;
    return true;
}
#pragma endregion MemoryReadStream

#pragma region TypeDescriptor
TypeDescriptor::~TypeDescriptor()
{
}

void TypeDescriptor::writeDescriptionWith(WriteStream *output)
{
    output->writeUInt8(uint8_t(kind));
}

bool TypeDescriptor::skipDataWith(ReadStream *input)
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
#pragma endregion TypeDescriptor

#pragma region StructTypeDescriptor

void StructTypeDescriptor::writeDescriptionWith(WriteStream *output)
{
    output->writeUInt8(uint8_t(kind));
    output->writeUInt32(index);
}

bool StructTypeDescriptor::skipDataWith(ReadStream *input)
{
    return typeMapper && typeMapper->skipFieldWith(input);
}

#pragma endregion StructTypeDescriptor

#pragma region FixedArrayTypeDescriptor

void FixedArrayTypeDescriptor::writeDescriptionWith(WriteStream *output)
{
    output->writeUInt8(uint8_t(kind));
    output->writeUInt32(size);
    element->writeDescriptionWith(output);
}

bool FixedArrayTypeDescriptor::skipDataWith(ReadStream *input)
{
    for(uint32_t i = 0; i < size; ++i)
    {
        if(!element->skipDataWith(input))
            return false;
    }

    return true;
}

#pragma endregion FixedArrayTypeDescriptor

#pragma region ArrayTypeDescriptor

void ArrayTypeDescriptor::writeDescriptionWith(WriteStream *output)
{
    output->writeUInt8(uint8_t(kind));
    element->writeDescriptionWith(output);
}

bool ArrayTypeDescriptor::skipDataWith(ReadStream *input)
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

#pragma endregion ArrayTypeDescriptor

#pragma region SetTypeDescriptor

void SetTypeDescriptor::writeDescriptionWith(WriteStream *output)
{
    output->writeUInt8(uint8_t(kind));
    element->writeDescriptionWith(output);
}

bool SetTypeDescriptor::skipDataWith(ReadStream *input)
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
#pragma endregion SetTypeDescriptor

#pragma region MapTypeDescriptor

void MapTypeDescriptor::writeDescriptionWith(WriteStream *output)
{
    output->writeUInt8(uint8_t(kind));
    key->writeDescriptionWith(output);
    value->writeDescriptionWith(output);
}

bool MapTypeDescriptor::skipDataWith(ReadStream *input)
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
#pragma endregion MapTypeDescriptor

#pragma region ObjectReferenceTypeDescriptor

void ObjectReferenceTypeDescriptor::writeDescriptionWith(WriteStream *output)
{
    output->writeUInt8(uint8_t(kind));
    output->writeUInt32(index);
}

#pragma endregion ObjectReferenceTypeDescriptor

#pragma region TypeDescriptorContext

TypeDescriptorPtr TypeDescriptorContext::getOrCreatePrimitiveTypeDescriptor(TypeDescriptorKind kind)
{
    auto &descriptor = primitiveTypeDescriptors[uint8_t(kind)];
    if(!descriptor)
    {
        descriptor = std::make_shared<TypeDescriptor> ();
        descriptor->kind = kind;
    }

    return descriptor;
}

TypeDescriptorPtr TypeDescriptorContext::getForTypeMapper(const TypeMapperPtr &mapper)
{
    auto it = mapperToDescriptorMap.find(mapper);
    if(it != mapperToDescriptorMap.end())
        return it->second;

    auto descriptor = mapper->getOrCreateTypeDescriptor(this);
    mapperToDescriptorMap.insert({mapper, descriptor});
    return descriptor;
}

uint32_t TypeDescriptorContext::getValueTypeCount()
{
    return uint32_t(valueTypes.size());
}

TypeDescriptorPtr TypeDescriptorContext::addValueType(const TypeMapperPtr &mapper)
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

void TypeDescriptorContext::pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
{
    for(auto &typeMapper : valueTypes)
    {
        binaryBlobBuilder.internString16(typeMapper->getName());
        typeMapper->pushDataIntoBinaryBlob(binaryBlobBuilder);
    }
}

void TypeDescriptorContext::writeValueTypeLayoutsWith(WriteStream *output)
{
    for(auto &typeMapper : valueTypes)
    {
        output->writeUTF8_32_16(typeMapper->getName());
        output->writeUInt16(typeMapper->getFieldCount());
        typeMapper->writeFieldDescriptionsWith(output);
    }
}

bool TypeDescriptorContext::readTypeDescriptorWith(TypeDescriptorPtr &descriptor, ReadStream *input)
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

void TypeDescriptorContext::addObjectTypeMapper(const TypeMapperPtr &typeMapper)
{
    objectTypeToClusterIndexMap.insert({typeMapper, clusterTypes.size()});
    clusterTypes.push_back(typeMapper);
}

TypeDescriptorPtr TypeDescriptorContext::getOrCreateForTypedObjectReference(const TypeMapperPtr &objectType)
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

TypeDescriptorPtr TypeDescriptorContext::getOrCreateArrayTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &elementType)
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

TypeDescriptorPtr TypeDescriptorContext::getOrCreateSetTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &elementType)
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

TypeDescriptorPtr TypeDescriptorContext::getOrCreateMapTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &keyType, const TypeDescriptorPtr &valueType)
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

#pragma endregion TypeDescriptorContext

#pragma region FieldDescription

FieldDescription::FieldDescription(const std::string &initialName, const TypeMapperPtr &initialTypeMapper, const FieldAccessorPtr &initialAccessor)
    : name(initialName), typeMapper(initialTypeMapper), accessor(initialAccessor)
{
}

void FieldDescription::pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder) const
{
    binaryBlobBuilder.internString16(name);
}

void FieldDescription::writeDescriptionWith(WriteStream *output) const
{
    output->writeUTF8_32_16(name);
    output->writeTypeDescriptorForTypeMapper(typeMapper.lock());
}

#pragma endregion FieldDescription

#pragma region MaterializationFieldDescription

bool MaterializationFieldDescription::readDescriptionWith(ReadStream *input)
{
    return input->readUTF8_32_16(name) && input->readTypeDescriptor(encoding);
}

#pragma endregion MaterializationFieldDescription

#pragma region TypeMapper

TypeMapper::~TypeMapper()
{
}

bool TypeMapper::isMaterializationAdaptationType() const
{
    return false;
}

bool TypeMapper::isSerializationDependencyType() const
{
    return false;
}

bool TypeMapper::isAggregateType() const
{
    return false;
}

bool TypeMapper::isObjectType() const
{
    return false;
}

bool TypeMapper::isReferenceType() const
{
    return false;
}

void TypeMapper::pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
{
    (void)binaryBlobBuilder;
}

TypeMapperPtr TypeMapper::getResolvedType() const
{
    return nullptr;
}

uint16_t TypeMapper::getFieldCount() const
{
    return 0;
}

FieldDescription *TypeMapper::getFieldNamed(const std::string &fieldName)
{
    (void)fieldName;
    return nullptr;
}

void TypeMapper::writeFieldDescriptionsWith(WriteStream *output) const
{
    (void)output;
    abort();
}

void TypeMapper::writeInstanceWith(void *basePointer, WriteStream *output)
{
    (void)basePointer;
    (void)output;
    abort();
}

void TypeMapper::writeFieldWith(void *fieldPointer, WriteStream *output)
{
    (void)fieldPointer;
    (void)output;
    abort();
}

void TypeMapper::pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder)
{
    (void)fieldPointer;
    (void)binaryBlobBuilder;
}

void TypeMapper::pushInstanceDataIntoBinaryBlob(void *instancePointer, BinaryBlobBuilder &binaryBlobBuilder)
{
    (void)instancePointer;
    (void)binaryBlobBuilder;
}

bool TypeMapper::canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
{
    (void)encoding;
    return false;
}

bool TypeMapper::readInstanceWith(void *basePointer, ReadStream *input)
{
    (void)basePointer;
    (void)input;
    abort();
}

bool TypeMapper::skipInstanceWith(ReadStream *input)
{
    (void)input;
    abort();
}

bool TypeMapper::readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
{
    (void)fieldPointer;
    (void)fieldEncoding;
    (void)input;
    abort();
}

bool TypeMapper::skipFieldWith(ReadStream *input)
{
    (void)input;
    abort();
}

ObjectMapperPtr TypeMapper::makeInstance()
{
    abort();
}

void TypeMapper::typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock)
{
    (void)aBlock;
}

void TypeMapper::withTypeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock)
{
    if(isSerializationDependencyType())
        aBlock(shared_from_this());
    typeMapperDependenciesDo(aBlock);
}

void TypeMapper::objectReferencesInInstanceDo(void *instancePointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock)
{
    (void)instancePointer;
    (void)cache;
    (void)aBlock;
}

void TypeMapper::objectReferencesInFieldDo(void *fieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock)
{
    (void)fieldPointer;
    (void)cache;
    (void)aBlock;
}

#pragma endregion TypeMapper

#pragma region ObjectMapper

ObjectMapper::~ObjectMapper()
{
}

std::shared_ptr<void> ObjectMapper::asObjectSharedPointer()
{
    return nullptr;
}

#pragma endregion ObjectMapper

#pragma region TypeMapperRegistry

TypeMapperRegistry::~TypeMapperRegistry()
{
}

TypeMapperPtr TypeMapperRegistry::getTypeMapperWithName(const std::string &name)
{
    (void)name;
    return nullptr;
}

TypeMapperRegistryPtr TypeMapperRegistry::getOrCreateForTransitiveClosureOf(const TypeMapperPtr &rootTypeMapper)
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

#pragma endregion TypeMapperRegistry

#pragma region TransitiveClosureTypeMapperRegistry

TypeMapperPtr TransitiveClosureTypeMapperRegistry::getTypeMapperWithName(const std::string &name)
{
    auto it = nameMap.find(name);
    return it != nameMap.end() ? it->second : nullptr;
}

void TransitiveClosureTypeMapperRegistry::addWithDependencies(const TypeMapperPtr &typeMapper)
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

#pragma endregion TransitiveClosureTypeMapperRegistry

#pragma region FieldAccessor

FieldAccessor::~FieldAccessor()
{
}

#pragma endregion FieldAccessor

#pragma region PrimitiveTypeMapper

const std::string &PrimitiveTypeMapper::getName() const
{
    return name;
}

#pragma endregion PrimitiveTypeMapper

#pragma region AggregateTypeMapper

bool AggregateTypeMapper::isAggregateType() const
{
    return true;
}

bool AggregateTypeMapper::isSerializationDependencyType() const
{
    return true;
}

const std::string &AggregateTypeMapper::getName() const
{
    return name;
}

void AggregateTypeMapper::pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
{
    binaryBlobBuilder.internString16(name);
    for(auto &field : fields)
        binaryBlobBuilder.internString16(field.name);
}

uint16_t AggregateTypeMapper::getFieldCount() const
{
    return uint16_t(fields.size());
}

FieldDescription *AggregateTypeMapper::getFieldNamed(const std::string &fieldName)
{
    auto it = fieldNameMap.find(fieldName);
    return it != fieldNameMap.end() ? &fields[it->second] : nullptr;
}

void AggregateTypeMapper::writeFieldDescriptionsWith(WriteStream *output) const
{
    for(auto &field : fields)
        field.writeDescriptionWith(output);
}

void AggregateTypeMapper::writeInstanceWith(void *basePointer, WriteStream *output)
{
    for(auto &field : fields)
    {
        auto fieldPointer = field.accessor->getPointerForBasePointer(basePointer);
        field.typeMapper.lock()->writeFieldWith(fieldPointer, output);
    }
}

void AggregateTypeMapper::pushInstanceDataIntoBinaryBlob(void *instancePointer, BinaryBlobBuilder &binaryBlobBuilder)
{
    for(auto &field : fields)
    {
        auto fieldPointer = field.accessor->getPointerForBasePointer(instancePointer);
        field.typeMapper.lock()->pushFieldDataIntoBinaryBlob(fieldPointer, binaryBlobBuilder);
    }
}

void AggregateTypeMapper::typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock)
{
    for(auto &field : fields)
    {
        auto fieldTypeMapper = field.typeMapper.lock();
        if(fieldTypeMapper)
            fieldTypeMapper->withTypeMapperDependenciesDo(aBlock);
    }
}

void AggregateTypeMapper::addFields(const std::vector<FieldDescription> &newFields)
{
    fields.reserve(newFields.size());
    for(auto &field : newFields)
    {
        fieldNameMap.insert({field.name, fields.size()});
        fields.push_back(field);
    }
}

#pragma endregion AggregateTypeMapper

#pragma region ObjectTypeMapper

bool ObjectTypeMapper::isObjectType() const
{
    return true;
}

ObjectMapperPtr ObjectTypeMapper::makeInstance()
{
    return factory();
}

TypeMapperPtr ObjectTypeMapper::makeWithFields(const std::string &name, const TypeMapperPtr &superType, const ObjectMapperFactory &factory, const std::vector<FieldDescription> &fields)
{
    auto result = std::make_shared<ObjectTypeMapper> ();
    result->name = name;
    result->superType = superType;
    result->addFields(fields);
    result->factory = factory;
    return result;
}

void ObjectTypeMapper::writeFieldWith(void *, WriteStream *)
{
    // This is implemented in a separate location.
    abort();
}

TypeDescriptorPtr ObjectTypeMapper::getOrCreateTypeDescriptor(TypeDescriptorContext *)
{
    abort();
}

void ObjectTypeMapper::typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock)
{
    auto st = superType.lock();
    if(st)
        st->withTypeMapperDependenciesDo(aBlock);
    AggregateTypeMapper::typeMapperDependenciesDo(aBlock);
}

void ObjectTypeMapper::objectReferencesInInstanceDo(void *instancePointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock)
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

#pragma endregion ObjectTypeMapper

#pragma region StructureTypeMapper

TypeMapperPtr StructureTypeMapper::makeWithFields(const std::string &name, const std::vector<FieldDescription> &fields)
{
    auto result = std::make_shared<StructureTypeMapper> ();
    result->name = name;
    result->addFields(fields);
    return result;
}

void StructureTypeMapper::writeFieldWith(void *fieldPointer, WriteStream *output)
{
    writeInstanceWith(fieldPointer, output);
}

void StructureTypeMapper::pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder)
{
    pushInstanceDataIntoBinaryBlob(fieldPointer, binaryBlobBuilder);
}

bool StructureTypeMapper::canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
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

bool StructureTypeMapper::readFieldWith(void *basePointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
{
    assert(canReadFieldWithTypeDescriptor(fieldEncoding));
    return std::static_pointer_cast<StructTypeDescriptor> (fieldEncoding)->typeMapper->readFieldWith(basePointer, fieldEncoding, input);
}

TypeDescriptorPtr StructureTypeMapper::getOrCreateTypeDescriptor(TypeDescriptorContext *)
{
    // This should not be reached.
    abort();
}

void StructureTypeMapper::objectReferencesInFieldDo(void *baseFieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock)
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

#pragma endregion StructureTypeMapper

#pragma region MaterializationTypeMapper

bool MaterializationTypeMapper::isAggregateType() const
{
    return true;
}

bool MaterializationTypeMapper::isMaterializationAdaptationType() const
{
    return true;
}

const std::string &MaterializationTypeMapper::getName() const
{
    return name;
}

TypeMapperPtr MaterializationTypeMapper::getResolvedType() const
{
    return resolvedType;
}

void MaterializationTypeMapper::pushDataIntoBinaryBlob(BinaryBlobBuilder &)
{
    abort();
}

uint16_t MaterializationTypeMapper::getFieldCount() const
{
    return uint16_t(fields.size());
}

void MaterializationTypeMapper::writeFieldDescriptionsWith(WriteStream *) const
{
    abort();
}

void MaterializationTypeMapper::writeFieldWith(void *, WriteStream *)
{
    abort();
}

TypeDescriptorPtr MaterializationTypeMapper::getOrCreateTypeDescriptor(TypeDescriptorContext *)
{
    abort();
}

void MaterializationTypeMapper::resolveTypeUsing(const TypeMapperPtr &newResolveType)
{
    if(!newResolveType)
        return;

    // Match the type kind.
    if(newResolveType->isObjectType() != isObjectType())
        return;

    // Set the resolved type. 
    resolvedType = newResolveType;
}

void MaterializationTypeMapper::resolveTypeFields()
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

#pragma endregion MaterializationTypeMapper

#pragma region StructureMaterializationTypeMapper

bool StructureMaterializationTypeMapper::canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
{
    return encoding->kind == TypeDescriptorKind::Struct && std::static_pointer_cast<StructTypeDescriptor> (encoding)->typeMapper == shared_from_this();
}

bool StructureMaterializationTypeMapper::readFieldWith(void *basePointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
{
    (void)fieldEncoding;
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

#pragma endregion StructureMaterializationTypeMapper

#pragma region ObjectMaterializationTypeMapper

bool ObjectMaterializationTypeMapper::isObjectType() const
{
    return true;
}

ObjectMapperPtr ObjectMaterializationTypeMapper::makeInstance()
{
    return resolvedType ? resolvedType->makeInstance() : nullptr;
}

bool ObjectMaterializationTypeMapper::readInstanceWith(void *basePointer, ReadStream *input)
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

bool ObjectMaterializationTypeMapper::skipInstanceWith(ReadStream *input)
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

#pragma endregion ObjectMaterializationTypeMapper

#pragma region SerializationCluster

void SerializationCluster::pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder)
{
    binaryBlobBuilder.internString16(name);
    typeMapper->pushDataIntoBinaryBlob(binaryBlobBuilder);
    for(auto &instance: instances)
    {
        auto instancePointer = instance->getObjectBasePointer();
        typeMapper->pushInstanceDataIntoBinaryBlob(instancePointer, binaryBlobBuilder);
    }
}

void SerializationCluster::addObject(const ObjectMapperPtr &object)
{
    instances.push_back(object);
}

void SerializationCluster::writeDescriptionWith(WriteStream *output)
{
    auto super = supertype.lock();
    output->writeUTF8_32_16(name);
    output->writeUInt32(super ? super->index + 1 : 0);
    output->writeUInt16(typeMapper->getFieldCount());
    output->writeUInt32(instances.size());
    typeMapper->writeFieldDescriptionsWith(output);
}

void SerializationCluster::writeInstancesWith(WriteStream *output)
{
    for(const auto &instance : instances)
    {
        auto basePointer = instance->getObjectBasePointer();
        typeMapper->writeInstanceWith(basePointer, output);
    }
}

#pragma endregion SerializationCluster

#pragma region Serializer

Serializer::Serializer(WriteStream *initialOutput)
    : output(initialOutput)
{
}

void Serializer::serializeRootObject(const ObjectMapperPtr &object)
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

void Serializer::addPendingObject(const ObjectMapperPtr &object)
{
    if(seenSet.find(object) != seenSet.end())
        return;
    
    tracingStack.push_back(object);
    seenSet.insert(object);
}

void Serializer::tracePendingObjects()
{
    while(!tracingStack.empty())
    {
        auto pendingObject = tracingStack.back();
        tracingStack.pop_back();
        tracePendingObject(pendingObject);
    }
}

void Serializer::tracePendingObject(const ObjectMapperPtr &object)
{
    auto typeMapper = object->getTypeMapper();
    auto cluster = getOrCreateClusterFor(typeMapper);
    cluster->addObject(object);

    typeMapper->objectReferencesInInstanceDo(object->getObjectBasePointer(), &objectPointerToMapperMap, [&](const ObjectMapperPtr &reference) {
        addPendingObject(reference);
    });
}

TypeDescriptorPtr Serializer::getOrCreateAggregateTypeDescriptorFor(const TypeMapperPtr &typeMapper)
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

void Serializer::scanReferenceTypeDependencies(const TypeMapperPtr &typeMapper)
{
    if(scannedReferenceType.find(typeMapper) == scannedReferenceType.end())
        return;

    scannedReferenceType.insert(typeMapper);
    typeMapper->typeMapperDependenciesDo([&](const TypeMapperPtr &dependency) {
        scanTypeMapperDependency(dependency);
    });
}

void Serializer::scanTypeMapperDependency(const TypeMapperPtr &typeMapper)
{
    if(typeMapper->isObjectType())
        getOrCreateClusterFor(typeMapper);
    else if(typeMapper->isAggregateType())
        getOrCreateAggregateTypeDescriptorFor(typeMapper);
    else if(typeMapper->isReferenceType())
        scanReferenceTypeDependencies(typeMapper);
}

SerializationClusterPtr Serializer::getOrCreateClusterFor(const TypeMapperPtr &typeMapper)
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

void Serializer::writeHeader()
{
    output->writeUInt32(CoalMagicNumber);
    output->writeUInt8(CoalVersionMajor);
    output->writeUInt8(CoalVersionMinor);
    output->writeUInt16(0); // Reserved

    output->writeUInt32(uint32_t(binaryBlobBuilder.getDataSize())); // Blob size
    output->writeUInt32(typeDescriptorContext.getValueTypeCount()); // Value type layouts size
    output->writeUInt32(clusters.size()); // Cluster Count
    output->writeUInt32(objectCount); // Cluster Count
}

void Serializer::writeBlob()
{
    output->writeBlob(&binaryBlobBuilder);
}

void Serializer::writeValueTypeLayouts()
{
    output->setTypeDescriptorContext(&typeDescriptorContext);
    typeDescriptorContext.writeValueTypeLayoutsWith(output);
}

void Serializer::writeClusterDescriptions()
{
    for(auto &cluster : clusters)
        cluster->writeDescriptionWith(output);
}

void Serializer::writeClusterInstances()
{
    for(auto &cluster : clusters)
        cluster->writeInstancesWith(output);
}

void Serializer::writeTrailerForObject(const ObjectMapperPtr &rootObject)
{
    output->writeUInt32(objectPointerToInstanceIndexTable.at(rootObject->getObjectBasePointer()) + 1);
}

void Serializer::prepareForWriting()
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

#pragma endregion Serializer

#pragma region Deserializer

Deserializer::Deserializer(ReadStream *initialInput)
    : input(initialInput)
{
}

ObjectMapperPtr Deserializer::deserializeRootObject(const TypeMapperPtr &rootTypeMapper)
{
    if(!typeMapperRegistry)
        typeMapperRegistry = TypeMapperRegistry::getOrCreateForTransitiveClosureOf(rootTypeMapper);

    if(!parseContent())
        return nullptr;
    return rootObject;
}

bool Deserializer::parseHeaderAndReadBlob()
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

bool Deserializer::parseContent()
{
    return parseHeaderAndReadBlob() &&
        parseValueTypeDescriptors() &&
        parseClusterDescriptors() &&
        validateAndResolveTypes() &&
        parseClusterInstances() &&
        parseTrailer();
}

bool Deserializer::parseValueTypeDescriptors()
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

bool Deserializer::parseClusterDescriptors()
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

bool Deserializer::validateAndResolveTypes()
{
    for(auto &type : clusterTypes)
        type->resolveTypeUsing(typeMapperRegistry->getTypeMapperWithName(type->getName()));

    for(auto &type : clusterTypes)
        type->resolveTypeFields();

    return true;
}

bool Deserializer::parseClusterInstances()
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

bool Deserializer::parseTrailer()
{
    uint32_t rootObjectIndex = 0;
    if(!input->readUInt32(rootObjectIndex) || rootObjectIndex > objectCount)
        return false;

    rootObject = rootObjectIndex > 0 ? instances[rootObjectIndex - 1] : 0;
    return true;
}

#pragma endregion Deserializer

} // End of namespace coal