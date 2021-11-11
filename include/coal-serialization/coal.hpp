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
#include <array>

#include <mutex> // for std::once_flag
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

class WriteStream;
class ReadStream;

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
    PrimitiveTypeDescriptorCount,

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
};

const char *typeDescriptorKindToString(TypeDescriptorKind kind);

/**
 * Binary blob builder
 */
class BinaryBlobBuilder
{
public:
    uint32_t getOffsetForBytes(const uint8_t *bytes, size_t dataSize) const;

    void pushBytes(const uint8_t *bytes, size_t dataSize);
    void internString8(const std::string &string);
    void internString16(const std::string &string);
    void internString32(const std::string &string);

    const uint8_t *getData() const;
    size_t getDataSize() const;

private:
    static constexpr size_t HashTableCapacity = 4096;
    static uint32_t hashForBytes(const uint8_t *bytes, size_t dataSize);

    std::vector<std::pair<size_t, size_t>> hashTable[HashTableCapacity];
    std::vector<uint8_t> data;
};

/**
 * Interface for a write stream.
 */
class WriteStream
{
public:
    virtual ~WriteStream();

    virtual void writeBytes(const uint8_t *data, size_t size) = 0;

    void writeUInt8(uint8_t value);
    void writeUInt16(uint16_t value);
    void writeUInt32(uint32_t value);
    void writeUInt64(uint64_t value);
    void writeInt8(int8_t value);
    void writeInt16(int16_t value);
    void writeInt32(int32_t value);
    void writeInt64(int64_t value);
    void writeFloat32(float value);
    void writeFloat64(float value);

    void writeBlob(const BinaryBlobBuilder *theBlob);
    void writeUTF8_32_8(const std::string &string);
    void writeUTF8_32_16(const std::string &string);
    void writeUTF8_32_32(const std::string &string);

    void setTypeDescriptorContext(TypeDescriptorContext *context);
    void writeTypeDescriptorForTypeMapper(const TypeMapperPtr &typeMapper);
    void setObjectPointerToIndexMap(const std::unordered_map<const void*, uint32_t> *map);

    void writeObjectPointerAsReference(const void *pointer);

private:
    const BinaryBlobBuilder *blob = nullptr;
    TypeDescriptorContext *typeDescriptorContext = nullptr;
    const std::unordered_map<const void*, uint32_t> *objectPointerToIndexMap = nullptr;
};

/**
 * Interface for a read stream.
 */
class ReadStream
{
public:
    virtual ~ReadStream();

    virtual bool readBytes(uint8_t *buffer, size_t size) = 0;
    virtual bool skipBytes(size_t size) = 0;

    bool readUInt8(uint8_t &destination);
    bool readUInt16(uint16_t &destination);
    bool readUInt32(uint32_t &destination);
    bool readUInt64(uint64_t &destination);
    bool readInt8(int8_t &destination);
    bool readInt16(int16_t &destination);
    bool readInt32(int32_t &destination);
    bool readInt64(int64_t &destination);
    bool readFloat32(float &destination);
    bool readFloat64(double &destination);

    bool readUTF8_32_8(std::string &output);
    bool readUTF8_32_16(std::string &output);
    bool readUTF8_32_32(std::string &output);

    bool readTypeDescriptor(TypeDescriptorPtr &typeDescriptor);

    void setTypeDescriptorContext(TypeDescriptorContext *context);
    void setBinaryBlob(const uint8_t *data, size_t size);

    void setInstances(const std::vector<ObjectMapperPtr> *theInstances);

    bool readInstanceReference(ObjectMapperPtr &destination);

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
    MemoryWriteStream(std::vector<uint8_t> &initialOutput);

    virtual void writeBytes(const uint8_t *data, size_t size) override;

private:
    std::vector<uint8_t> &output;
};

/**
 * Memory read stream
 */
class MemoryReadStream : public ReadStream
{
public:
    MemoryReadStream(const uint8_t *initialData, size_t initialDataSize);

    virtual bool readBytes(uint8_t *buffer, size_t size) override;
    virtual bool skipBytes(size_t size) override;

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
    virtual ~TypeDescriptor();

    virtual void writeDescriptionWith(WriteStream *output);
    virtual bool skipDataWith(ReadStream *input);

    TypeDescriptorKind kind;
};

/**
 * Structure type descriptor
 */
class StructTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override;
    virtual bool skipDataWith(ReadStream *input) override;

    uint32_t index = 0;
    TypeMapperPtr typeMapper;
};

/**
 * Fixed array type descriptor
 */
class FixedArrayTypeDescriptor : public TypeDescriptor
{
public:

    virtual void writeDescriptionWith(WriteStream *output) override;
    virtual bool skipDataWith(ReadStream *input) override;

    uint32_t size;
    TypeDescriptorPtr element;
};

/**
 * Array type descriptor
 */
class ArrayTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override;
    virtual bool skipDataWith(ReadStream *input) override;

    TypeDescriptorPtr element;
};

/**
 * Set type descriptor
 */
class SetTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override;
    virtual bool skipDataWith(ReadStream *input) override;

    TypeDescriptorPtr element;
};

/**
 * Map type descriptor
 */
class MapTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override;
    virtual bool skipDataWith(ReadStream *input) override;

    TypeDescriptorPtr key;
    TypeDescriptorPtr value;
};

/**
 * Structure type descriptor
 */
class ObjectReferenceTypeDescriptor : public TypeDescriptor
{
public:
    virtual void writeDescriptionWith(WriteStream *output) override;

    uint32_t index = 0;
    TypeMapperWeakPtr typeMapper;
};


/**
 * TypeDescriptorContext
 */
class TypeDescriptorContext
{
public:
    TypeDescriptorPtr getOrCreatePrimitiveTypeDescriptor(TypeDescriptorKind kind);
    TypeDescriptorPtr getForTypeMapper(const TypeMapperPtr &mapper);

    uint32_t getValueTypeCount();
    TypeDescriptorPtr addValueType(const TypeMapperPtr &mapper);

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder);
    void writeValueTypeLayoutsWith(WriteStream *output);
    bool readTypeDescriptorWith(TypeDescriptorPtr &descriptor, ReadStream *input);
    void addObjectTypeMapper(const TypeMapperPtr &typeMapper);

    TypeDescriptorPtr getOrCreateForTypedObjectReference(const TypeMapperPtr &objectType);
    TypeDescriptorPtr getOrCreateArrayTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &elementType);
    TypeDescriptorPtr getOrCreateSetTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &elementType);
    TypeDescriptorPtr getOrCreateMapTypeDescriptor(TypeDescriptorKind kind, const TypeDescriptorPtr &keyType, const TypeDescriptorPtr &valueType);

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

/**
 * Aggregate field description.
 */
struct FieldDescription
{
    FieldDescription() = default;
    FieldDescription(const std::string &initialName, const TypeMapperPtr &initialTypeMapper, const FieldAccessorPtr &initialAccessor);

    template<typename CT, typename MT>
    FieldDescription(const std::string &initialName, MT CT::*fieldPointer);

    std::string name;
    TypeMapperWeakPtr typeMapper;
    FieldAccessorPtr accessor;

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder) const;
    void writeDescriptionWith(WriteStream *output) const;
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

    bool readDescriptionWith(ReadStream *input);
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
    virtual ~TypeMapper();

    virtual bool isMaterializationAdaptationType() const;
    virtual bool isSerializationDependencyType() const;
    virtual bool isAggregateType() const;
    virtual bool isObjectType() const;
    virtual bool isReferenceType() const;

    virtual const std::string &getName() const = 0;

    virtual void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder);
    virtual TypeMapperPtr getResolvedType() const;
    virtual uint16_t getFieldCount() const;
    virtual FieldDescription *getFieldNamed(const std::string &fieldName);

    virtual void writeFieldDescriptionsWith(WriteStream *output) const;
    virtual void writeInstanceWith(void *basePointer, WriteStream *output);
    virtual void writeFieldWith(void *fieldPointer, WriteStream *output);

    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder);
    virtual void pushInstanceDataIntoBinaryBlob(void *instancePointer, BinaryBlobBuilder &binaryBlobBuilder);

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const;
    virtual bool readInstanceWith(void *basePointer, ReadStream *input);
    virtual bool skipInstanceWith(ReadStream *input);

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input);
    virtual bool skipFieldWith(ReadStream *input);

    virtual ObjectMapperPtr makeInstance();

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) = 0;

    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock);
    virtual void withTypeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock);

    virtual void objectReferencesInInstanceDo(void *instancePointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock);
    virtual void objectReferencesInFieldDo(void *fieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock);
};

/**
 * Object mapper interface.
 * I am an interface used for gluing different kinds of objects with serializer and deserializer.
 */
class ObjectMapper
{
public:
    virtual ~ObjectMapper();

    virtual TypeMapperPtr getTypeMapper() const = 0;
    virtual void *getObjectBasePointer() = 0;
    
    virtual std::shared_ptr<void> asObjectSharedPointer();
};

/**
 * Type mapper registry interface.
 * I am an interface used for looking up type mappers by name. I am typically used for customizing the materialization process
 */
class TypeMapperRegistry
{
public:
    virtual ~TypeMapperRegistry();

    virtual TypeMapperPtr getTypeMapperWithName(const std::string &name);

    static TypeMapperRegistryPtr getOrCreateForTransitiveClosureOf(const TypeMapperPtr &rootTypeMapper);
};

/**
 * Transitive closure type mapper registry
 * I am a type mapper registry that is built by using the transitive closure that starts with some specified types.
 */
class TransitiveClosureTypeMapperRegistry : public TypeMapperRegistry
{
public:
    virtual TypeMapperPtr getTypeMapperWithName(const std::string &name) override;
    void addWithDependencies(const TypeMapperPtr &typeMapper);

private:
    std::unordered_set<TypeMapperPtr> addedTypes;
    std::unordered_map<std::string, TypeMapperPtr> nameMap;
};

/**
 * I am an accessor for a field.
 */
class FieldAccessor
{
public:
    virtual ~FieldAccessor();

    virtual void *getPointerForBasePointer(void *basePointer) = 0;
};

/**
 * Aggregate type mapper
 * I am object type mapper
 */
class PrimitiveTypeMapper : public TypeMapper
{
public:

    virtual const std::string &getName() const override;

    std::string name;
};

/**
 * Aggregate type mapper
 * I am object type mapper
 */
class AggregateTypeMapper : public TypeMapper
{
public:
    virtual bool isAggregateType() const override;
    virtual bool isSerializationDependencyType() const override;

    virtual const std::string &getName() const override;

    virtual void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder) override;

    virtual uint16_t getFieldCount() const override;
    virtual FieldDescription *getFieldNamed(const std::string &name) override;

    virtual void writeFieldDescriptionsWith(WriteStream *output) const override;
    virtual void writeInstanceWith(void *basePointer, WriteStream *output) override;

    virtual void pushInstanceDataIntoBinaryBlob(void *instancePointer, BinaryBlobBuilder &binaryBlobBuilder) override;
    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock) override;

protected:

    void addFields(const std::vector<FieldDescription> &newFields);

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
    virtual bool isObjectType() const override;

    virtual ObjectMapperPtr makeInstance() override;

    static TypeMapperPtr makeWithFields(const std::string &name, const TypeMapperPtr &superType, const ObjectMapperFactory &factory, const std::vector<FieldDescription> &fields);

    virtual void writeFieldWith(void *, WriteStream *) override;
    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *) override;

    void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock) override;

    virtual void objectReferencesInInstanceDo(void *instancePointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override;

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
    static TypeMapperPtr makeWithFields(const std::string &name, const std::vector<FieldDescription> &fields);

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output) override;
    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder) override;

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override;
    virtual bool readFieldWith(void *basePointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override;

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *) override;
    virtual void objectReferencesInFieldDo(void *baseFieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override;
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

    virtual bool isAggregateType() const override;
    virtual bool isMaterializationAdaptationType() const override;

    virtual const std::string &getName() const override;
    virtual TypeMapperPtr getResolvedType() const override;

    virtual void pushDataIntoBinaryBlob(BinaryBlobBuilder &) override;

    virtual uint16_t getFieldCount() const override;

    virtual void writeFieldDescriptionsWith(WriteStream *) const override;
    virtual void writeFieldWith(void *, WriteStream *) override;

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *) override;

    void resolveTypeUsing(const TypeMapperPtr &newResolveType);
    void resolveTypeFields();
};

/**
 * Type mapper used to resolve differences in structure types.
 */
class StructureMaterializationTypeMapper : public MaterializationTypeMapper
{
public:
    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override;
    virtual bool readFieldWith(void *basePointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override;
};

/**
 * Type mapper used to resolve differences in structure types.
 */
class ObjectMaterializationTypeMapper : public MaterializationTypeMapper
{
public:
    virtual bool isObjectType() const override;
    virtual ObjectMapperPtr makeInstance() override;

    virtual bool readInstanceWith(void *basePointer, ReadStream *input) override;
    virtual bool skipInstanceWith(ReadStream *input) override;

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

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override
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

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override
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


    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) override
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

    void pushDataIntoBinaryBlob(BinaryBlobBuilder &binaryBlobBuilder);

    void addObject(const ObjectMapperPtr &object);

    void writeDescriptionWith(WriteStream *output);
    void writeInstancesWith(WriteStream *output);
};

/**
 * Coal serializer.
 */
class Serializer
{
public:
    Serializer(WriteStream *initialOutput);

    template<typename ROT>
    void serializeRootObjectOrValue(ROT &&root)
    {
        serializeRootObject(ObjectMapperClassFor<ROT>::type::makeFor(&objectPointerToMapperMap, root));
    }

    void serializeRootObject(const ObjectMapperPtr &object);

private:
    enum class ValueTypeScanColor: uint8_t
    {
        White = 0,
        Gray,
        Black
    };

    void addPendingObject(const ObjectMapperPtr &object);
    void tracePendingObjects();
    void tracePendingObject(const ObjectMapperPtr &object);

    TypeDescriptorPtr getOrCreateAggregateTypeDescriptorFor(const TypeMapperPtr &typeMapper);
    void scanReferenceTypeDependencies(const TypeMapperPtr &typeMapper);
    void scanTypeMapperDependency(const TypeMapperPtr &typeMapper);
    SerializationClusterPtr getOrCreateClusterFor(const TypeMapperPtr &typeMapper);

    void writeHeader();
    void writeBlob();
    void writeValueTypeLayouts();
    void writeClusterDescriptions();
    void writeClusterInstances();
    void writeTrailerForObject(const ObjectMapperPtr &rootObject);
    void prepareForWriting();

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
    Deserializer(ReadStream *initialInput);

    template<typename T>
    std::optional<T> deserializeRootObjectOrValueOfType()
    {
        typedef typename ObjectMapperClassFor<T>::type RootObjectMapperClass;

        auto result = deserializeRootObject(RootObjectMapperClass::typeMapperSingleton());
        return RootObjectMapperClass::unwrapDeserializedRootObjectOrValue(result);
    }

    ObjectMapperPtr deserializeRootObject(const TypeMapperPtr &rootTypeMapper);

private:
    bool parseHeaderAndReadBlob();
    bool parseContent();
    bool parseValueTypeDescriptors();
    bool parseClusterDescriptors();
    bool validateAndResolveTypes();
    bool parseClusterInstances();
    bool parseTrailer();

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
