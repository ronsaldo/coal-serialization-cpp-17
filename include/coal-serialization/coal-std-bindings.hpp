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

#ifndef COAL_SERIALIZATION_COAL_STD_BINDINGS_HPP
#define COAL_SERIALIZATION_COAL_STD_BINDINGS_HPP

#pragma once

#include "coal.hpp"

#include <array>
#include <map>
#include <set>

namespace coal
{
/**
 * std::string type mapper.
 */
class StdStringTypeMapper : public PrimitiveTypeMapper
{
public:
    static constexpr bool IsObjectType = false;
    static constexpr bool IsReferenceType = false;

    static TypeMapperPtr uniqueInstance();

    StdStringTypeMapper();

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output) override;
    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder) override;

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override;

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override;
    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) override;
};

template<>
struct TypeMapperFor<std::string> : SingletonTypeMapperFor<StdStringTypeMapper> {};

/**
 * std::vector type mapper.
 */
template<typename ET>
class StdVectorTypeMapper : public PrimitiveTypeMapper
{
public:
    static constexpr bool IsObjectType = false;
    static constexpr bool IsReferenceType = false;

    typedef StdVectorTypeMapper<ET> ThisType;

    static TypeMapperPtr uniqueInstance()
    {
        static auto singleton = std::make_shared<ThisType> ();
        return singleton;
    }

    StdVectorTypeMapper()
    {
        name = typeDescriptorKindToString(TypeDescriptorKind::Array32);
    }

    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock) override
    {
        typeMapperForType<ET> ()->withTypeMapperDependenciesDo(aBlock);
    }

    virtual void objectReferencesInFieldDo(void *fieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override
    {
        auto &vector = *reinterpret_cast<std::vector<ET>*> (fieldPointer);
        auto elementType = typeMapperForType<ET> ();
        for(auto &element : vector)
            elementType->objectReferencesInFieldDo(&element, cache, aBlock);
    }

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output) override
    {
        auto &vector = *reinterpret_cast<std::vector<ET>*> (fieldPointer);
        output->writeUInt32(uint32_t(vector.size()));
        auto elementType = typeMapperForType<ET> ();
        for(auto &element : vector)
            elementType->writeFieldWith(&element, output);
    }

    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder) override
    {
        auto &vector = *reinterpret_cast<std::vector<ET>*> (fieldPointer);
        auto elementType = typeMapperForType<ET> ();
        for(auto &element : vector)
            elementType->pushFieldDataIntoBinaryBlob(&element, binaryBlobBuilder);
    }

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override
    {
        switch(encoding->kind)
        {
        case TypeDescriptorKind::Array8:
        case TypeDescriptorKind::Array16:
        case TypeDescriptorKind::Array32:
            {
                auto targetTypeMapper = typeMapperForType<ET> ();
                auto elementTypeDescriptor = std::static_pointer_cast<ArrayTypeDescriptor> (encoding)->element;
                return targetTypeMapper->canReadFieldWithTypeDescriptor(elementTypeDescriptor);
            }
        default:
            return false;
        }
    }

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override
    {
        auto &destination = *reinterpret_cast<std::vector<ET>*> (fieldPointer);

        switch(fieldEncoding->kind)
        {
        case TypeDescriptorKind::Array8:
            {
                uint8_t count = 0;
                if(!input->readUInt8(count))
                    return false;
                destination.resize(count);
            }
            break;
        case TypeDescriptorKind::Array16:
            {
                uint16_t count = 0;
                if(!input->readUInt16(count))
                    return false;
                destination.resize(count);
            }
            break;
        case TypeDescriptorKind::Array32:
            {
                uint32_t count = 0;
                if(!input->readUInt32(count))
                    return false;
                destination.resize(count);
            }
            break;
        default:
            return false;
        }

        auto targetTypeMapper = typeMapperForType<ET> ();
        auto elementTypeDescriptor = std::static_pointer_cast<ArrayTypeDescriptor> (fieldEncoding)->element;
        for(size_t i = 0; i < destination.size(); ++i)
        {
            auto elementFieldPointer = &destination[i];
            if(!targetTypeMapper->readFieldWith(elementFieldPointer, elementTypeDescriptor, input))
                return false;
        }

        return true;
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) override
    {
        return context->getOrCreateArrayTypeDescriptor(TypeDescriptorKind::Array32, 
            context->getForTypeMapper(typeMapperForType<ET> ())
        );
    }
};

template<typename ET>
struct TypeMapperFor<std::vector<ET>> : SingletonTypeMapperFor<StdVectorTypeMapper<ET>> {};

/**
 * std::(unordered_)set type mapper.
 */
template<typename CT>
class StdSetTypeMapper : public PrimitiveTypeMapper
{
public:
    static constexpr bool IsObjectType = false;
    static constexpr bool IsReferenceType = false;

    typedef CT ContainerType;
    typedef typename CT::value_type ElementType;
    typedef StdSetTypeMapper<CT> ThisType;

    static TypeMapperPtr uniqueInstance()
    {
        static auto singleton = std::make_shared<ThisType> ();
        return singleton;
    }

    StdSetTypeMapper()
    {
        name = typeDescriptorKindToString(TypeDescriptorKind::Set32);
    }

    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock) override
    {
        typeMapperForType<ElementType> ()->withTypeMapperDependenciesDo(aBlock);
    }

    virtual void objectReferencesInFieldDo(void *fieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override
    {
        auto &set = *reinterpret_cast<ContainerType*> (fieldPointer);
        auto elementType = typeMapperForType<ElementType> ();
        for(auto &element : set)
            elementType->objectReferencesInFieldDo(const_cast<void*> (static_cast<const void*> (&element)), cache, aBlock);
    }

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output) override
    {
        auto &set = *reinterpret_cast<ContainerType*> (fieldPointer);
        output->writeUInt32(uint32_t(set.size()));
        auto elementType = typeMapperForType<ElementType> ();
        for(auto &element : set)
            elementType->writeFieldWith(const_cast<void*> (static_cast<const void*> (&element)), output);
    }

    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder) override
    {
        auto &set = *reinterpret_cast<ContainerType*> (fieldPointer);
        auto elementType = typeMapperForType<ElementType> ();
        for(auto &element : set)
            elementType->pushFieldDataIntoBinaryBlob(const_cast<void*> (static_cast<const void*> (&element)), binaryBlobBuilder);
    }

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override
    {
        switch(encoding->kind)
        {
        case TypeDescriptorKind::Set8:
        case TypeDescriptorKind::Set16:
        case TypeDescriptorKind::Set32:
            {
                auto targetTypeMapper = typeMapperForType<ElementType> ();
                auto elementTypeDescriptor = std::static_pointer_cast<SetTypeDescriptor> (encoding)->element;
                return targetTypeMapper->canReadFieldWithTypeDescriptor(elementTypeDescriptor);
            }
        default:
            return false;
        }
    }

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override
    {
        auto &destination = *reinterpret_cast<ContainerType*> (fieldPointer);
        size_t elementCount = 0;

        switch(fieldEncoding->kind)
        {
        case TypeDescriptorKind::Set8:
            {
                uint8_t count = 0;
                if(!input->readUInt8(count))
                    return false;
                elementCount = count;
            }
            break;
        case TypeDescriptorKind::Set16:
            {
                uint16_t count = 0;
                if(!input->readUInt16(count))
                    return false;
                elementCount = count;
            }
            break;
        case TypeDescriptorKind::Set32:
            {
                uint32_t count = 0;
                if(!input->readUInt32(count))
                    return false;
                elementCount = count;
            }
            break;
        default:
            return false;
        }

        auto targetTypeMapper = typeMapperForType<ElementType> ();
        auto elementTypeDescriptor = std::static_pointer_cast<SetTypeDescriptor> (fieldEncoding)->element;
        for(size_t i = 0; i < elementCount; ++i)
        {
            ElementType readedElement;
            if(!targetTypeMapper->readFieldWith(&readedElement, elementTypeDescriptor, input))
                return false;

            destination.insert(readedElement);
        }

        return true;
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) override
    {
        return context->getOrCreateSetTypeDescriptor(TypeDescriptorKind::Set32, 
            context->getForTypeMapper(typeMapperForType<ElementType> ())
        );
    }
};

template<typename ET>
struct TypeMapperFor<std::set<ET>> : SingletonTypeMapperFor<StdSetTypeMapper<std::set<ET>>> {};

template<typename ET>
struct TypeMapperFor<std::unordered_set<ET>> : SingletonTypeMapperFor<StdSetTypeMapper<std::unordered_set<ET>>> {};

/**
 * std::(unordered_)map type mapper.
 */
template<typename CT>
class StdMapTypeMapper : public PrimitiveTypeMapper
{
public:
    static constexpr bool IsObjectType = false;
    static constexpr bool IsReferenceType = false;

    typedef CT ContainerType;
    typedef typename CT::value_type ElementType;
    typedef typename CT::key_type KeyType;
    typedef typename CT::mapped_type ValueType;
    typedef StdMapTypeMapper<CT> ThisType;

    static TypeMapperPtr uniqueInstance()
    {
        static auto singleton = std::make_shared<ThisType> ();
        return singleton;
    }

    StdMapTypeMapper()
    {
        name = typeDescriptorKindToString(TypeDescriptorKind::Map32);
    }

    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock) override
    {
        typeMapperForType<KeyType> ()->withTypeMapperDependenciesDo(aBlock);
        typeMapperForType<ValueType> ()->withTypeMapperDependenciesDo(aBlock);
    }

    virtual void objectReferencesInFieldDo(void *fieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override
    {
        auto &map = *reinterpret_cast<ContainerType*> (fieldPointer);
        auto keyType = typeMapperForType<KeyType> ();
        auto valueType = typeMapperForType<ValueType> ();
        for(auto &element : map)
        {
            keyType->objectReferencesInFieldDo(const_cast<void*> (static_cast<const void*> (&element.first)), cache, aBlock);
            valueType->objectReferencesInFieldDo(&element.second, cache, aBlock);
        }
    }

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output) override
    {
        auto &map = *reinterpret_cast<ContainerType*> (fieldPointer);
        output->writeUInt32(uint32_t(map.size()));

        auto keyType = typeMapperForType<KeyType> ();
        auto valueType = typeMapperForType<ValueType> ();
        for(auto &element : map)
        {
            keyType->writeFieldWith(const_cast<void*> (static_cast<const void*> (&element.first)), output);
            valueType->writeFieldWith(&element.second, output);
        }
    }

    virtual void pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder) override
    {
        auto &map = *reinterpret_cast<ContainerType*> (fieldPointer);

        auto keyType = typeMapperForType<KeyType> ();
        auto valueType = typeMapperForType<ValueType> ();
        for(auto &element : map)
        {
            keyType->pushFieldDataIntoBinaryBlob(const_cast<void*> (static_cast<const void*> (&element.first)), binaryBlobBuilder);
            valueType->pushFieldDataIntoBinaryBlob(&element.second, binaryBlobBuilder);
        }
    }

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override
    {
        switch(encoding->kind)
        {
        case TypeDescriptorKind::Map8:
        case TypeDescriptorKind::Map16:
        case TypeDescriptorKind::Map32:
            {
                auto targetKeyTypeMapper = typeMapperForType<KeyType> ();
                auto targetValueTypeMapper = typeMapperForType<ValueType> ();
                auto mapTypeDescriptor = std::static_pointer_cast<MapTypeDescriptor> (encoding);
                return targetKeyTypeMapper->canReadFieldWithTypeDescriptor(mapTypeDescriptor->key) &&
                    targetValueTypeMapper->canReadFieldWithTypeDescriptor(mapTypeDescriptor->value);
            }
        default:
            return false;
        }
    }

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override
    {
        auto &destination = *reinterpret_cast<ContainerType*> (fieldPointer);
        size_t elementCount = 0;

        switch(fieldEncoding->kind)
        {
        case TypeDescriptorKind::Map8:
            {
                uint8_t count = 0;
                if(!input->readUInt8(count))
                    return false;
                elementCount = count;
            }
            break;
        case TypeDescriptorKind::Map16:
            {
                uint16_t count = 0;
                if(!input->readUInt16(count))
                    return false;
                elementCount = count;
            }
            break;
        case TypeDescriptorKind::Map32:
            {
                uint32_t count = 0;
                if(!input->readUInt32(count))
                    return false;
                elementCount = count;
            }
            break;
        default:
            return false;
        }

        auto targetKeyTypeMapper = typeMapperForType<KeyType> ();
        auto targetValueTypeMapper = typeMapperForType<ValueType> ();
        auto mapTypeDescriptor = std::static_pointer_cast<MapTypeDescriptor> (fieldEncoding);
        for(size_t i = 0; i < elementCount; ++i)
        {
            KeyType readedKey;
            ValueType readedValue;
            if(!targetKeyTypeMapper->readFieldWith(&readedKey, mapTypeDescriptor->key, input) ||
               !targetValueTypeMapper->readFieldWith(&readedValue, mapTypeDescriptor->value, input))
                return false;

            destination.insert(std::make_pair(readedKey, readedValue));
        }

        return true;
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) override
    {
        return context->getOrCreateMapTypeDescriptor(TypeDescriptorKind::Map32, 
            context->getForTypeMapper(typeMapperForType<KeyType> ()),
            context->getForTypeMapper(typeMapperForType<ValueType> ())
        );
    }
};

template<typename KT, typename VT>
struct TypeMapperFor<std::map<KT, VT>> : SingletonTypeMapperFor<StdMapTypeMapper<std::map<KT, VT>>> {};

template<typename KT, typename VT>
struct TypeMapperFor<std::unordered_map<KT, VT>> : SingletonTypeMapperFor<StdMapTypeMapper<std::unordered_map<KT, VT>>> {};

class SharedObjectWrapper : public ObjectMapper
{
public:
    typedef std::shared_ptr<void> ValueTypePtr;
    typedef SharedObjectWrapper ThisType;

    SharedObjectWrapper(const ValueTypePtr &initialReference, const TypeMapperPtr &initialTypeMapper);

    virtual TypeMapperPtr getTypeMapper() const override;
    virtual void *getObjectBasePointer() override;
    virtual std::shared_ptr<void> asObjectSharedPointer() override;

    ValueTypePtr reference;
    TypeMapperPtr typeMapper;
};

/**
 * I wrap a specific std::shared_ptr<> instance.
 */
template<typename VT>
class SpecificSharedObjectWrapper
{
public:
    typedef VT ValueType;
    typedef std::shared_ptr<ValueType> ValueTypePtr;

    static TypeMapperPtr typeMapperSingleton()
    {
        return typeMapperForType<ValueType> ();
    }

    static ObjectMapperPtr makeFor(const ValueTypePtr &value)
    {
        auto typeMapper = value ? value->getCoalTypeMapper() : typeMapperForType<ValueType> ();
        return std::make_shared<SharedObjectWrapper> (value, typeMapper);
    }

    static ObjectMapperPtr makeFor(std::unordered_map<void *, ObjectMapperPtr> *cache, const ValueTypePtr &value)
    {
        if(!value)
            return nullptr;

        auto it = cache->find(value.get());
        if(it != cache->end())
            return it->second;

        auto result = makeFor(value);
        cache->insert({value.get(), result});
        return result;
    }

    static std::optional<ValueTypePtr> unwrapDeserializedRootObjectOrValue(const ObjectMapperPtr &deserializedRootObject)
    {
        if(!deserializedRootObject)
            return std::nullopt;

        return 
            std::reinterpret_pointer_cast<ValueType> (
                std::static_pointer_cast<SharedObjectWrapper> (deserializedRootObject)->reference
            );
    }
};

/**
 * Tag for marking a serializable shared object class.
 */
struct SerializableSharedObjectClassTag
{
    typedef void SerializableSuperType;

    virtual ~SerializableSharedObjectClassTag();

    virtual TypeMapperPtr getCoalTypeMapper() const = 0;
};

template<typename T>
ObjectMapperPtr makeSharedObjectWrapperFor(const std::shared_ptr<T> &reference)
{
    return SpecificSharedObjectWrapper<T>::makeFor(reference);
}

template<typename T>
ObjectMapperPtr makeNewSharedObject()
{
    return makeSharedObjectWrapperFor<T> (std::make_shared<T> ());
}

/**
 * std::shared_ptr reference type mapper.
 */
template<typename OT>
class SharedPtrTypeMapperFor : public PrimitiveTypeMapper
{
public:
    typedef OT ObjectType;
    typedef std::shared_ptr<OT> ObjectTypePtr;
    typedef SharedPtrTypeMapperFor<OT> ThisType;

    static constexpr bool IsObjectType = false;
    static constexpr bool IsReferenceType = true;

    static TypeMapperPtr uniqueInstance()
    {
        static auto singleton = std::make_shared<ThisType> ();
        return singleton;
    }

    SharedPtrTypeMapperFor()
    {
        name = typeDescriptorKindToString(TypeDescriptorKind::TypedObject);
    }

    virtual bool isSerializationDependencyType() const override
    {
        return true;
    }

    virtual bool isReferenceType() const override
    {
        return true;
    }

    virtual void writeFieldWith(void *fieldPointer, WriteStream *output) override
    {
        auto objectPointer = reinterpret_cast<ObjectTypePtr*> (fieldPointer);
        output->writeObjectPointerAsReference(objectPointer->get());
    }

    virtual bool canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const override
    {
        // Untyped object.
        if(encoding->kind == TypeDescriptorKind::Object)
            return true;
            
        if(encoding->kind != TypeDescriptorKind::TypedObject)
            return false;

        auto sourceTypeMapper = std::static_pointer_cast<ObjectReferenceTypeDescriptor> (encoding)->typeMapper.lock();
        auto targetTypeMapper = typeMapperForType<ObjectType> ();
        return sourceTypeMapper && sourceTypeMapper->getResolvedType() == targetTypeMapper;
    }

    virtual bool readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input) override
    {
        ObjectMapperPtr instance;
        if(!input->readInstanceReference(instance))
            return false;

        auto destination = reinterpret_cast<ObjectTypePtr*> (fieldPointer);
        destination->reset();
        if(!instance)
            return true;

        // Check the validity of the cast.
        if(fieldEncoding->kind == TypeDescriptorKind::Object)
        {
            // TODO: Check for a super type here.
            if(instance->getTypeMapper() != typeMapperForType<ObjectType> ())
                return true;
        }

        // Cast the instance.
        *destination = std::reinterpret_pointer_cast<ObjectType> (instance->asObjectSharedPointer());
        return true;
    }

    virtual void typeMapperDependenciesDo(const TypeMapperIterationBlock &aBlock) override
    {
        aBlock(typeMapperForType<ObjectType> ());
    }

    virtual TypeDescriptorPtr getOrCreateTypeDescriptor(TypeDescriptorContext *context) override
    {
        return context->getOrCreateForTypedObjectReference(typeMapperForType<ObjectType> ());
    }

    virtual void objectReferencesInFieldDo(void *baseFieldPointer, std::unordered_map<void*, ObjectMapperPtr> *cache, const ObjectReferenceIterationBlock &aBlock) override
    {
        auto referencePointer = reinterpret_cast<ObjectTypePtr*> (baseFieldPointer);
        if(!*referencePointer)
            return;

        auto wrapper = SpecificSharedObjectWrapper<ObjectType>::makeFor(cache, *referencePointer);
        aBlock(wrapper);
    }
};

template<typename T>
struct TypeMapperFor<std::shared_ptr<T>> : SingletonTypeMapperFor<SharedPtrTypeMapperFor<T>> {};

template<typename T>
struct ClassTypeMetadataFor<T, typename std::enable_if< std::is_base_of<SerializableSharedObjectClassTag, T>::value >::type>
{
    typedef void type;

    static ObjectMapperPtr newInstance()
    {
        return makeNewSharedObject<T> ();
    }

    static FieldDescriptions getFields()
    {
        return T::__coal_fields__();
    }

    static std::string getTypeName()
    {
        return T::__coal_typename__;
    }

    static TypeMapperPtr getSuperType()
    {
        return typeMapperForType<typename T::SerializableSuperType> ();
    }
};

template<typename T>
struct ObjectMapperClassFor<std::shared_ptr<T>, typename std::enable_if<TypeMapperFor<T>::IsObjectType>::type>
{
    typedef SpecificSharedObjectWrapper<T> type;
};

template<typename BaseType>
struct MakeSerializableSharedSubclassOfBase : BaseType {};

template<>
struct MakeSerializableSharedSubclassOfBase<void> : SerializableSharedObjectClassTag {};

template<typename SeT, typename SuT = void>
struct MakeSerializableSharedSubclassOf : MakeSerializableSharedSubclassOfBase<SuT>
{
    typedef SeT SelfType;
    typedef SuT SuperType;
    typedef SuT SerializableSuperType;

    static coal::FieldDescriptions __coal_fields__()
    {
        return {};
    }

    virtual coal::TypeMapperPtr getCoalTypeMapper() const override
    {
        return coal::typeMapperForType<SelfType> ();
    }
};

} // End of namespace coal

#endif //COAL_SERIALIZATION_COAL_STD_BINDINGS_HPP
