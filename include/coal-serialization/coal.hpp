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

namespace coal
{

class StructType;
typedef std::shared_ptr<StructType> StructTypePtr;

class TypeDescriptor;
typedef std::shared_ptr<TypeDescriptor> TypeDescriptorPtr;

class ClusterDescription;
typedef std::shared_ptr<ClusterDescription> ClusterDescriptionPtr;

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

} // End of namespace coal

#endif //COAL_SERIALIZATION_COAL_HPP