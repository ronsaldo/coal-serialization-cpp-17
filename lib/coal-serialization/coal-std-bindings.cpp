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

#include "coal-serialization/coal-std-bindings.hpp"

namespace coal
{

TypeMapperPtr StdStringTypeMapper::uniqueInstance()
{
    static auto singleton = std::make_shared<StdStringTypeMapper> ();
    return singleton;
}

StdStringTypeMapper::StdStringTypeMapper()
{
    name = typeDescriptorKindToString(TypeDescriptorKind::UTF8_32_32);
}

void StdStringTypeMapper::writeFieldWith(void *fieldPointer, WriteStream *output)
{
    auto string = reinterpret_cast<std::string*> (fieldPointer);
    output->writeUTF8_32_32(*string);
}

void StdStringTypeMapper::pushFieldDataIntoBinaryBlob(void *fieldPointer, BinaryBlobBuilder &binaryBlobBuilder)
{
    auto string = reinterpret_cast<std::string*> (fieldPointer);
    binaryBlobBuilder.internString32(*string);
}

bool StdStringTypeMapper::canReadFieldWithTypeDescriptor(const TypeDescriptorPtr &encoding) const
{
    switch(encoding->kind)
    {
    case TypeDescriptorKind::UTF8_32_8:
    case TypeDescriptorKind::UTF8_32_16:
    case TypeDescriptorKind::UTF8_32_32:
        return true;
    default:
        return false;
    }
}

bool StdStringTypeMapper::readFieldWith(void *fieldPointer, const TypeDescriptorPtr &fieldEncoding, ReadStream *input)
{
    auto destination = reinterpret_cast<std::string*> (fieldPointer);

    switch(fieldEncoding->kind)
    {
    case TypeDescriptorKind::UTF8_32_8:
        return input->readUTF8_32_8(*destination);
    case TypeDescriptorKind::UTF8_32_16:
        return input->readUTF8_32_16(*destination);
    case TypeDescriptorKind::UTF8_32_32:
        return input->readUTF8_32_32(*destination);
    default:
        return false;
    }
}

TypeDescriptorPtr StdStringTypeMapper::getOrCreateTypeDescriptor(TypeDescriptorContext *context)
{
    return context->getOrCreatePrimitiveTypeDescriptor(TypeDescriptorKind::UTF8_32_32);
}

} // End of namespace coal