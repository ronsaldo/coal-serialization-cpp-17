#include "coal-serialization/coal.hpp"
#include "coal-serialization/coal-std-bindings.hpp"

#include <fstream>

void writeDataToFile(const std::string &filename, const std::vector<uint8_t> &data)
{
    std::ofstream out(filename, std::ios::binary | std::ios::out | std::ios::trunc);
    out.write(reinterpret_cast<const char*> (data.data()), data.size());
}

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

        writeDataToFile("utf8_32_32-hello.coal", coal::serialize<std::string> ("Hello World\n\r"));

        writeDataToFile("array32-1-2-3-3-42.coal", coal::serialize(std::vector<int>{1, 2, 3, 3, 42}));
        writeDataToFile("array32-Hello-World-crlf.coal", coal::serialize(std::vector<std::string>{"Hello", "World", "\r\n"}));

        writeDataToFile("set32-1-2-3-42.coal", coal::serialize(std::unordered_set<int>{1, 2, 3, 3, 42}));
        writeDataToFile("set32-Hello-World-crlf.coal", coal::serialize(std::unordered_set<std::string>{"Hello", "World", "\r\n"}));

        writeDataToFile("map32-First-1-Second-2-Third-3.coal", coal::serialize(std::map<std::string, int>{{"First", 1}, {"Second", 2}, {"Third", 3}}));
    }


    return 0;
}