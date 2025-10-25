#include "storage/record.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <string_view>

namespace kizuna::record
{
    namespace
    {
        inline void append_u16(std::vector<std::uint8_t> &buf, std::uint16_t v)
        {
            buf.push_back(static_cast<std::uint8_t>(v & 0xFF));
            buf.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFF));
        }

        inline bool read_u16(const std::uint8_t *&p, const std::uint8_t *end, std::uint16_t &out)
        {
            if (p + 2 > end) return false;
            out = static_cast<std::uint16_t>(p[0]) | (static_cast<std::uint16_t>(p[1]) << 8);
            p += 2;
            return true;
        }

        template <typename T>
        inline void append_pod(std::vector<std::uint8_t> &buf, const T &v)
        {
            const std::uint8_t *raw = reinterpret_cast<const std::uint8_t *>(&v);
            buf.insert(buf.end(), raw, raw + sizeof(T));
        }

        inline std::size_t nullmap_bytes(std::size_t column_count)
        {
            return (column_count + 7) / 8;
        }

        inline void set_null_bit(std::vector<std::uint8_t> &bitmap, std::size_t index)
        {
            const std::size_t byte_index = index / 8;
            const std::uint8_t bit = static_cast<std::uint8_t>(1u << (index % 8));
            bitmap[byte_index] = static_cast<std::uint8_t>(bitmap[byte_index] | bit);
        }

        inline bool get_null_bit(const std::uint8_t *bitmap, std::size_t bytes, std::size_t index)
        {
            const std::size_t byte_index = index / 8;
            if (byte_index >= bytes) return false;
            const std::uint8_t bit = static_cast<std::uint8_t>(1u << (index % 8));
            return (bitmap[byte_index] & bit) != 0;
        }
    } // namespace

    Field from_null(DataType declared_type)
    {
        Field f;
        f.type = declared_type;
        f.is_null = true;
        return f;
    }

    Field from_bool(bool v)
    {
        Field f;
        f.type = DataType::BOOLEAN;
        f.is_null = false;
        f.payload.push_back(static_cast<std::uint8_t>(v ? 1 : 0));
        return f;
    }

    Field from_int32(std::int32_t v)
    {
        Field f;
        f.type = DataType::INTEGER;
        f.is_null = false;
        append_pod(f.payload, v);
        return f;
    }

    Field from_int64(std::int64_t v)
    {
        Field f;
        f.type = DataType::BIGINT;
        f.is_null = false;
        append_pod(f.payload, v);
        return f;
    }

    Field from_double(double v)
    {
        Field f;
        f.type = DataType::DOUBLE;
        f.is_null = false;
        append_pod(f.payload, v);
        return f;
    }

    Field from_string(std::string_view s)
    {
        Field f;
        f.type = DataType::VARCHAR;
        f.is_null = false;
        f.payload.insert(f.payload.end(), s.begin(), s.end());
        return f;
    }

    Field from_date(std::int64_t days_since_epoch)
    {
        Field f;
        f.type = DataType::DATE;
        f.is_null = false;
        append_pod(f.payload, days_since_epoch);
        return f;
    }

    Field from_blob(const std::vector<std::uint8_t> &b)
    {
        Field f;
        f.type = DataType::BLOB;
        f.is_null = false;
        f.payload = b;
        return f;
    }

    std::vector<std::uint8_t> encode(const std::vector<Field> &fields)
    {
        const std::size_t count = fields.size();
        if (count > std::numeric_limits<std::uint16_t>::max())
        {
            KIZUNA_THROW_RECORD(StatusCode::INVALID_ARGUMENT, "Too many fields", std::to_string(count));
        }

        const std::size_t bitmap_len = nullmap_bytes(count);
        if (bitmap_len > std::numeric_limits<std::uint16_t>::max())
        {
            KIZUNA_THROW_RECORD(StatusCode::INVALID_ARGUMENT, "Null bitmap too large", std::to_string(bitmap_len));
        }

        std::vector<std::uint8_t> bitmap(bitmap_len, 0);
        for (std::size_t i = 0; i < count; ++i)
        {
            if (fields[i].is_null)
            {
                if (!fields[i].payload.empty())
                {
                    KIZUNA_THROW_RECORD(StatusCode::INVALID_ARGUMENT, "Null field had payload", std::to_string(i));
                }
                set_null_bit(bitmap, i);
            }
        }

        std::vector<std::uint8_t> out;
        out.reserve(4 + bitmap_len + count * 4);
        append_u16(out, static_cast<std::uint16_t>(count));
        append_u16(out, static_cast<std::uint16_t>(bitmap_len));
        out.insert(out.end(), bitmap.begin(), bitmap.end());

        std::size_t total = out.size();
        for (std::size_t i = 0; i < count; ++i)
        {
            const auto &field = fields[i];
            out.push_back(static_cast<std::uint8_t>(field.type));
            total += 1;

            std::uint16_t len = 0;
            if (!field.is_null)
            {
                if (field.payload.size() > std::numeric_limits<std::uint16_t>::max())
                {
                    KIZUNA_THROW_RECORD(StatusCode::RECORD_TOO_LARGE, "Field too large", std::to_string(field.payload.size()));
                }

                const std::size_t expected = get_type_size(field.type);
                if (expected > 0 && field.payload.size() != expected)
                {
                    KIZUNA_THROW_RECORD(StatusCode::INVALID_ARGUMENT, "Fixed field wrong size", std::to_string(i));
                }

                len = static_cast<std::uint16_t>(field.payload.size());
            }

            append_u16(out, len);
            total += 2;

            if (!field.is_null && len > 0)
            {
                out.insert(out.end(), field.payload.begin(), field.payload.end());
                total += len;
            }

            if (total > config::MAX_RECORD_SIZE)
            {
                KIZUNA_THROW_RECORD(StatusCode::RECORD_TOO_LARGE, "Encoded record too large", std::to_string(total));
            }
        }

        return out;
    }

    bool decode(const std::uint8_t *data, std::size_t len, std::vector<Field> &out_fields)
    {
        out_fields.clear();
        const std::uint8_t *p = data;
        const std::uint8_t *end = data + len;

        std::uint16_t count = 0;
        if (!read_u16(p, end, count)) return false;

        std::uint16_t bitmap_len = 0;
        if (!read_u16(p, end, bitmap_len)) return false;

        const std::size_t needed_bitmap = nullmap_bytes(count);
        if (bitmap_len < needed_bitmap) return false;
        if (p + bitmap_len > end) return false;

        const std::uint8_t *bitmap_ptr = p;
        p += bitmap_len;

        out_fields.reserve(count);
        for (std::uint16_t i = 0; i < count; ++i)
        {
            if (p >= end) return false;
            const DataType type = static_cast<DataType>(*p++);

            std::uint16_t field_len = 0;
            if (!read_u16(p, end, field_len)) return false;
            if (p + field_len > end) return false;

            Field f;
            f.type = type;
            f.is_null = get_null_bit(bitmap_ptr, bitmap_len, i);

            if (f.is_null)
            {
                if (field_len != 0) return false;
            }
            else if (field_len > 0)
            {
                f.payload.assign(p, p + field_len);
            }

            out_fields.emplace_back(std::move(f));
            p += field_len;
        }

        return p == end;
    }
}


