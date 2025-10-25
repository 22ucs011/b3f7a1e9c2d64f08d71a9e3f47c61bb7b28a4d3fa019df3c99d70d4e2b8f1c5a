#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/config.h"
#include "common/exception.h"

namespace kizuna
{
    // Page layout for the slotted-page heap with a buddy-style header.
    struct PageHeader
    {
        uint32_t page_id;           // 4
        uint32_t next_page_id;      // 8 (0 when no chain)
        uint32_t prev_page_id;      // 12 (0 when no chain)
        uint16_t record_count;      // 14
        uint16_t free_space_offset; // 16
        uint16_t slot_count;        // 18
        uint8_t page_type;          // 19
        uint8_t flags;              // 20 (spare bits for later)
        uint32_t lsn;               // 24 (future WAL hook)
    };

    class Page
    {
    public:
        static constexpr size_t kHeaderSize = config::PAGE_HEADER_SIZE;
        static_assert(kHeaderSize == sizeof(PageHeader), "config::PAGE_HEADER_SIZE must equal PageHeader size");

        Page()
        {
            std::memset(storage_.data(), 0, storage_.size());
            auto &h = header();
            h.page_id = 0;
            h.next_page_id = config::INVALID_PAGE_ID;
            h.prev_page_id = config::INVALID_PAGE_ID;
            h.record_count = 0;
            h.slot_count = 0;
            h.page_type = static_cast<uint8_t>(PageType::INVALID);
            h.flags = 0;
            h.lsn = 0;
            h.free_space_offset = static_cast<uint16_t>(kHeaderSize);
        }

        void init(PageType type, page_id_t id)
        {
            auto &h = header();
            h.page_id = id;
            h.next_page_id = config::INVALID_PAGE_ID;
            h.prev_page_id = config::INVALID_PAGE_ID;
            h.record_count = 0;
            h.slot_count = 0;
            h.page_type = static_cast<uint8_t>(type);
            h.flags = 0;
            h.lsn = 0;
            h.free_space_offset = static_cast<uint16_t>(kHeaderSize);
        }

        uint8_t *data() { return storage_.data(); }
        const uint8_t *data() const { return storage_.data(); }

        PageHeader &header() { return *reinterpret_cast<PageHeader *>(storage_.data()); }
        const PageHeader &header() const { return *reinterpret_cast<const PageHeader *>(storage_.data()); }

        page_id_t next_page_id() const { return header().next_page_id; }
        void set_next_page_id(page_id_t id) { header().next_page_id = id; }
        page_id_t prev_page_id() const { return header().prev_page_id; }
        void set_prev_page_id(page_id_t id) { header().prev_page_id = id; }

        static constexpr size_t page_size() { return config::PAGE_SIZE; }
        static constexpr size_t slot_size() { return sizeof(uint16_t); }

        size_t free_bytes() const
        {
            const auto &h = header();
            const size_t records_limit = page_size() - (static_cast<size_t>(h.slot_count) + 1) * slot_size();
            if (h.free_space_offset > records_limit) return 0;
            return records_limit - h.free_space_offset;
        }

        bool insert(const uint8_t *payload, uint16_t len, slot_id_t &out_slot)
        {
            auto &h = header();
            auto type = static_cast<PageType>(h.page_type);
            if (type == PageType::INVALID)
            {
                h.page_type = static_cast<uint8_t>(PageType::DATA);
                type = PageType::DATA;
            }
            if (type != PageType::DATA)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE,
                                      "Insert on non-DATA page",
                                      std::to_string(h.page_id));
            }

            const uint16_t max_slots = static_cast<uint16_t>((page_size() - kHeaderSize) / slot_size());
            if (h.slot_count > max_slots)
            {
                h.slot_count = 0;
                h.record_count = 0;
            }
            if (h.record_count > h.slot_count)
            {
                h.record_count = h.slot_count;
            }
            if (h.free_space_offset < kHeaderSize || h.free_space_offset > page_size())
            {
                h.free_space_offset = static_cast<uint16_t>(kHeaderSize);
            }

            const size_t needed = static_cast<size_t>(len) + 2 + slot_size();
            if (needed > free_bytes())
                return false;

            const size_t used_slots_after = static_cast<size_t>(h.slot_count + 1) * slot_size();
            const size_t records_limit = page_size() - used_slots_after;
            size_t record_start = h.free_space_offset;
            if (record_start + 2 + static_cast<size_t>(len) > records_limit)
                return false;

            uint8_t *base = data();
            base[record_start + 0] = static_cast<uint8_t>(len & 0xFF);
            base[record_start + 1] = static_cast<uint8_t>((len >> 8) & 0xFF);
            std::memcpy(base + record_start + 2, payload, len);

            uint16_t slot_offset = static_cast<uint16_t>(record_start);
            const size_t slot_pos = page_size() - ((static_cast<size_t>(h.slot_count) + 1) * slot_size());
            std::memcpy(base + slot_pos, &slot_offset, sizeof(slot_offset));

            h.slot_count += 1;
            h.record_count += 1;
            out_slot = static_cast<slot_id_t>(h.slot_count - 1);
            h.free_space_offset = static_cast<uint16_t>(record_start + 2 + len);
            return true;
        }

        bool read(slot_id_t slot, std::vector<uint8_t> &out) const
        {
            const auto type = static_cast<PageType>(header().page_type);
            if (type != PageType::DATA && type != PageType::INVALID)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE,
                                      "Read on non-DATA page",
                                      std::to_string(header().page_id));
            }
            if (slot >= header().slot_count) return false;
            const uint8_t *base = data();
            const size_t slot_pos = page_size() - ((static_cast<size_t>(slot) + 1) * slot_size());
            uint16_t record_off = 0;
            std::memcpy(&record_off, base + slot_pos, sizeof(record_off));
            if (record_off == 0xFFFF) return false;
            const auto &h = header();
            size_t records_end = h.free_space_offset;
            if (records_end < kHeaderSize || records_end > page_size())
            {
                records_end = kHeaderSize;
            }
            if (record_off + 2 > records_end) return false;
            const uint16_t len = static_cast<uint16_t>(base[record_off]) | (static_cast<uint16_t>(base[record_off + 1]) << 8);
            if (record_off + 2 + len > records_end) return false;
            out.resize(len);
            std::memcpy(out.data(), base + record_off + 2, len);
            return true;
        }

        bool erase(slot_id_t slot)
        {
            auto &h = header();
            auto type = static_cast<PageType>(h.page_type);
            if (type == PageType::INVALID)
            {
                h.page_type = static_cast<uint8_t>(PageType::DATA);
                type = PageType::DATA;
            }
            if (type != PageType::DATA)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE,
                                      "Erase on non-DATA page",
                                      std::to_string(h.page_id));
            }
            const uint16_t max_slots = static_cast<uint16_t>((page_size() - kHeaderSize) / slot_size());
            if (h.slot_count > max_slots)
            {
                h.slot_count = 0;
                h.record_count = 0;
            }
            if (h.record_count > h.slot_count)
            {
                h.record_count = h.slot_count;
            }
            if (h.free_space_offset < kHeaderSize || h.free_space_offset > page_size())
            {
                h.free_space_offset = static_cast<uint16_t>(kHeaderSize);
            }
            if (slot >= h.slot_count) return false;
            uint8_t *base = data();
            const size_t slot_pos = page_size() - ((static_cast<size_t>(slot) + 1) * slot_size());
            uint16_t record_off = 0;
            std::memcpy(&record_off, base + slot_pos, sizeof(record_off));
            if (record_off == 0xFFFF) return false;

            uint16_t tomb = 0xFFFF;
            std::memcpy(base + slot_pos, &tomb, sizeof(tomb));
            header().record_count -= 1;
            return true;
        }
        bool update(slot_id_t slot, const uint8_t *payload, uint16_t len)
        {
            auto &h = header();
            auto type = static_cast<PageType>(h.page_type);
            if (type == PageType::INVALID)
            {
                h.page_type = static_cast<uint8_t>(PageType::DATA);
                type = PageType::DATA;
            }
            if (type != PageType::DATA)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE,
                                      "Update on non-DATA page",
                                      std::to_string(h.page_id));
            }
            if (slot >= h.slot_count) return false;
            uint8_t *base = data();
            const size_t slot_pos = page_size() - ((static_cast<size_t>(slot) + 1) * slot_size());
            uint16_t record_off = 0;
            std::memcpy(&record_off, base + slot_pos, sizeof(record_off));
            if (record_off == 0xFFFF) return false;
            const uint16_t current_len = static_cast<uint16_t>(base[record_off]) | (static_cast<uint16_t>(base[record_off + 1]) << 8);
            if (len > current_len)
                return false;
            base[record_off + 0] = static_cast<uint8_t>(len & 0xFF);
            base[record_off + 1] = static_cast<uint8_t>((len >> 8) & 0xFF);
            std::memcpy(base + record_off + 2, payload, len);
            if (current_len > len)
            {
                std::memset(base + record_off + 2 + len, 0, current_len - len);
            }
            return true;
        }

    private:
        std::array<uint8_t, config::PAGE_SIZE> storage_{};
    };
}

