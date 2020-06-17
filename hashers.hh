/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "bytes.hh"
#include "hashing.hh"

class md5_hasher;

template <typename T, size_t size> class cryptopp_hasher : public hasher {
    struct impl;
    std::unique_ptr<impl> _impl;

public:
    cryptopp_hasher();
    ~cryptopp_hasher();
    cryptopp_hasher(cryptopp_hasher&&) noexcept;
    cryptopp_hasher(const cryptopp_hasher&);
    cryptopp_hasher& operator=(cryptopp_hasher&&) noexcept;
    cryptopp_hasher& operator=(const cryptopp_hasher&);

    bytes finalize();
    std::array<uint8_t, size> finalize_array();
    void update(const char* ptr, size_t length) override;

    // Use update and finalize to compute the hash over the full view.
    static bytes calculate(const std::string_view& s);
};

class md5_hasher final : public cryptopp_hasher<md5_hasher, 16> {};

class sha256_hasher final : public cryptopp_hasher<sha256_hasher, 32> {};
