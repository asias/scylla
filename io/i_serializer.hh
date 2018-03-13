/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cstdint>

namespace io {
class TypeSizes;

template<typename T>
class i_serializer {
public:
    /**
     * Serialize the specified type into the specified DataOutput instance.
     *
     *
     * @param t type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @throws java.io.IOException
     */
    virtual void serialize(T& t, DataOutputPlus out) = 0;

    /**
     * Deserialize from the specified DataInput instance.
     * @param in DataInput from which deserialization needs to happen.
     * @throws IOException
     * @return the type that was deserialized
     */
    virtual T& deserialize(DataInput in) = 0;

    virtual uint64_t serializedSize(T& t, TypeSizes type) = 0;
};

}
