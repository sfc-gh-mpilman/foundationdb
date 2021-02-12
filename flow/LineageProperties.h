/*
 * LineageProperties.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "flow/IRandom.h"
#include <tuple>
#include <atomic>
#include <mutex>
#include <unordered_map>

struct LineagePropertyBase {
    virtual ~LineagePropertyBase();
};

struct LineagePropertiesBase {
    std::atomic<bool> onHeap = false;
    std::atomic<unsigned> refCount;
    uint32_t id;
    virtual ~LineagePropertiesBase();
};

class LineageCollection {
    std::atomic<bool> sampling = false;
    uint64_t counter = 0;
    std::mutex mapMutex;
public:
    uint32_t add(LineagePropertiesBase* lineageProperties);
    void move(uint32_t idx, LineagePropertiesBase* properties);
    void remove(uint32_t id);
};

LineageCollection& lineageCollection();

template<class... Properties>
struct LineageProperties : LineagePropertiesBase {
    std::tuple<Properties...> members;
    ~LineageProperties() {
        if (onHeap) { return; }
        if (refCount.load() > 0) {
            auto n = new LineageProperties<Properties...>{*this};
            lineageCollection().move(id, n);
        } else {
            lineageCollection().remove(id);
        }
    }
};

struct LineagePropertiesPtr {
    LineagePropertiesBase* ptr = nullptr;
    uint32_t id = 0;
    LineagePropertiesPtr() = default;
    explicit LineagePropertiesPtr(LineagePropertiesBase* ptr);
};
