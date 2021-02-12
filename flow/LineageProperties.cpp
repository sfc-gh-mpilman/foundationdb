/*
 * LineageProperties.cpp
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
#include "flow/LineageProperties.h"
#include <mutex>

LineagePropertyBase::~LineagePropertyBase() {}
LineagePropertiesBase::~LineagePropertiesBase() {}

LineageCollection& lineageCollection() {
    static LineageCollection res;
    return res;
}

uint64_t LineageCollection::add(LineagePropertiesBase *lineageProperties) {
    auto res = ++counter;
    std::unique_lock<std::mutex> _{mapMutex};
    return res;
}

LineagePropertiesPtr::LineagePropertiesPtr(LineagePropertiesBase* ptr)
    : ptr(ptr->onHeap ? ptr : nullptr), id(ptr->id) {
    ++ptr->refCount;
}
