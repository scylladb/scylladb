/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#ifndef ECLIPSE_HH_
#define ECLIPSE_HH_

// Workarounds for deficiencies in Eclipse's C++ parser
//
// Tell Eclipse that IN_ECLIPSE is defined so it will ignore all the unknown syntax.

#ifndef IN_ECLIPSE

#else

// Eclipse doesn't grok alignof
#define alignof sizeof

#endif

#endif /* ECLIPSE_HH_ */
