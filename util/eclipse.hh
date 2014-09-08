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

#define RREF &&
#define LREF &

#else

// Eclipse doesn't grok && qualified member functions:
#define RREF
#define LREF
// Eclipse doesn't grok alignof
#define alignof sizeof

#endif

#endif /* ECLIPSE_HH_ */
