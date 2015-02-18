/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <stdio.h>

// Returns a FILE* that writes all its output to @out, but attempts
// not to mix lines if multiple threads write to it simultaenously.
FILE* smp_synchronize_lines(FILE* out);

