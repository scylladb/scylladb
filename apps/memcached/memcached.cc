/*
 * Copyright 2014 Cloudius Systems
 * memcached
 */

#include "memcached.hh"

int main(int ac, char** av)
{
    constexpr bool WithFlashCache = false;
    memcache_instance<WithFlashCache> instance;
    return instance.run(ac, av);
}
