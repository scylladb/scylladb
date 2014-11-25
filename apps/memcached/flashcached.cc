/*
 * Copyright 2014 Cloudius Systems
 * flashcached
 */

#include "memcached.hh"

int main(int ac, char** av)
{
    constexpr bool WithFlashCache = true;
    memcache_instance<WithFlashCache> instance;
    return instance.run(ac, av);
}
