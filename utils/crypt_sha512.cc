/*
 * This file originates from musl libc (git.musl-libc.org).
 * Modifications have been made and are licensed under the following terms:
 * Copyright (C) 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * public domain sha512 crypt implementation
 *
 * original sha crypt design: http://people.redhat.com/drepper/SHA-crypt.txt
 * in this implementation at least 32bit int is assumed,
 * key length is limited, the $6$ prefix is mandatory, '\n' and ':' is rejected
 * in the salt and rounds= setting must contain a valid iteration count,
 * on error "*" is returned.
 */
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <memory>

#include <openssl/evp.h>

#include "crypt_sha512.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

/* SHA-512 from libcrypto, which picks the fastest implementation the CPU supports */

struct sha512 {
	EVP_MD_CTX *ctx;
	bool failed;      /* a libcrypto call failed */
};

static const EVP_MD *sha512_md()
{
	/* fetched explicitly, since EVP_sha512() makes every init look the digest up again */
	static thread_local std::unique_ptr<EVP_MD, decltype(&EVP_MD_free)> md(nullptr, &EVP_MD_free);
	/* a failed fetch is retried on the next call */
	if (!md)
		md.reset(EVP_MD_fetch(nullptr, "SHA512", nullptr));
	return md.get();
}

static void sha512_init(struct sha512 *s)
{
	s->failed |= EVP_DigestInit_ex2(s->ctx, sha512_md(), nullptr) != 1;
}

static void sha512_sum(struct sha512 *s, uint8_t *md)
{
	s->failed |= EVP_DigestFinal_ex(s->ctx, md, nullptr) != 1;
}

static void sha512_update(struct sha512 *s, const void *m, unsigned long len)
{
	s->failed |= EVP_DigestUpdate(s->ctx, m, len) != 1;
}

static const unsigned char b64[] =
"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

static char *to64(char *s, unsigned int u, int n)
{
	while (--n >= 0) {
		*s++ = b64[u % 64];
		u /= 64;
	}
	return s;
}

/* key limit is not part of the original design, added for DoS protection.
 * rounds limit has been lowered (versus the reference/spec), also for DoS
 * protection. runtime is O(klen^2 + klen*rounds) */
#define KEY_MAX 256
#define SALT_MAX 16
#define ROUNDS_DEFAULT 5000
#define ROUNDS_MIN 1000
#define ROUNDS_MAX 9999999

/* hash n bytes of the repeated md message digest */
static void hashmd(struct sha512 *s, unsigned int n, const void *md)
{
	unsigned int i;

	for (i = n; i > 64; i -= 64)
		sha512_update(s, md, 64);
	sha512_update(s, md, i);
}

static seastar::future<char *> sha512crypt(const char *key, const char *setting, char *output)
{
	struct sha512 ctx;
	unsigned char md[64], kmd[64], smd[64];
	unsigned int i, r, klen, slen;
	char rounds[20] = "";
	const char *salt;
	char *p;

	/* reject large keys */
	for (i = 0; i <= KEY_MAX && key[i]; i++);
	if (i > KEY_MAX)
		co_return nullptr;
	klen = i;

	/* setting: $6$rounds=n$salt$ (rounds=n$ and closing $ are optional) */
	if (strncmp(setting, "$6$", 3) != 0)
		co_return nullptr;
	salt = setting + 3;

	r = ROUNDS_DEFAULT;
	if (strncmp(salt, "rounds=", sizeof "rounds=" - 1) == 0) {
		unsigned long u;
		char *end;

		/*
		 * this is a deviation from the reference:
		 * bad rounds setting is rejected if it is
		 * - empty
		 * - unterminated (missing '$')
		 * - begins with anything but a decimal digit
		 * the reference implementation treats these bad
		 * rounds as part of the salt or parse them with
		 * strtoul semantics which may cause problems
		 * including non-portable hashes that depend on
		 * the host's value of ULONG_MAX.
		 */
		salt += sizeof "rounds=" - 1;
		if (!isdigit(*salt))
			co_return nullptr;
		u = strtoul(salt, &end, 10);
		if (*end != '$')
			co_return nullptr;
		salt = end+1;
		if (u < ROUNDS_MIN)
			r = ROUNDS_MIN;
		else if (u > ROUNDS_MAX)
			co_return nullptr;
		else
			r = u;
		/* needed when rounds is zero prefixed or out of bounds */
		sprintf(rounds, "rounds=%u$", r);
	}

	for (i = 0; i < SALT_MAX && salt[i] && salt[i] != '$'; i++)
		/* reject characters that interfere with /etc/shadow parsing */
		if (salt[i] == '\n' || salt[i] == ':')
			co_return nullptr;
	slen = i;

	/* EVP_MD_CTX_free() scrubs the digest state */
	std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> mdctx(EVP_MD_CTX_new(), &EVP_MD_CTX_free);
	if (!mdctx || !sha512_md())
		co_return nullptr;
	ctx.ctx = mdctx.get();
	ctx.failed = false;

	/* B = sha(key salt key) */
	sha512_init(&ctx);
	sha512_update(&ctx, key, klen);
	sha512_update(&ctx, salt, slen);
	sha512_update(&ctx, key, klen);
	sha512_sum(&ctx, md);

	/* A = sha(key salt repeat-B alternate-B-key) */
	sha512_init(&ctx);
	sha512_update(&ctx, key, klen);
	sha512_update(&ctx, salt, slen);
	hashmd(&ctx, klen, md);
	for (i = klen; i > 0; i >>= 1)
		if (i & 1)
			sha512_update(&ctx, md, sizeof md);
		else
			sha512_update(&ctx, key, klen);
	sha512_sum(&ctx, md);

	/* DP = sha(repeat-key), this step takes O(klen^2) time */
	sha512_init(&ctx);
	for (i = 0; i < klen; i++)
		sha512_update(&ctx, key, klen);
	sha512_sum(&ctx, kmd);

	/* DS = sha(repeat-salt) */
	sha512_init(&ctx);
	for (i = 0; i < 16 + md[0]; i++)
		sha512_update(&ctx, salt, slen);
	sha512_sum(&ctx, smd);

	/* iterate A = f(A,DP,DS), this step takes O(rounds*klen) time */
	for (i = 0; i < r; i++) {
		sha512_init(&ctx);
		if (i % 2)
			hashmd(&ctx, klen, kmd);
		else
			sha512_update(&ctx, md, sizeof md);
		if (i % 3)
			sha512_update(&ctx, smd, slen);
		if (i % 7)
			hashmd(&ctx, klen, kmd);
		if (i % 2)
			sha512_update(&ctx, md, sizeof md);
		else
			hashmd(&ctx, klen, kmd);
		sha512_sum(&ctx, md);
		co_await seastar::coroutine::maybe_yield();
	}

	if (ctx.failed) {
		explicit_bzero(md, sizeof md);
		explicit_bzero(kmd, sizeof kmd);
		explicit_bzero(smd, sizeof smd);
		co_return nullptr;
	}

	/* output is $6$rounds=n$salt$hash */
	p = output;
	p += sprintf(p, "$6$%s%.*s$", rounds, slen, salt);
#if 1
	static const unsigned char perm[][3] = {
		{0,21,42},{22,43,1},{44,2,23},{3,24,45},{25,46,4},
		{47,5,26},{6,27,48},{28,49,7},{50,8,29},{9,30,51},
		{31,52,10},{53,11,32},{12,33,54},{34,55,13},{56,14,35},
		{15,36,57},{37,58,16},{59,17,38},{18,39,60},{40,61,19},
		{62,20,41} };
	for (i=0; i<21; i++) p = to64(p,
		(md[perm[i][0]]<<16)|(md[perm[i][1]]<<8)|md[perm[i][2]], 4);
#else
	p = to64(p, (md[0]<<16)|(md[21]<<8)|md[42], 4);
	p = to64(p, (md[22]<<16)|(md[43]<<8)|md[1], 4);
	p = to64(p, (md[44]<<16)|(md[2]<<8)|md[23], 4);
	p = to64(p, (md[3]<<16)|(md[24]<<8)|md[45], 4);
	p = to64(p, (md[25]<<16)|(md[46]<<8)|md[4], 4);
	p = to64(p, (md[47]<<16)|(md[5]<<8)|md[26], 4);
	p = to64(p, (md[6]<<16)|(md[27]<<8)|md[48], 4);
	p = to64(p, (md[28]<<16)|(md[49]<<8)|md[7], 4);
	p = to64(p, (md[50]<<16)|(md[8]<<8)|md[29], 4);
	p = to64(p, (md[9]<<16)|(md[30]<<8)|md[51], 4);
	p = to64(p, (md[31]<<16)|(md[52]<<8)|md[10], 4);
	p = to64(p, (md[53]<<16)|(md[11]<<8)|md[32], 4);
	p = to64(p, (md[12]<<16)|(md[33]<<8)|md[54], 4);
	p = to64(p, (md[34]<<16)|(md[55]<<8)|md[13], 4);
	p = to64(p, (md[56]<<16)|(md[14]<<8)|md[35], 4);
	p = to64(p, (md[15]<<16)|(md[36]<<8)|md[57], 4);
	p = to64(p, (md[37]<<16)|(md[58]<<8)|md[16], 4);
	p = to64(p, (md[59]<<16)|(md[17]<<8)|md[38], 4);
	p = to64(p, (md[18]<<16)|(md[39]<<8)|md[60], 4);
	p = to64(p, (md[40]<<16)|(md[61]<<8)|md[19], 4);
	p = to64(p, (md[62]<<16)|(md[20]<<8)|md[41], 4);
#endif
	p = to64(p, md[63], 2);
	*p = 0;
	/* the coroutine frame lives on the heap, scrub key-derived state from it */
	explicit_bzero(md, sizeof md);
	explicit_bzero(kmd, sizeof kmd);
	explicit_bzero(smd, sizeof smd);
	co_return output;
}

/* known-answer test result, -1 until the test completes on this thread */
static thread_local int self_test_result = -1;

static seastar::future<> self_test()
{
	static const char testkey[] = "Xy01@#\x01\x02\x80\x7f\xff\r\n\x81\t !";
	static const char testsetting[] = "$6$rounds=1234$abc0123456789$";
	static const char testhash[] = "$6$rounds=1234$abc0123456789$BCpt8zLrc/RcyuXmCDOE1ALqMXB2MH6n1g891HhFj8.w7LxGv.FTkqq6Vxc/km3Y0jE0j24jY5PIv/oOu6reg1";
	char testbuf[128];
	char *q;

	q = co_await sha512crypt(testkey, testsetting, testbuf);
	/* failing to compute the hash says nothing about its correctness, so retry on the next call */
	if (q == testbuf)
		self_test_result = !memcmp(testbuf, testhash, sizeof testhash);
}

seastar::future<const char *> __crypt_sha512(const char *key, const char *setting, char *output)
{
	char *p;

	if (self_test_result < 0)
		co_await self_test();
	if (self_test_result != 1)
		co_return "*";
	p = co_await sha512crypt(key, setting, output);
	if (!p)
		co_return "*";
	co_return p;
}
