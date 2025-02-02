/*
 * openssl.c
 *		Wrapper for OpenSSL library.
 *
 * Copyright (c) 2001 Marko Kreen
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in the
 *	  documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * contrib/pgcrypto/openssl.c
 */

#include "postgres.h"

#include "px.h"

#include <openssl/evp.h>
#include <openssl/blowfish.h>
#include <openssl/cast.h>
#include <openssl/des.h>
#include <openssl/rand.h>
#include <openssl/err.h>

#include "utils/memutils.h"
#include "utils/resowner.h"

/*
 * Max lengths we might want to handle.
 */
#define MAX_KEY		(512/8)
#define MAX_IV		(128/8)

/*
 * Compatibility with OpenSSL 0.9.6
 *
 * It needs AES and newer DES and digest API.
 */
#if OPENSSL_VERSION_NUMBER >= 0x00907000L

/*
 * Nothing needed for OpenSSL 0.9.7+
 */

#include <openssl/aes.h>
#else							/* old OPENSSL */

/*
 * Emulate OpenSSL AES.
 */

#include "rijndael.c"

#define AES_ENCRYPT 1
#define AES_DECRYPT 0
#define AES_KEY		rijndael_ctx

static int
AES_set_encrypt_key(const uint8 *key, int kbits, AES_KEY *ctx)
{
	aes_set_key(ctx, key, kbits, 1);
	return 0;
}

static int
AES_set_decrypt_key(const uint8 *key, int kbits, AES_KEY *ctx)
{
	aes_set_key(ctx, key, kbits, 0);
	return 0;
}

static void
AES_ecb_encrypt(const uint8 *src, uint8 *dst, AES_KEY *ctx, int enc)
{
	memcpy(dst, src, 16);
	if (enc)
		aes_ecb_encrypt(ctx, dst, 16);
	else
		aes_ecb_decrypt(ctx, dst, 16);
}

static void
AES_cbc_encrypt(const uint8 *src, uint8 *dst, int len, AES_KEY *ctx, uint8 *iv, int enc)
{
	memcpy(dst, src, len);
	if (enc)
	{
		aes_cbc_encrypt(ctx, iv, dst, len);
		memcpy(iv, dst + len - 16, 16);
	}
	else
	{
		aes_cbc_decrypt(ctx, iv, dst, len);
		memcpy(iv, src + len - 16, 16);
	}
}

/*
 * Emulate DES_* API
 */

#define DES_key_schedule des_key_schedule
#define DES_cblock des_cblock
#define DES_set_key(k, ks) \
		des_set_key((k), *(ks))
#define DES_ecb_encrypt(i, o, k, e) \
		des_ecb_encrypt((i), (o), *(k), (e))
#define DES_ncbc_encrypt(i, o, l, k, iv, e) \
		des_ncbc_encrypt((i), (o), (l), *(k), (iv), (e))
#define DES_ecb3_encrypt(i, o, k1, k2, k3, e) \
		des_ecb3_encrypt((des_cblock *)(i), (des_cblock *)(o), \
				*(k1), *(k2), *(k3), (e))
#define DES_ede3_cbc_encrypt(i, o, l, k1, k2, k3, iv, e) \
		des_ede3_cbc_encrypt((i), (o), \
				(l), *(k1), *(k2), *(k3), (iv), (e))

/*
 * Emulate newer digest API.
 */

static void
EVP_MD_CTX_init(EVP_MD_CTX *ctx)
{
	memset(ctx, 0, sizeof(*ctx));
}

static int
EVP_MD_CTX_cleanup(EVP_MD_CTX *ctx)
{
	px_memset(ctx, 0, sizeof(*ctx));
	return 1;
}

static int
EVP_DigestInit_ex(EVP_MD_CTX *ctx, const EVP_MD *md, void *engine)
{
	EVP_DigestInit(ctx, md);
	return 1;
}

static int
EVP_DigestFinal_ex(EVP_MD_CTX *ctx, unsigned char *res, unsigned int *len)
{
	EVP_DigestFinal(ctx, res, len);
	return 1;
}
#endif   /* old OpenSSL */

/*
 * Provide SHA2 for older OpenSSL < 0.9.8
 */
#if OPENSSL_VERSION_NUMBER < 0x00908000L

#include "sha2.c"
#include "internal-sha2.c"

typedef void (*init_f) (PX_MD *md);

static int
compat_find_digest(const char *name, PX_MD **res)
{
	init_f		init = NULL;

	if (pg_strcasecmp(name, "sha224") == 0)
		init = init_sha224;
	else if (pg_strcasecmp(name, "sha256") == 0)
		init = init_sha256;
	else if (pg_strcasecmp(name, "sha384") == 0)
		init = init_sha384;
	else if (pg_strcasecmp(name, "sha512") == 0)
		init = init_sha512;
	else
		return PXE_NO_HASH;

	*res = px_alloc(sizeof(PX_MD));
	init(*res);
	return 0;
}
#else
#define compat_find_digest(name, res)  (PXE_NO_HASH)
#endif

/*
 * Fips mode
 */
static bool fips = false;

#define NOT_FIPS_CERTIFIED \
	if (fips) \
		ereport(ERROR, \
				(errmsg("requested functionality not allowed in FIPS mode")));

/*
 * Hashes
 */

/*
 * To make sure we don't leak OpenSSL handles on abort, we keep OSSLDigest
 * objects in a linked list, allocated in TopMemoryContext. We use the
 * ResourceOwner mechanism to free them on abort.
 */
typedef struct OSSLDigest
{
	const EVP_MD *algo;
	EVP_MD_CTX *ctx;

	ResourceOwner owner;
	struct OSSLDigest *next;
	struct OSSLDigest *prev;
} OSSLDigest;

static OSSLDigest *open_digests = NULL;
static bool resowner_callback_registered = false;

static void
free_openssldigest(OSSLDigest *digest)
{
	EVP_MD_CTX_destroy(digest->ctx);
	if (digest->prev)
		digest->prev->next = digest->next;
	else
		open_digests = digest->next;
	if (digest->next)
		digest->next->prev = digest->prev;
	pfree(digest);
}

/*
 * Close any open OpenSSL handles on abort.
 */
static void
digest_free_callback(ResourceReleasePhase phase,
					 bool isCommit,
					 bool isTopLevel,
					 void *arg)
{
	OSSLDigest *curr;
	OSSLDigest *next;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	next = open_digests;
	while (next)
	{
		curr = next;
		next = curr->next;

		if (curr->owner == CurrentResourceOwner)
		{
			if (isCommit)
				elog(WARNING, "pgcrypto digest reference leak: digest %p still referenced", curr);
			free_openssldigest(curr);
		}
	}
}

static unsigned
digest_result_size(PX_MD *h)
{
	OSSLDigest *digest = (OSSLDigest *) h->p.ptr;

	return EVP_MD_CTX_size(digest->ctx);
}

static unsigned
digest_block_size(PX_MD *h)
{
	OSSLDigest *digest = (OSSLDigest *) h->p.ptr;

	return EVP_MD_CTX_block_size(digest->ctx);
}

static void
digest_reset(PX_MD *h)
{
	OSSLDigest *digest = (OSSLDigest *) h->p.ptr;

	EVP_DigestInit_ex(digest->ctx, digest->algo, NULL);
}

static void
digest_update(PX_MD *h, const uint8 *data, unsigned dlen)
{
	OSSLDigest *digest = (OSSLDigest *) h->p.ptr;

	EVP_DigestUpdate(digest->ctx, data, dlen);
}

static void
digest_finish(PX_MD *h, uint8 *dst)
{
	OSSLDigest *digest = (OSSLDigest *) h->p.ptr;

	EVP_DigestFinal_ex(digest->ctx, dst, NULL);
}

static void
digest_free(PX_MD *h)
{
	OSSLDigest *digest = (OSSLDigest *) h->p.ptr;

	free_openssldigest(digest);
	px_free(h);
}

static int	px_openssl_initialized = 0;

/* PUBLIC functions */

int
px_find_digest(const char *name, PX_MD **res)
{
	const EVP_MD *md;
	EVP_MD_CTX *ctx;
	PX_MD	   *h;
	OSSLDigest *digest;

	if (!px_openssl_initialized)
	{
		px_openssl_initialized = 1;
		OpenSSL_add_all_algorithms();
	}

	if (!resowner_callback_registered)
	{
		RegisterResourceReleaseCallback(digest_free_callback, NULL);
		resowner_callback_registered = true;
	}

	md = EVP_get_digestbyname(name);
	if (md == NULL)
		return compat_find_digest(name, res);

	/*
	 * Create an OSSLDigest object, an OpenSSL MD object, and a PX_MD object.
	 * The order is crucial, to make sure we don't leak anything on
	 * out-of-memory or other error.
	 */
	digest = MemoryContextAlloc(TopMemoryContext, sizeof(*digest));

	ctx = EVP_MD_CTX_create();
	if (!ctx)
	{
		pfree(digest);
		return -1;
	}
	if (EVP_DigestInit_ex(ctx, md, NULL) == 0)
	{
		pfree(digest);
		return -1;
	}

	digest->algo = md;
	digest->ctx = ctx;
	digest->owner = CurrentResourceOwner;
	digest->next = open_digests;
	digest->prev = NULL;
	open_digests = digest;

	/* The PX_MD object is allocated in the current memory context. */
	h = px_alloc(sizeof(*h));
	h->result_size = digest_result_size;
	h->block_size = digest_block_size;
	h->reset = digest_reset;
	h->update = digest_update;
	h->finish = digest_finish;
	h->free = digest_free;
	h->p.ptr = (void *) digest;

	*res = h;
	return 0;
}

/*
 * Ciphers
 *
 * The problem with OpenSSL is that the EVP* family
 * of functions does not allow enough flexibility
 * and forces some of the parameters (keylen,
 * padding) to SSL defaults.
 *
 * So need to manage ciphers ourselves.
 */

struct ossl_cipher
{
	int			(*init) (PX_Cipher *c, const uint8 *key, unsigned klen, const uint8 *iv);
	int			(*encrypt) (PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res);
	int			(*decrypt) (PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res);

	int			block_size;
	int			max_key_size;
	int			stream_cipher;
};

typedef struct
{
	union
	{
		struct
		{
			BF_KEY		key;
			int			num;
		}			bf;
		struct
		{
			DES_key_schedule key_schedule;
		}			des;
		struct
		{
			DES_key_schedule k1,
						k2,
						k3;
		}			des3;
		CAST_KEY	cast_key;
		AES_KEY		aes_key;
	}			u;
	uint8		key[MAX_KEY];
	uint8		iv[MAX_IV];
	unsigned	klen;
	unsigned	init;
	const struct ossl_cipher *ciph;
} ossldata;

/* generic */

static unsigned
gen_ossl_block_size(PX_Cipher *c)
{
	ossldata   *od = (ossldata *) c->ptr;

	return od->ciph->block_size;
}

static unsigned
gen_ossl_key_size(PX_Cipher *c)
{
	ossldata   *od = (ossldata *) c->ptr;

	return od->ciph->max_key_size;
}

static unsigned
gen_ossl_iv_size(PX_Cipher *c)
{
	unsigned	ivlen;
	ossldata   *od = (ossldata *) c->ptr;

	ivlen = od->ciph->block_size;
	return ivlen;
}

static void
gen_ossl_free(PX_Cipher *c)
{
	ossldata   *od = (ossldata *) c->ptr;

	px_memset(od, 0, sizeof(*od));
	px_free(od);
	px_free(c);
}

/* Blowfish */

/*
 * Check if strong crypto is supported. Some openssl installations
 * support only short keys and unfortunately BF_set_key does not return any
 * error value. This function tests if is possible to use strong key.
 */
static int
bf_check_supported_key_len(void)
{
	static const uint8 key[56] = {
		0xf0, 0xe1, 0xd2, 0xc3, 0xb4, 0xa5, 0x96, 0x87, 0x78, 0x69,
		0x5a, 0x4b, 0x3c, 0x2d, 0x1e, 0x0f, 0x00, 0x11, 0x22, 0x33,
		0x44, 0x55, 0x66, 0x77, 0x04, 0x68, 0x91, 0x04, 0xc2, 0xfd,
		0x3b, 0x2f, 0x58, 0x40, 0x23, 0x64, 0x1a, 0xba, 0x61, 0x76,
		0x1f, 0x1f, 0x1f, 0x1f, 0x0e, 0x0e, 0x0e, 0x0e, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff
	};

	static const uint8 data[8] = {0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10};
	static const uint8 res[8] = {0xc0, 0x45, 0x04, 0x01, 0x2e, 0x4e, 0x1f, 0x53};
	static uint8 out[8];

	BF_KEY		bf_key;

	/* encrypt with 448bits key and verify output */
	BF_set_key(&bf_key, 56, key);
	BF_ecb_encrypt(data, out, &bf_key, BF_ENCRYPT);

	if (memcmp(out, res, 8) != 0)
		return 0;				/* Output does not match -> strong cipher is
								 * not supported */
	return 1;
}

static int
bf_init(PX_Cipher *c, const uint8 *key, unsigned klen, const uint8 *iv)
{
	ossldata   *od = c->ptr;
	static int	bf_is_strong = -1;

	/*
	 * Test if key len is supported. BF_set_key silently cut large keys and it
	 * could be a problem when user transfer crypted data from one server to
	 * another.
	 */

	if (bf_is_strong == -1)
		bf_is_strong = bf_check_supported_key_len();

	if (!bf_is_strong && klen > 16)
		return PXE_KEY_TOO_BIG;

	/* Key len is supported. We can use it. */
	BF_set_key(&od->u.bf.key, klen, key);
	if (iv)
		memcpy(od->iv, iv, BF_BLOCK);
	else
		memset(od->iv, 0, BF_BLOCK);
	od->u.bf.num = 0;
	return 0;
}

static int
bf_ecb_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	unsigned	i;
	ossldata   *od = c->ptr;

	for (i = 0; i < dlen / bs; i++)
		BF_ecb_encrypt(data + i * bs, res + i * bs, &od->u.bf.key, BF_ENCRYPT);
	return 0;
}

static int
bf_ecb_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c),
				i;
	ossldata   *od = c->ptr;

	for (i = 0; i < dlen / bs; i++)
		BF_ecb_encrypt(data + i * bs, res + i * bs, &od->u.bf.key, BF_DECRYPT);
	return 0;
}

static int
bf_cbc_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	ossldata   *od = c->ptr;

	BF_cbc_encrypt(data, res, dlen, &od->u.bf.key, od->iv, BF_ENCRYPT);
	return 0;
}

static int
bf_cbc_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	ossldata   *od = c->ptr;

	BF_cbc_encrypt(data, res, dlen, &od->u.bf.key, od->iv, BF_DECRYPT);
	return 0;
}

static int
bf_cfb64_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	ossldata   *od = c->ptr;

	BF_cfb64_encrypt(data, res, dlen, &od->u.bf.key, od->iv,
					 &od->u.bf.num, BF_ENCRYPT);
	return 0;
}

static int
bf_cfb64_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	ossldata   *od = c->ptr;

	BF_cfb64_encrypt(data, res, dlen, &od->u.bf.key, od->iv,
					 &od->u.bf.num, BF_DECRYPT);
	return 0;
}

/* DES */

static int
ossl_des_init(PX_Cipher *c, const uint8 *key, unsigned klen, const uint8 *iv)
{
	ossldata   *od = c->ptr;
	DES_cblock	xkey;

	memset(&xkey, 0, sizeof(xkey));
	memcpy(&xkey, key, klen > 8 ? 8 : klen);
	DES_set_key(&xkey, &od->u.des.key_schedule);
	memset(&xkey, 0, sizeof(xkey));

	if (iv)
		memcpy(od->iv, iv, 8);
	else
		memset(od->iv, 0, 8);
	return 0;
}

static int
ossl_des_ecb_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	unsigned	i;
	ossldata   *od = c->ptr;

	for (i = 0; i < dlen / bs; i++)
		DES_ecb_encrypt((DES_cblock *) (data + i * bs),
						(DES_cblock *) (res + i * bs),
						&od->u.des.key_schedule, 1);
	return 0;
}

static int
ossl_des_ecb_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	unsigned	i;
	ossldata   *od = c->ptr;

	for (i = 0; i < dlen / bs; i++)
		DES_ecb_encrypt((DES_cblock *) (data + i * bs),
						(DES_cblock *) (res + i * bs),
						&od->u.des.key_schedule, 0);
	return 0;
}

static int
ossl_des_cbc_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	ossldata   *od = c->ptr;

	DES_ncbc_encrypt(data, res, dlen, &od->u.des.key_schedule,
					 (DES_cblock *) od->iv, 1);
	return 0;
}

static int
ossl_des_cbc_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	ossldata   *od = c->ptr;

	DES_ncbc_encrypt(data, res, dlen, &od->u.des.key_schedule,
					 (DES_cblock *) od->iv, 0);
	return 0;
}

/* DES3 */

static int
ossl_des3_init(PX_Cipher *c, const uint8 *key, unsigned klen, const uint8 *iv)
{
	ossldata   *od = c->ptr;
	DES_cblock	xkey1,
				xkey2,
				xkey3;

	memset(&xkey1, 0, sizeof(xkey1));
	memset(&xkey2, 0, sizeof(xkey2));
	memset(&xkey3, 0, sizeof(xkey3));
	memcpy(&xkey1, key, klen > 8 ? 8 : klen);
	if (klen > 8)
		memcpy(&xkey2, key + 8, (klen - 8) > 8 ? 8 : (klen - 8));
	if (klen > 16)
		memcpy(&xkey3, key + 16, (klen - 16) > 8 ? 8 : (klen - 16));

	DES_set_key(&xkey1, &od->u.des3.k1);
	DES_set_key(&xkey2, &od->u.des3.k2);
	DES_set_key(&xkey3, &od->u.des3.k3);
	memset(&xkey1, 0, sizeof(xkey1));
	memset(&xkey2, 0, sizeof(xkey2));
	memset(&xkey3, 0, sizeof(xkey3));

	if (iv)
		memcpy(od->iv, iv, 8);
	else
		memset(od->iv, 0, 8);
	return 0;
}

static int
ossl_des3_ecb_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					  uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	unsigned	i;
	ossldata   *od = c->ptr;

	for (i = 0; i < dlen / bs; i++)
		DES_ecb3_encrypt((void *) (data + i * bs), (void *) (res + i * bs),
						 &od->u.des3.k1, &od->u.des3.k2, &od->u.des3.k3, 1);
	return 0;
}

static int
ossl_des3_ecb_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					  uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	unsigned	i;
	ossldata   *od = c->ptr;

	for (i = 0; i < dlen / bs; i++)
		DES_ecb3_encrypt((void *) (data + i * bs), (void *) (res + i * bs),
						 &od->u.des3.k1, &od->u.des3.k2, &od->u.des3.k3, 0);
	return 0;
}

static int
ossl_des3_cbc_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					  uint8 *res)
{
	ossldata   *od = c->ptr;

	DES_ede3_cbc_encrypt(data, res, dlen,
						 &od->u.des3.k1, &od->u.des3.k2, &od->u.des3.k3,
						 (DES_cblock *) od->iv, 1);
	return 0;
}

static int
ossl_des3_cbc_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					  uint8 *res)
{
	ossldata   *od = c->ptr;

	DES_ede3_cbc_encrypt(data, res, dlen,
						 &od->u.des3.k1, &od->u.des3.k2, &od->u.des3.k3,
						 (DES_cblock *) od->iv, 0);
	return 0;
}

/* CAST5 */

static int
ossl_cast_init(PX_Cipher *c, const uint8 *key, unsigned klen, const uint8 *iv)
{
	ossldata   *od = c->ptr;
	unsigned	bs = gen_ossl_block_size(c);

	CAST_set_key(&od->u.cast_key, klen, key);
	if (iv)
		memcpy(od->iv, iv, bs);
	else
		memset(od->iv, 0, bs);
	return 0;
}

static int
ossl_cast_ecb_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	ossldata   *od = c->ptr;
	const uint8 *end = data + dlen - bs;

	for (; data <= end; data += bs, res += bs)
		CAST_ecb_encrypt(data, res, &od->u.cast_key, CAST_ENCRYPT);
	return 0;
}

static int
ossl_cast_ecb_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	ossldata   *od = c->ptr;
	const uint8 *end = data + dlen - bs;

	for (; data <= end; data += bs, res += bs)
		CAST_ecb_encrypt(data, res, &od->u.cast_key, CAST_DECRYPT);
	return 0;
}

static int
ossl_cast_cbc_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	ossldata   *od = c->ptr;

	CAST_cbc_encrypt(data, res, dlen, &od->u.cast_key, od->iv, CAST_ENCRYPT);
	return 0;
}

static int
ossl_cast_cbc_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	ossldata   *od = c->ptr;

	CAST_cbc_encrypt(data, res, dlen, &od->u.cast_key, od->iv, CAST_DECRYPT);
	return 0;
}

/* AES */

static int
ossl_aes_init(PX_Cipher *c, const uint8 *key, unsigned klen, const uint8 *iv)
{
	ossldata   *od = c->ptr;
	unsigned	bs = gen_ossl_block_size(c);

	if (klen <= 128 / 8)
		od->klen = 128 / 8;
	else if (klen <= 192 / 8)
		od->klen = 192 / 8;
	else if (klen <= 256 / 8)
		od->klen = 256 / 8;
	else
		return PXE_KEY_TOO_BIG;

	memcpy(od->key, key, klen);

	if (iv)
		memcpy(od->iv, iv, bs);
	else
		memset(od->iv, 0, bs);
	return 0;
}

static int
ossl_aes_key_init(ossldata *od, int type)
{
	int			err;

	/*
	 * Strong key support could be missing on some openssl installations. We
	 * must check return value from set key function.
	 */
	if (type == AES_ENCRYPT)
		err = AES_set_encrypt_key(od->key, od->klen * 8, &od->u.aes_key);
	else
		err = AES_set_decrypt_key(od->key, od->klen * 8, &od->u.aes_key);

	if (err == 0)
	{
		od->init = 1;
		return 0;
	}
	od->init = 0;
	return PXE_KEY_TOO_BIG;
}

static int
ossl_aes_ecb_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	ossldata   *od = c->ptr;
	const uint8 *end = data + dlen - bs;
	int			err;

	if (!od->init)
		if ((err = ossl_aes_key_init(od, AES_ENCRYPT)) != 0)
			return err;

	for (; data <= end; data += bs, res += bs)
		AES_ecb_encrypt(data, res, &od->u.aes_key, AES_ENCRYPT);
	return 0;
}

static int
ossl_aes_ecb_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	unsigned	bs = gen_ossl_block_size(c);
	ossldata   *od = c->ptr;
	const uint8 *end = data + dlen - bs;
	int			err;

	if (!od->init)
		if ((err = ossl_aes_key_init(od, AES_DECRYPT)) != 0)
			return err;

	for (; data <= end; data += bs, res += bs)
		AES_ecb_encrypt(data, res, &od->u.aes_key, AES_DECRYPT);
	return 0;
}

static int
ossl_aes_cbc_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	ossldata   *od = c->ptr;
	int			err;

	if (!od->init)
		if ((err = ossl_aes_key_init(od, AES_ENCRYPT)) != 0)
			return err;

	AES_cbc_encrypt(data, res, dlen, &od->u.aes_key, od->iv, AES_ENCRYPT);
	return 0;
}

static int
ossl_aes_cbc_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen,
					 uint8 *res)
{
	ossldata   *od = c->ptr;
	int			err;

	if (!od->init)
		if ((err = ossl_aes_key_init(od, AES_DECRYPT)) != 0)
			return err;

	AES_cbc_encrypt(data, res, dlen, &od->u.aes_key, od->iv, AES_DECRYPT);
	return 0;
}

/*
 * aliases
 */

static PX_Alias ossl_aliases_all[] = {
	{"bf", "bf-cbc"},
	{"blowfish", "bf-cbc"},
	{"blowfish-cbc", "bf-cbc"},
	{"blowfish-ecb", "bf-ecb"},
	{"blowfish-cfb", "bf-cfb"},
	{"des", "des-cbc"},
	{"3des", "des3-cbc"},
	{"3des-ecb", "des3-ecb"},
	{"3des-cbc", "des3-cbc"},
	{"cast5", "cast5-cbc"},
	{"aes", "aes-cbc"},
	{"rijndael", "aes-cbc"},
	{"rijndael-cbc", "aes-cbc"},
	{"rijndael-ecb", "aes-ecb"},
	{NULL}
};

static PX_Alias *ossl_aliases = ossl_aliases_all;

static const struct ossl_cipher ossl_bf_cbc = {
	bf_init, bf_cbc_encrypt, bf_cbc_decrypt,
	64 / 8, 448 / 8, 0
};

static const struct ossl_cipher ossl_bf_ecb = {
	bf_init, bf_ecb_encrypt, bf_ecb_decrypt,
	64 / 8, 448 / 8, 0
};

static const struct ossl_cipher ossl_bf_cfb = {
	bf_init, bf_cfb64_encrypt, bf_cfb64_decrypt,
	64 / 8, 448 / 8, 1
};

static const struct ossl_cipher ossl_des_ecb = {
	ossl_des_init, ossl_des_ecb_encrypt, ossl_des_ecb_decrypt,
	64 / 8, 64 / 8, 0
};

static const struct ossl_cipher ossl_des_cbc = {
	ossl_des_init, ossl_des_cbc_encrypt, ossl_des_cbc_decrypt,
	64 / 8, 64 / 8, 0
};

static const struct ossl_cipher ossl_des3_ecb = {
	ossl_des3_init, ossl_des3_ecb_encrypt, ossl_des3_ecb_decrypt,
	64 / 8, 192 / 8, 0
};

static const struct ossl_cipher ossl_des3_cbc = {
	ossl_des3_init, ossl_des3_cbc_encrypt, ossl_des3_cbc_decrypt,
	64 / 8, 192 / 8, 0
};

static const struct ossl_cipher ossl_cast_ecb = {
	ossl_cast_init, ossl_cast_ecb_encrypt, ossl_cast_ecb_decrypt,
	64 / 8, 128 / 8, 0
};

static const struct ossl_cipher ossl_cast_cbc = {
	ossl_cast_init, ossl_cast_cbc_encrypt, ossl_cast_cbc_decrypt,
	64 / 8, 128 / 8, 0
};

static const struct ossl_cipher ossl_aes_ecb = {
	ossl_aes_init, ossl_aes_ecb_encrypt, ossl_aes_ecb_decrypt,
	128 / 8, 256 / 8, 0
};

static const struct ossl_cipher ossl_aes_cbc = {
	ossl_aes_init, ossl_aes_cbc_encrypt, ossl_aes_cbc_decrypt,
	128 / 8, 256 / 8, 0
};

/*
 * Special handlers
 */
struct ossl_cipher_lookup
{
	const char *name;
	const struct ossl_cipher *ciph;
};

static const struct ossl_cipher_lookup ossl_cipher_types_all[] = {
	{"bf-cbc", &ossl_bf_cbc},
	{"bf-ecb", &ossl_bf_ecb},
	{"bf-cfb", &ossl_bf_cfb},
	{"des-ecb", &ossl_des_ecb},
	{"des-cbc", &ossl_des_cbc},
	{"des3-ecb", &ossl_des3_ecb},
	{"des3-cbc", &ossl_des3_cbc},
	{"cast5-ecb", &ossl_cast_ecb},
	{"cast5-cbc", &ossl_cast_cbc},
	{"aes-ecb", &ossl_aes_ecb},
	{"aes-cbc", &ossl_aes_cbc},
	{NULL}
};

static const struct ossl_cipher_lookup *ossl_cipher_types = ossl_cipher_types_all;

/* PUBLIC functions */

int
px_find_cipher(const char *name, PX_Cipher **res)
{
	const struct ossl_cipher_lookup *i;
	PX_Cipher  *c = NULL;
	ossldata   *od;

	NOT_FIPS_CERTIFIED

	name = px_resolve_alias(ossl_aliases, name);
	for (i = ossl_cipher_types; i->name; i++)
		if (strcmp(i->name, name) == 0)
			break;
	if (i->name == NULL)
		return PXE_NO_CIPHER;

	od = px_alloc(sizeof(*od));
	memset(od, 0, sizeof(*od));
	od->ciph = i->ciph;

	c = px_alloc(sizeof(*c));
	c->block_size = gen_ossl_block_size;
	c->key_size = gen_ossl_key_size;
	c->iv_size = gen_ossl_iv_size;
	c->free = gen_ossl_free;
	c->init = od->ciph->init;
	c->encrypt = od->ciph->encrypt;
	c->decrypt = od->ciph->decrypt;
	c->ptr = od;

	*res = c;
	return 0;
}

void
px_disable_fipsmode(void)
{
#ifndef OPENSSL_FIPS
	/*
	 * If this build doesn't support FIPS mode at all, we shouldn't be able
	 * to reach this point, so Assert that and return to handle production
	 * builds gracefully.
	 */
	Assert(!fips);
#else
	ossl_aliases = ossl_aliases_all;
	ossl_cipher_types = ossl_cipher_types_all;
	fips = false;

	if (!FIPS_mode_set)
		return;

	FIPS_mode_set(0);
#endif

	return;
}

void
px_enable_fipsmode(void)
{
#ifndef OPENSSL_FIPS
	ereport(ERROR,
			(errmsg("FIPS enabled OpenSSL is required for strict FIPS mode"),
			 errhint("Recompile OpenSSL with the FIPS module, or install a FIPS enabled OpenSSL distribution.")));
#else

	/*
	 * While AES and 3DES are allowed ciphers under FIPS-140 level 2, pgcrypto
	 * is calling the lowlevel API for these which is disallowed under FIPS.
	 * However, rather than returning NULL as is done when calling the high
	 * level functions, the lowlevel API throws a SIGABORT so we need to avoid
	 * calling this altogether.
	 */
	ossl_aliases = NULL;
	ossl_cipher_types = NULL;

	/* Make sure that we are linked against a FIPS enabled OpenSSL */
	if (!FIPS_mode_set)
	{
		ereport(ERROR,
				(errmsg("FIPS enabled OpenSSL is required for strict FIPS mode"),
				 errhint("Recompile OpenSSL with the FIPS module, or install a FIPS enabled OpenSSL distribution.")));
	}

	/*
	 * A non-zero return value means that FIPS mode was enabled, but the
	 * full range of possible non-zero return values is not documented so
	 * rather than checking for success we check for failure.
	 */
	if (FIPS_mode_set(1) == 0)
	{
		char		errbuf[128];

		ERR_load_crypto_strings();
		ERR_error_string_n(ERR_get_error(), errbuf, sizeof(errbuf));
		ERR_free_strings();

		ereport(ERROR,
				(errmsg("unable to enable FIPS mode: %lx, %s",
						ERR_get_error(), errbuf)));
	}

	fips = true;
#endif
}

void
px_check_fipsmode(void)
{
#ifndef OPENSSL_FIPS
	ereport(ERROR,
			(errmsg("FIPS enabled OpenSSL is required for strict FIPS mode"),
			 errhint("Recompile OpenSSL with the FIPS module, or install a FIPS enabled OpenSSL distribution.")));
#else

	/* Make sure that we are linked against a FIPS enabled OpenSSL */
	if (!FIPS_mode_set)
	{
		ereport(ERROR,
				(errmsg("FIPS enabled OpenSSL is required for strict FIPS mode"),
				 errhint("Recompile OpenSSL with the FIPS module, or install a FIPS enabled OpenSSL distribution.")));
	}

#endif
}

