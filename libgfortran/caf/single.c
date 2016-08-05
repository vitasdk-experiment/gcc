/* Single-image implementation of GNU Fortran Coarray Library
   Copyright (C) 2011-2016 Free Software Foundation, Inc.
   Contributed by Tobias Burnus <burnus@net-b.de>

This file is part of the GNU Fortran Coarray Runtime Library (libcaf).

Libcaf is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

Libcaf is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

Under Section 7 of GPL version 3, you are granted additional
permissions described in the GCC Runtime Library Exception, version
3.1, as published by the Free Software Foundation.

You should have received a copy of the GNU General Public License and
a copy of the GCC Runtime Library Exception along with this program;
see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
<http://www.gnu.org/licenses/>.  */

#include "libcaf.h"
#include <stdio.h>  /* For fputs and fprintf.  */
#include <stdlib.h> /* For exit and malloc.  */
#include <string.h> /* For memcpy and memset.  */
#include <stdarg.h> /* For variadic arguments.  */
#include <assert.h>

/* Define GFC_CAF_CHECK to enable run-time checking.  */
/* #define GFC_CAF_CHECK  1  */

struct caf_single_token
{
  /* The pointer to the memory registered.  For arrays this is the data member
     in the descriptor.  For components it's the pure data pointer.  */
  void *memptr;
  /* When this token describes an array, then desc is the array descriptor. For
     all other coarrays, desc is NULL.
     Note, the array descriptor is only set for non-top-level arrays.
     TODO: Set the descriptor also for top-level arrays.  */
  gfc_descriptor_t *desc;
  /* The number of allocatable/pointer components in this derived type.
     This is the number of entries of the components c-array below.  */
  int num_comps;
  /* The tokens of the allocatable components of this derived type.
     If this token does not represent a derived type, then NULL.
     When a component c is not registered yet, then component[c] ==  NULL.  */
  struct caf_single_token **components;
  /* Set when the caf lib has allocated the memory in memptr and is responsible
     for freeing it on deregister. */
  bool owning_memory;
};
typedef struct caf_single_token *caf_single_token_t;

#define TOKEN(X) ((caf_single_token_t) (X))
#define MEMTOK(X) ((caf_single_token_t) (X))->memptr

/* Single-image implementation of the CAF library.
   Note: For performance reasons -fcoarry=single should be used
   rather than this library.  */

/* Global variables.  */
caf_static_t *caf_static_list = NULL;

/* Keep in sync with mpi.c.  */
static void
caf_runtime_error (const char *message, ...)
{
  va_list ap;
  fprintf (stderr, "Fortran runtime error: ");
  va_start (ap, message);
  vfprintf (stderr, message, ap);
  va_end (ap);
  fprintf (stderr, "\n");

  /* FIXME: Shutdown the Fortran RTL to flush the buffer.  PR 43849.  */
  exit (EXIT_FAILURE);
}

/* Error handling is similar everytime.  */
static void
caf_internal_error (const char *msg, size_t msg_len, int *stat, char *errmsg,
		    int errmsg_len)
{
  if (stat)
  {
    *stat = 1;
    if (errmsg_len > 0)
    {
      int len = ((int) msg_len > errmsg_len) ? errmsg_len
						  : (int) msg_len;
      memcpy (errmsg, msg, len);
      if (errmsg_len > len)
	memset (&errmsg[len], ' ', errmsg_len-len);
    }
    return;
  }
  else
    caf_runtime_error (msg);
}


void
_gfortran_caf_init (int *argc __attribute__ ((unused)),
		    char ***argv __attribute__ ((unused)))
{
}


void
_gfortran_caf_finalize (void)
{
  while (caf_static_list != NULL)
    {
      caf_static_t *tmp = caf_static_list->prev;
      free (caf_static_list->token);
      free (caf_static_list);
      caf_static_list = tmp;
    }
}


int
_gfortran_caf_this_image (int distance __attribute__ ((unused)))
{
  return 1;
}


int
_gfortran_caf_num_images (int distance __attribute__ ((unused)),
			  int failed __attribute__ ((unused)))
{
  return 1;
}


/* This error message is used by both caf_register and caf_register_component.
 */
const char alloc_fail_msg[] = "Failed to allocate coarray";


void *
_gfortran_caf_register (size_t size, caf_register_t type, caf_token_t *token,
			int *stat, char *errmsg, int errmsg_len,
			int num_alloc_comps)
{
  void *local;
  caf_single_token_t single_token;

  if (type == CAF_REGTYPE_LOCK_STATIC || type == CAF_REGTYPE_LOCK_ALLOC
      || type == CAF_REGTYPE_CRITICAL || type == CAF_REGTYPE_EVENT_STATIC
      || type == CAF_REGTYPE_EVENT_ALLOC)
    local = calloc (size, sizeof (bool));
  else
    local = malloc (size);
  *token = malloc (sizeof (struct caf_single_token));

  if (unlikely (local == NULL || *token == NULL))
    {
      caf_internal_error (alloc_fail_msg, sizeof (alloc_fail_msg), stat, errmsg,
			  errmsg_len);
      return NULL;
    }

  single_token = TOKEN(*token);
  single_token->memptr = local;
  single_token->desc = NULL;
  single_token->owning_memory = true;
  single_token->num_comps = num_alloc_comps;
  if (num_alloc_comps > 0)
    {
      single_token->components = (caf_single_token_t *) calloc (num_alloc_comps,
						   sizeof (caf_single_token_t));
      if (unlikely (single_token->components == NULL))
	{
	  caf_internal_error (alloc_fail_msg, sizeof (alloc_fail_msg), stat,
			      errmsg, errmsg_len);
	  free (local);
	  free (*token);
	  return NULL;
	}
    }
  else
    single_token->components = NULL;

  if (stat)
    *stat = 0;

  if (type == CAF_REGTYPE_COARRAY_STATIC || type == CAF_REGTYPE_LOCK_STATIC
      || type == CAF_REGTYPE_CRITICAL || type == CAF_REGTYPE_EVENT_STATIC
      || type == CAF_REGTYPE_EVENT_ALLOC)
    {
      caf_static_t *tmp = malloc (sizeof (caf_static_t));
      tmp->prev  = caf_static_list;
      tmp->token = *token;
      caf_static_list = tmp;
    }
  return local;
}


void
_gfortran_caf_register_component (caf_token_t token, caf_register_t type,
				  size_t size, int comp_idx,
				  gfc_descriptor_t *descriptor,
				  int *stat, char *errmsg, int errmsg_len,
				  int num_comp)
{
  caf_single_token_t single_token = TOKEN(token);
  void *component = GFC_DESCRIPTOR_DATA (descriptor);

  if (unlikely (single_token->num_comps < comp_idx))
    {
      const char msg[] = "Failed to register component (component_id out of "
			 "range)";
      caf_internal_error (msg, sizeof (msg), stat, errmsg, errmsg_len);
      return;
    }

  single_token->components[comp_idx] = (caf_single_token_t) calloc (1,
					      sizeof (struct caf_single_token));
  if (unlikely (single_token->components[comp_idx] == NULL))
    {
      caf_internal_error (alloc_fail_msg, sizeof (alloc_fail_msg), stat, errmsg,
			  errmsg_len);
     return;
    }

  if (component == NULL)
    {
      single_token->components[comp_idx]->owning_memory = true;

      if (type == CAF_REGTYPE_LOCK_STATIC || type == CAF_REGTYPE_LOCK_ALLOC
	  || type == CAF_REGTYPE_CRITICAL || type == CAF_REGTYPE_EVENT_STATIC
	  || type == CAF_REGTYPE_EVENT_ALLOC)
	component = calloc (size, sizeof (bool));
      else
	component = malloc (size);

      if (unlikely (component == NULL))
	{
	  caf_internal_error (alloc_fail_msg, sizeof (alloc_fail_msg), stat,
			      errmsg, errmsg_len);
	  /* Roll back to prevent memory loss.  */
	  free (single_token->components[comp_idx]);
	  single_token->components[comp_idx] = NULL;
	  return;
	}
      GFC_DESCRIPTOR_DATA (descriptor) = component;
    }

  single_token->components[comp_idx]->memptr = component;
  single_token->components[comp_idx]->desc = descriptor;
  if (num_comp)
    {
      single_token->components[comp_idx]->components = (caf_single_token_t *)
				calloc (num_comp, sizeof (caf_single_token_t));
      if (unlikely (single_token->components[comp_idx]->components == NULL))
	{
	  caf_internal_error (alloc_fail_msg, sizeof (alloc_fail_msg), stat,
			      errmsg, errmsg_len);
	  /* Roll back to prevent memory loss.  */
	  if (single_token->components[comp_idx]->owning_memory)
	    {
	      free (single_token->components[comp_idx]->memptr);
	      component = NULL;
	    }
	  free (single_token->components[comp_idx]);
	  single_token->components[comp_idx] = NULL;
	  return;
	}
      single_token->components[comp_idx]->num_comps = num_comp;
    }

  if (stat)
    *stat = 0;
}


void
_gfortran_caf_deregister (caf_token_t *token, int *stat, char *errmsg,
			  int errmsg_len)
{
  caf_single_token_t single_token = TOKEN(*token);
  /* Free components.  */
  for (int i= 0; i< single_token->num_comps; ++i)
    if (single_token->components[i] != NULL)
      {
	_gfortran_caf_deregister_component (single_token, i,
					  &single_token->components[i]->memptr,
					    NULL, errmsg, errmsg_len);
      }

  /* Free the array of the components.  */
  if (single_token->components != NULL)
    free (single_token->components);

  free (TOKEN(*token));

  if (stat)
    *stat = 0;
}


void
_gfortran_caf_deregister_component (caf_token_t token, int comp_num,
				    void **component, int *stat,
				    char *errmsg, int errmsg_len)
{
  caf_single_token_t single_token = TOKEN(token);
  if (unlikely (single_token->num_comps < comp_num))
    {
      const char msg[] = "Failed to free component (component_id out of range)";
      caf_internal_error (msg, sizeof (msg), stat, errmsg, errmsg_len);
      return;
    }

  if (single_token->components[comp_num])
    {
      if (single_token->components[comp_num]->components != NULL)
	{
	  int i;
	  for (i = 0; i < single_token->components[comp_num]->num_comps; ++i)
	    if (single_token->components[comp_num]->components[i])
	      _gfortran_caf_deregister_component (
		    single_token->components[comp_num], i,
		    &single_token->components[comp_num]->components[i]->memptr,
		    stat, errmsg, errmsg_len);
	}
      if (single_token->components[comp_num]->owning_memory)
	{
	  /* Have to free the components memory, because we allocated it. */
	  free (single_token->components[comp_num]->memptr);
	  *component = NULL;
	}

      /* Now free our record-keeping structure. */
      free (single_token->components[comp_num]);
    }
  single_token->components[comp_num] = NULL;

  if (stat)
    *stat = 0;
}


void
_gfortran_caf_sync_all (int *stat,
			char *errmsg __attribute__ ((unused)),
			int errmsg_len __attribute__ ((unused)))
{
  __asm__ __volatile__ ("":::"memory");
  if (stat)
    *stat = 0;
}


void
_gfortran_caf_sync_memory (int *stat,
			   char *errmsg __attribute__ ((unused)),
			   int errmsg_len __attribute__ ((unused)))
{
  __asm__ __volatile__ ("":::"memory");
  if (stat)
    *stat = 0;
}


void
_gfortran_caf_sync_images (int count __attribute__ ((unused)),
			   int images[] __attribute__ ((unused)),
			   int *stat,
			   char *errmsg __attribute__ ((unused)),
			   int errmsg_len __attribute__ ((unused)))
{
#ifdef GFC_CAF_CHECK
  int i;

  for (i = 0; i < count; i++)
    if (images[i] != 1)
      {
	fprintf (stderr, "COARRAY ERROR: Invalid image index %d to SYNC "
		 "IMAGES", images[i]);
	exit (EXIT_FAILURE);
      }
#endif

  __asm__ __volatile__ ("":::"memory");
  if (stat)
    *stat = 0;
}

void
_gfortran_caf_stop_numeric(int32_t stop_code)
{
  fprintf (stderr, "STOP %d\n", stop_code);
  exit (0);
}

void
_gfortran_caf_stop_str(const char *string, int32_t len)
{
  fputs ("STOP ", stderr);
  while (len--)
    fputc (*(string++), stderr);
  fputs ("\n", stderr);

  exit (0);
}

void
_gfortran_caf_error_stop_str (const char *string, int32_t len)
{
  fputs ("ERROR STOP ", stderr);
  while (len--)
    fputc (*(string++), stderr);
  fputs ("\n", stderr);

  exit (1);
}


void
_gfortran_caf_error_stop (int32_t error)
{
  fprintf (stderr, "ERROR STOP %d\n", error);
  exit (error);
}


void
_gfortran_caf_co_broadcast (gfc_descriptor_t *a __attribute__ ((unused)),
			    int source_image __attribute__ ((unused)),
			    int *stat, char *errmsg __attribute__ ((unused)),
			    int errmsg_len __attribute__ ((unused)))
{
  if (stat)
    *stat = 0;
}

void
_gfortran_caf_co_sum (gfc_descriptor_t *a __attribute__ ((unused)),
		      int result_image __attribute__ ((unused)),
		      int *stat, char *errmsg __attribute__ ((unused)),
		      int errmsg_len __attribute__ ((unused)))
{
  if (stat)
    *stat = 0;
}

void
_gfortran_caf_co_min (gfc_descriptor_t *a __attribute__ ((unused)),
		      int result_image __attribute__ ((unused)),
		      int *stat, char *errmsg __attribute__ ((unused)),
		      int a_len __attribute__ ((unused)),
		      int errmsg_len __attribute__ ((unused)))
{
  if (stat)
    *stat = 0;
}

void
_gfortran_caf_co_max (gfc_descriptor_t *a __attribute__ ((unused)),
		      int result_image __attribute__ ((unused)),
		      int *stat, char *errmsg __attribute__ ((unused)),
		      int a_len __attribute__ ((unused)),
		      int errmsg_len __attribute__ ((unused)))
{
  if (stat)
    *stat = 0;
}


void
_gfortran_caf_co_reduce (gfc_descriptor_t *a __attribute__ ((unused)),
                        void * (*opr) (void *, void *)
                               __attribute__ ((unused)),
                        int opr_flags __attribute__ ((unused)),
                        int result_image __attribute__ ((unused)),
                        int *stat, char *errmsg __attribute__ ((unused)),
                        int a_len __attribute__ ((unused)),
                        int errmsg_len __attribute__ ((unused)))
 {
   if (stat)
     *stat = 0;
 }


static void
assign_char4_from_char1 (size_t dst_size, size_t src_size, uint32_t *dst,
			 unsigned char *src)
{
  size_t i, n;
  n = dst_size/4 > src_size ? src_size : dst_size/4;
  for (i = 0; i < n; ++i)
    dst[i] = (int32_t) src[i];
  for (; i < dst_size/4; ++i)
    dst[i] = (int32_t) ' ';
}


static void
assign_char1_from_char4 (size_t dst_size, size_t src_size, unsigned char *dst,
			 uint32_t *src)
{
  size_t i, n;
  n = dst_size > src_size/4 ? src_size/4 : dst_size;
  for (i = 0; i < n; ++i)
    dst[i] = src[i] > UINT8_MAX ? (unsigned char) '?' : (unsigned char) src[i];
  if (dst_size > n)
    memset(&dst[n], ' ', dst_size - n);
}


static void
convert_type (void *dst, int dst_type, int dst_kind, void *src, int src_type,
	      int src_kind, int *stat)
{
#ifdef HAVE_GFC_INTEGER_16
  typedef __int128 int128t;
#else
  typedef int64_t int128t;
#endif

#if defined(GFC_REAL_16_IS_LONG_DOUBLE)
  typedef long double real128t;
  typedef _Complex long double complex128t;
#elif defined(HAVE_GFC_REAL_16)
  typedef _Complex float __attribute__((mode(TC))) __complex128;
  typedef __float128 real128t;
  typedef __complex128 complex128t;
#elif defined(HAVE_GFC_REAL_10)
  typedef long double real128t;
  typedef long double complex128t;
#else
  typedef double real128t;
  typedef _Complex double complex128t;
#endif

  int128t int_val = 0;
  real128t real_val = 0;
  complex128t cmpx_val = 0;

  switch (src_type)
    {
    case BT_INTEGER:
      if (src_kind == 1)
	int_val = *(int8_t*) src;
      else if (src_kind == 2)
	int_val = *(int16_t*) src;
      else if (src_kind == 4)
	int_val = *(int32_t*) src;
      else if (src_kind == 8)
	int_val = *(int64_t*) src;
#ifdef HAVE_GFC_INTEGER_16
      else if (src_kind == 16)
	int_val = *(int128t*) src;
#endif
      else
	goto error;
      break;
    case BT_REAL:
      if (src_kind == 4)
	real_val = *(float*) src;
      else if (src_kind == 8)
	real_val = *(double*) src;
#ifdef HAVE_GFC_REAL_10
      else if (src_kind == 10)
	real_val = *(long double*) src;
#endif
#ifdef HAVE_GFC_REAL_16
      else if (src_kind == 16)
	real_val = *(real128t*) src;
#endif
      else
	goto error;
      break;
    case BT_COMPLEX:
      if (src_kind == 4)
	cmpx_val = *(_Complex float*) src;
      else if (src_kind == 8)
	cmpx_val = *(_Complex double*) src;
#ifdef HAVE_GFC_REAL_10
      else if (src_kind == 10)
	cmpx_val = *(_Complex long double*) src;
#endif
#ifdef HAVE_GFC_REAL_16
      else if (src_kind == 16)
	cmpx_val = *(complex128t*) src;
#endif
      else
	goto error;
      break;
    default:
      goto error;
    }

  switch (dst_type)
    {
    case BT_INTEGER:
      if (src_type == BT_INTEGER)
	{
	  if (dst_kind == 1)
	    *(int8_t*) dst = (int8_t) int_val;
	  else if (dst_kind == 2)
	    *(int16_t*) dst = (int16_t) int_val;
	  else if (dst_kind == 4)
	    *(int32_t*) dst = (int32_t) int_val;
	  else if (dst_kind == 8)
	    *(int64_t*) dst = (int64_t) int_val;
#ifdef HAVE_GFC_INTEGER_16
	  else if (dst_kind == 16)
	    *(int128t*) dst = (int128t) int_val;
#endif
	  else
	    goto error;
	}
      else if (src_type == BT_REAL)
	{
	  if (dst_kind == 1)
	    *(int8_t*) dst = (int8_t) real_val;
	  else if (dst_kind == 2)
	    *(int16_t*) dst = (int16_t) real_val;
	  else if (dst_kind == 4)
	    *(int32_t*) dst = (int32_t) real_val;
	  else if (dst_kind == 8)
	    *(int64_t*) dst = (int64_t) real_val;
#ifdef HAVE_GFC_INTEGER_16
	  else if (dst_kind == 16)
	    *(int128t*) dst = (int128t) real_val;
#endif
	  else
	    goto error;
	}
      else if (src_type == BT_COMPLEX)
	{
	  if (dst_kind == 1)
	    *(int8_t*) dst = (int8_t) cmpx_val;
	  else if (dst_kind == 2)
	    *(int16_t*) dst = (int16_t) cmpx_val;
	  else if (dst_kind == 4)
	    *(int32_t*) dst = (int32_t) cmpx_val;
	  else if (dst_kind == 8)
	    *(int64_t*) dst = (int64_t) cmpx_val;
#ifdef HAVE_GFC_INTEGER_16
	  else if (dst_kind == 16)
	    *(int128t*) dst = (int128t) cmpx_val;
#endif
	  else
	    goto error;
	}
      else
	goto error;
      break;
    case BT_REAL:
      if (src_type == BT_INTEGER)
	{
	  if (dst_kind == 4)
	    *(float*) dst = (float) int_val;
	  else if (dst_kind == 8)
	    *(double*) dst = (double) int_val;
#ifdef HAVE_GFC_REAL_10
	  else if (dst_kind == 10)
	    *(long double*) dst = (long double) int_val;
#endif
#ifdef HAVE_GFC_REAL_16
	  else if (dst_kind == 16)
	    *(real128t*) dst = (real128t) int_val;
#endif
	  else
	    goto error;
	}
      else if (src_type == BT_REAL)
	{
	  if (dst_kind == 4)
	    *(float*) dst = (float) real_val;
	  else if (dst_kind == 8)
	    *(double*) dst = (double) real_val;
#ifdef HAVE_GFC_REAL_10
	  else if (dst_kind == 10)
	    *(long double*) dst = (long double) real_val;
#endif
#ifdef HAVE_GFC_REAL_16
	  else if (dst_kind == 16)
	    *(real128t*) dst = (real128t) real_val;
#endif
	  else
	    goto error;
	}
      else if (src_type == BT_COMPLEX)
	{
	  if (dst_kind == 4)
	    *(float*) dst = (float) cmpx_val;
	  else if (dst_kind == 8)
	    *(double*) dst = (double) cmpx_val;
#ifdef HAVE_GFC_REAL_10
	  else if (dst_kind == 10)
	    *(long double*) dst = (long double) cmpx_val;
#endif
#ifdef HAVE_GFC_REAL_16
	  else if (dst_kind == 16)
	    *(real128t*) dst = (real128t) cmpx_val;
#endif
	  else
	    goto error;
	}
      break;
    case BT_COMPLEX:
      if (src_type == BT_INTEGER)
	{
	  if (dst_kind == 4)
	    *(_Complex float*) dst = (_Complex float) int_val;
	  else if (dst_kind == 8)
	    *(_Complex double*) dst = (_Complex double) int_val;
#ifdef HAVE_GFC_REAL_10
	  else if (dst_kind == 10)
	    *(_Complex long double*) dst = (_Complex long double) int_val;
#endif
#ifdef HAVE_GFC_REAL_16
	  else if (dst_kind == 16)
	    *(complex128t*) dst = (complex128t) int_val;
#endif
	  else
	    goto error;
	}
      else if (src_type == BT_REAL)
	{
	  if (dst_kind == 4)
	    *(_Complex float*) dst = (_Complex float) real_val;
	  else if (dst_kind == 8)
	    *(_Complex double*) dst = (_Complex double) real_val;
#ifdef HAVE_GFC_REAL_10
	  else if (dst_kind == 10)
	    *(_Complex long double*) dst = (_Complex long double) real_val;
#endif
#ifdef HAVE_GFC_REAL_16
	  else if (dst_kind == 16)
	    *(complex128t*) dst = (complex128t) real_val;
#endif
	  else
	    goto error;
	}
      else if (src_type == BT_COMPLEX)
	{
	  if (dst_kind == 4)
	    *(_Complex float*) dst = (_Complex float) cmpx_val;
	  else if (dst_kind == 8)
	    *(_Complex double*) dst = (_Complex double) cmpx_val;
#ifdef HAVE_GFC_REAL_10
	  else if (dst_kind == 10)
	    *(_Complex long double*) dst = (_Complex long double) cmpx_val;
#endif
#ifdef HAVE_GFC_REAL_16
	  else if (dst_kind == 16)
	    *(complex128t*) dst = (complex128t) cmpx_val;
#endif
	  else
	    goto error;
	}
      else
	goto error;
      break;
    default:
      goto error;
    }

error:
  fprintf (stderr, "libcaf_single RUNTIME ERROR: Cannot convert type %d kind "
	   "%d to type %d kind %d\n", src_type, src_kind, dst_type, dst_kind);
  if (stat)
    *stat = 1;
  else
    abort ();
}


void
_gfortran_caf_get (caf_token_t token, size_t offset,
		   int image_index __attribute__ ((unused)),
		   gfc_descriptor_t *src,
		   caf_vector_t *src_vector __attribute__ ((unused)),
		   gfc_descriptor_t *dest, int src_kind, int dst_kind,
		   bool may_require_tmp, int *stat)
{
  /* FIXME: Handle vector subscripts.  */
  size_t i, k, size;
  int j;
  int rank = GFC_DESCRIPTOR_RANK (dest);
  size_t src_size = GFC_DESCRIPTOR_SIZE (src);
  size_t dst_size = GFC_DESCRIPTOR_SIZE (dest);

  if (stat)
    *stat = 0;

  if (rank == 0)
    {
      void *sr = (void *) ((char *) MEMTOK (token) + offset);
      if (GFC_DESCRIPTOR_TYPE (dest) == GFC_DESCRIPTOR_TYPE (src)
	  && dst_kind == src_kind)
	{
	  memmove (GFC_DESCRIPTOR_DATA (dest), sr,
		   dst_size > src_size ? src_size : dst_size);
	  if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_size > src_size)
	    {
	      if (dst_kind == 1)
		memset ((void*)(char*) GFC_DESCRIPTOR_DATA (dest) + src_size,
			' ', dst_size - src_size);
	      else /* dst_kind == 4.  */
		for (i = src_size/4; i < dst_size/4; i++)
		  ((int32_t*) GFC_DESCRIPTOR_DATA (dest))[i] = (int32_t) ' ';
	    }
	}
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_kind == 1)
	assign_char1_from_char4 (dst_size, src_size, GFC_DESCRIPTOR_DATA (dest),
				 sr);
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER)
	assign_char4_from_char1 (dst_size, src_size, GFC_DESCRIPTOR_DATA (dest),
				 sr);
      else
	convert_type (GFC_DESCRIPTOR_DATA (dest), GFC_DESCRIPTOR_TYPE (dest),
		      dst_kind, sr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
      return;
    }

  size = 1;
  for (j = 0; j < rank; j++)
    {
      ptrdiff_t dimextent = dest->dim[j]._ubound - dest->dim[j].lower_bound + 1;
      if (dimextent < 0)
	dimextent = 0;
      size *= dimextent;
    }

  if (size == 0)
    return;

  if (may_require_tmp)
    {
      ptrdiff_t array_offset_sr, array_offset_dst;
      void *tmp = malloc (size*src_size);

      array_offset_dst = 0;
      for (i = 0; i < size; i++)
	{
	  ptrdiff_t array_offset_sr = 0;
	  ptrdiff_t stride = 1;
	  ptrdiff_t extent = 1;
	  for (j = 0; j < GFC_DESCRIPTOR_RANK (src)-1; j++)
	    {
	      array_offset_sr += ((i / (extent*stride))
				  % (src->dim[j]._ubound
				    - src->dim[j].lower_bound + 1))
				 * src->dim[j]._stride;
	      extent = (src->dim[j]._ubound - src->dim[j].lower_bound + 1);
	      stride = src->dim[j]._stride;
	    }
	  array_offset_sr += (i / extent) * src->dim[rank-1]._stride;
	  void *sr = (void *)((char *) MEMTOK (token) + offset
			  + array_offset_sr*GFC_DESCRIPTOR_SIZE (src));
          memcpy ((void *) ((char *) tmp + array_offset_dst), sr, src_size);
          array_offset_dst += src_size;
	}

      array_offset_sr = 0;
      for (i = 0; i < size; i++)
	{
	  ptrdiff_t array_offset_dst = 0;
	  ptrdiff_t stride = 1;
	  ptrdiff_t extent = 1;
	  for (j = 0; j < rank-1; j++)
	    {
	      array_offset_dst += ((i / (extent*stride))
				   % (dest->dim[j]._ubound
				      - dest->dim[j].lower_bound + 1))
				  * dest->dim[j]._stride;
	      extent = (dest->dim[j]._ubound - dest->dim[j].lower_bound + 1);
	      stride = dest->dim[j]._stride;
	    }
	  array_offset_dst += (i / extent) * dest->dim[rank-1]._stride;
	  void *dst = dest->base_addr
		      + array_offset_dst*GFC_DESCRIPTOR_SIZE (dest);
          void *sr = tmp + array_offset_sr;

	  if (GFC_DESCRIPTOR_TYPE (dest) == GFC_DESCRIPTOR_TYPE (src)
	      && dst_kind == src_kind)
	    {
	      memmove (dst, sr, dst_size > src_size ? src_size : dst_size);
	      if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER
	          && dst_size > src_size)
		{
		  if (dst_kind == 1)
		    memset ((void*)(char*) dst + src_size, ' ',
			    dst_size-src_size);
		  else /* dst_kind == 4.  */
		    for (k = src_size/4; k < dst_size/4; k++)
		      ((int32_t*) dst)[k] = (int32_t) ' ';
		}
	    }
	  else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_kind == 1)
	    assign_char1_from_char4 (dst_size, src_size, dst, sr);
	  else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER)
	    assign_char4_from_char1 (dst_size, src_size, dst, sr);
	  else
	    convert_type (dst, GFC_DESCRIPTOR_TYPE (dest), dst_kind,
			  sr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
          array_offset_sr += src_size;
	}

      free (tmp);
      return;
    }

  for (i = 0; i < size; i++)
    {
      ptrdiff_t array_offset_dst = 0;
      ptrdiff_t stride = 1;
      ptrdiff_t extent = 1;
      for (j = 0; j < rank-1; j++)
	{
	  array_offset_dst += ((i / (extent*stride))
			       % (dest->dim[j]._ubound
				  - dest->dim[j].lower_bound + 1))
			      * dest->dim[j]._stride;
	  extent = (dest->dim[j]._ubound - dest->dim[j].lower_bound + 1);
          stride = dest->dim[j]._stride;
	}
      array_offset_dst += (i / extent) * dest->dim[rank-1]._stride;
      void *dst = dest->base_addr + array_offset_dst*GFC_DESCRIPTOR_SIZE (dest);

      ptrdiff_t array_offset_sr = 0;
      stride = 1;
      extent = 1;
      for (j = 0; j < GFC_DESCRIPTOR_RANK (src)-1; j++)
	{
	  array_offset_sr += ((i / (extent*stride))
			       % (src->dim[j]._ubound
				  - src->dim[j].lower_bound + 1))
			      * src->dim[j]._stride;
	  extent = (src->dim[j]._ubound - src->dim[j].lower_bound + 1);
	  stride = src->dim[j]._stride;
	}
      array_offset_sr += (i / extent) * src->dim[rank-1]._stride;
      void *sr = (void *)((char *) MEMTOK (token) + offset
			  + array_offset_sr*GFC_DESCRIPTOR_SIZE (src));

      if (GFC_DESCRIPTOR_TYPE (dest) == GFC_DESCRIPTOR_TYPE (src)
	  && dst_kind == src_kind)
	{
	  memmove (dst, sr, dst_size > src_size ? src_size : dst_size);
	  if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_size > src_size)
	    {
	      if (dst_kind == 1)
		memset ((void*)(char*) dst + src_size, ' ', dst_size-src_size);
	      else /* dst_kind == 4.  */
		for (k = src_size/4; k < dst_size/4; k++)
		  ((int32_t*) dst)[k] = (int32_t) ' ';
	    }
	}
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_kind == 1)
	assign_char1_from_char4 (dst_size, src_size, dst, sr);
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER)
	assign_char4_from_char1 (dst_size, src_size, dst, sr);
      else
	convert_type (dst, GFC_DESCRIPTOR_TYPE (dest), dst_kind,
		      sr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
    }
}


void
_gfortran_caf_send (caf_token_t token, size_t offset,
		    int image_index __attribute__ ((unused)),
		    gfc_descriptor_t *dest,
		    caf_vector_t *dst_vector __attribute__ ((unused)),
		    gfc_descriptor_t *src, int dst_kind, int src_kind,
		    bool may_require_tmp, int *stat)
{
  /* FIXME: Handle vector subscripts.  */
  size_t i, k, size;
  int j;
  int rank = GFC_DESCRIPTOR_RANK (dest);
  size_t src_size = GFC_DESCRIPTOR_SIZE (src);
  size_t dst_size = GFC_DESCRIPTOR_SIZE (dest);

  if (stat)
    *stat = 0;

  if (rank == 0)
    {
      void *dst = (void *) ((char *) MEMTOK (token) + offset);
      if (GFC_DESCRIPTOR_TYPE (dest) == GFC_DESCRIPTOR_TYPE (src)
	  && dst_kind == src_kind)
	{
	  memmove (dst, GFC_DESCRIPTOR_DATA (src),
		   dst_size > src_size ? src_size : dst_size);
	  if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_size > src_size)
	    {
	      if (dst_kind == 1)
		memset ((void*)(char*) dst + src_size, ' ', dst_size-src_size);
	      else /* dst_kind == 4.  */
		for (i = src_size/4; i < dst_size/4; i++)
		  ((int32_t*) dst)[i] = (int32_t) ' ';
	    }
	}
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_kind == 1)
	assign_char1_from_char4 (dst_size, src_size, dst,
				 GFC_DESCRIPTOR_DATA (src));
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER)
	assign_char4_from_char1 (dst_size, src_size, dst,
				 GFC_DESCRIPTOR_DATA (src));
      else
	convert_type (dst, GFC_DESCRIPTOR_TYPE (dest), dst_kind,
		      GFC_DESCRIPTOR_DATA (src), GFC_DESCRIPTOR_TYPE (src),
		      src_kind, stat);
      return;
    }

  size = 1;
  for (j = 0; j < rank; j++)
    {
      ptrdiff_t dimextent = dest->dim[j]._ubound - dest->dim[j].lower_bound + 1;
      if (dimextent < 0)
	dimextent = 0;
      size *= dimextent;
    }

  if (size == 0)
    return;

  if (may_require_tmp)
    {
      ptrdiff_t array_offset_sr, array_offset_dst;
      void *tmp;

      if (GFC_DESCRIPTOR_RANK (src) == 0)
	{
	  tmp = malloc (src_size);
	  memcpy (tmp, GFC_DESCRIPTOR_DATA (src), src_size);
	}
      else
	{
	  tmp = malloc (size*src_size);
	  array_offset_dst = 0;
	  for (i = 0; i < size; i++)
	    {
	      ptrdiff_t array_offset_sr = 0;
	      ptrdiff_t stride = 1;
	      ptrdiff_t extent = 1;
	      for (j = 0; j < GFC_DESCRIPTOR_RANK (src)-1; j++)
		{
		  array_offset_sr += ((i / (extent*stride))
				      % (src->dim[j]._ubound
					 - src->dim[j].lower_bound + 1))
				     * src->dim[j]._stride;
		  extent = (src->dim[j]._ubound - src->dim[j].lower_bound + 1);
		  stride = src->dim[j]._stride;
		}
	      array_offset_sr += (i / extent) * src->dim[rank-1]._stride;
	      void *sr = (void *) ((char *) src->base_addr
				   + array_offset_sr*GFC_DESCRIPTOR_SIZE (src));
	      memcpy ((void *) ((char *) tmp + array_offset_dst), sr, src_size);
	      array_offset_dst += src_size;
	    }
	}

      array_offset_sr = 0;
      for (i = 0; i < size; i++)
	{
	  ptrdiff_t array_offset_dst = 0;
	  ptrdiff_t stride = 1;
	  ptrdiff_t extent = 1;
	  for (j = 0; j < rank-1; j++)
	    {
	      array_offset_dst += ((i / (extent*stride))
				   % (dest->dim[j]._ubound
				      - dest->dim[j].lower_bound + 1))
				  * dest->dim[j]._stride;
	  extent = (dest->dim[j]._ubound - dest->dim[j].lower_bound + 1);
          stride = dest->dim[j]._stride;
	    }
	  array_offset_dst += (i / extent) * dest->dim[rank-1]._stride;
	  void *dst = (void *)((char *) MEMTOK (token) + offset
		      + array_offset_dst*GFC_DESCRIPTOR_SIZE (dest));
          void *sr = tmp + array_offset_sr;
	  if (GFC_DESCRIPTOR_TYPE (dest) == GFC_DESCRIPTOR_TYPE (src)
	      && dst_kind == src_kind)
	    {
	      memmove (dst, sr,
		       dst_size > src_size ? src_size : dst_size);
	      if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER
		  && dst_size > src_size)
		{
		  if (dst_kind == 1)
		    memset ((void*)(char*) dst + src_size, ' ',
			    dst_size-src_size);
		  else /* dst_kind == 4.  */
		    for (k = src_size/4; k < dst_size/4; k++)
		      ((int32_t*) dst)[k] = (int32_t) ' ';
		}
	    }
	  else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_kind == 1)
	    assign_char1_from_char4 (dst_size, src_size, dst, sr);
	  else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER)
	    assign_char4_from_char1 (dst_size, src_size, dst, sr);
	  else
	    convert_type (dst, GFC_DESCRIPTOR_TYPE (dest), dst_kind,
			  sr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
          if (GFC_DESCRIPTOR_RANK (src))
	    array_offset_sr += src_size;
	}
      free (tmp);
      return;
    }

  for (i = 0; i < size; i++)
    {
      ptrdiff_t array_offset_dst = 0;
      ptrdiff_t stride = 1;
      ptrdiff_t extent = 1;
      for (j = 0; j < rank-1; j++)
	{
	  array_offset_dst += ((i / (extent*stride))
			       % (dest->dim[j]._ubound
				  - dest->dim[j].lower_bound + 1))
			      * dest->dim[j]._stride;
	  extent = (dest->dim[j]._ubound - dest->dim[j].lower_bound + 1);
          stride = dest->dim[j]._stride;
	}
      array_offset_dst += (i / extent) * dest->dim[rank-1]._stride;
      void *dst = (void *)((char *) MEMTOK (token) + offset
			   + array_offset_dst*GFC_DESCRIPTOR_SIZE (dest));
      void *sr;
      if (GFC_DESCRIPTOR_RANK (src) != 0)
	{
	  ptrdiff_t array_offset_sr = 0;
	  stride = 1;
	  extent = 1;
	  for (j = 0; j < GFC_DESCRIPTOR_RANK (src)-1; j++)
	    {
	      array_offset_sr += ((i / (extent*stride))
				  % (src->dim[j]._ubound
				     - src->dim[j].lower_bound + 1))
				 * src->dim[j]._stride;
	      extent = (src->dim[j]._ubound - src->dim[j].lower_bound + 1);
	      stride = src->dim[j]._stride;
	    }
	  array_offset_sr += (i / extent) * src->dim[rank-1]._stride;
	  sr = (void *)((char *) src->base_addr
			+ array_offset_sr*GFC_DESCRIPTOR_SIZE (src));
	}
      else
	sr = src->base_addr;

      if (GFC_DESCRIPTOR_TYPE (dest) == GFC_DESCRIPTOR_TYPE (src)
	  && dst_kind == src_kind)
	{
	  memmove (dst, sr,
		   dst_size > src_size ? src_size : dst_size);
	  if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_size > src_size)
	    {
	      if (dst_kind == 1)
		memset ((void*)(char*) dst + src_size, ' ', dst_size-src_size);
	      else /* dst_kind == 4.  */
		for (k = src_size/4; k < dst_size/4; k++)
		  ((int32_t*) dst)[k] = (int32_t) ' ';
	    }
	}
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER && dst_kind == 1)
	assign_char1_from_char4 (dst_size, src_size, dst, sr);
      else if (GFC_DESCRIPTOR_TYPE (dest) == BT_CHARACTER)
	assign_char4_from_char1 (dst_size, src_size, dst, sr);
      else
	convert_type (dst, GFC_DESCRIPTOR_TYPE (dest), dst_kind,
		      sr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
    }
}


void
_gfortran_caf_sendget (caf_token_t dst_token, size_t dst_offset,
		       int dst_image_index, gfc_descriptor_t *dest,
		       caf_vector_t *dst_vector, caf_token_t src_token,
		       size_t src_offset,
		       int src_image_index __attribute__ ((unused)),
		       gfc_descriptor_t *src,
		       caf_vector_t *src_vector __attribute__ ((unused)),
		       int dst_kind, int src_kind, bool may_require_tmp)
{
  /* FIXME: Handle vector subscript of 'src_vector'.  */
  /* For a single image, src->base_addr should be the same as src_token + offset
     but to play save, we do it properly.  */
  void *src_base = GFC_DESCRIPTOR_DATA (src);
  GFC_DESCRIPTOR_DATA (src) = (void *) ((char *) MEMTOK (src_token) + src_offset);
  _gfortran_caf_send (dst_token, dst_offset, dst_image_index, dest, dst_vector,
		      src, dst_kind, src_kind, may_require_tmp, NULL);
  GFC_DESCRIPTOR_DATA (src) = src_base;
}


/* Emitted when a theorectically unreachable part is reached.  */
const char unreachable[] = "Fatal error: unreachable alternative found.\n";


static void
copy_data (void *ds, void *sr, gfc_descriptor_t *dst, gfc_descriptor_t *src,
	   int dst_kind, int src_kind, size_t dst_size, size_t src_size,
	   size_t num, int *stat)
{
  size_t k;
  if (GFC_DESCRIPTOR_TYPE (dst) == GFC_DESCRIPTOR_TYPE (src)
      && dst_kind == src_kind)
    {
      memmove (ds, sr, (dst_size > src_size ? src_size : dst_size) * num);
      if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER && dst_size > src_size)
	{
	  if (dst_kind == 1)
	    memset ((void*)(char*) dst + src_size, ' ', dst_size-src_size);
	  else /* dst_kind == 4.  */
	    for (k = src_size/4; k < dst_size/4; k++)
	      ((int32_t*) dst)[k] = (int32_t) ' ';
	}
    }
  else if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER && dst_kind == 1)
    assign_char1_from_char4 (dst_size, src_size, ds, sr);
  else if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER)
    assign_char4_from_char1 (dst_size, src_size, ds, sr);
  else
    for (k = 0; k < num; ++k)
      {
	convert_type (ds, GFC_DESCRIPTOR_TYPE (dst), dst_kind,
		      sr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
	ds += dst_size;
	sr += src_size;
      }
}


static void
get_for_ref (caf_reference_t *ref, size_t *i, size_t *dst_index,
	     caf_single_token_t single_token, gfc_descriptor_t *dst,
	     gfc_descriptor_t *src, void *ds, void *sr,
	     int dst_kind, int src_kind, size_t dst_dim, size_t src_dim,
	     size_t num, int *stat)
{
  const char vecrefunknownkind[] = "libcaf_single::caf_get_by_ref(): "
      "unknown kind in vector-ref.\n";
  ptrdiff_t extent_src = 1, array_offset_src = 0;

  if (unlikely (ref == NULL))
    /* May be we should issue an error here, because this case should not
       occur.  */
    return;

  if (ref->next == NULL)
    {
      size_t dst_size = GFC_DESCRIPTOR_SIZE (dst);
      ptrdiff_t array_offset_dst = 0;;
      size_t dst_rank = GFC_DESCRIPTOR_RANK (dst);

      switch (ref->type)
	{
	case CAF_REF_COMPONENT:
	  if (ref->u.c.idx >= 0)
	    copy_data (ds, single_token->components[ref->u.c.idx]->memptr,
		       dst, single_token->components[ref->u.c.idx]->desc,
		       dst_kind, src_kind, dst_size, ref->item_size, 1, stat);
	  else
	    copy_data (ds, sr + ref->u.c.offset, dst, src,
		       dst_kind, src_kind, dst_size, ref->item_size, 1, stat);
	  ++(*i);
	  return;
	case CAF_REF_ARRAY:
	  if (ref->u.a.mode[src_dim] == CAF_ARR_REF_NONE)
	    {
	      for (size_t d = 0; d < dst_rank; ++d)
		array_offset_dst += dst_index[d];
	      copy_data (ds + array_offset_dst * dst_size, sr, dst, src,
			 dst_kind, src_kind, dst_size, ref->item_size, num,
			 stat);
	      *i += num;
	      return;
	    }
	  else
	    {
	      /* Only when on the left most index switch the data pointer to
		 the array's data pointer.  */
	      if (src_dim == 0)
		sr = GFC_DESCRIPTOR_DATA (src);

	      break;
	    }
	default:
	  caf_runtime_error(unreachable);
	}
    }

  switch (ref->type)
    {
    case CAF_REF_COMPONENT:
      if (ref->u.c.idx >= 0)
	get_for_ref (ref->next, i, dst_index,
		     single_token->components[ref->u.c.idx],
		     dst, single_token->components[ref->u.c.idx]->desc, ds,
		     single_token->components[ref->u.c.idx]->memptr, dst_kind,
		     src_kind, dst_dim, 0, 1, stat);
      else
	get_for_ref (ref->next, i, dst_index, single_token, dst,
		     (gfc_descriptor_t *)(sr + ref->u.c.offset), ds,
		     sr + ref->u.c.offset, dst_kind, src_kind, dst_dim, 0, 1,
		     stat);
      return;
    case CAF_REF_ARRAY:
      if (ref->u.a.mode[src_dim] == CAF_ARR_REF_NONE)
	{
	  get_for_ref (ref->next, i, dst_index, single_token, dst,
		       (gfc_descriptor_t *)sr, ds, sr, dst_kind, src_kind,
		       dst_dim, 0, 1, stat);
	  return;
	}
      switch (ref->u.a.mode[src_dim])
	{
	case CAF_ARR_REF_VECTOR:
	  extent_src = GFC_DIMENSION_EXTENT (src->dim[src_dim]);
	  array_offset_src = 0;
	  dst_index[dst_dim] = 0;
	  for (size_t idx = 0; idx < ref->u.a.dim[src_dim].v.nvec;
	       ++idx)
	    {
#define KINDCASE(kind, type) case kind:                                      \
	      array_offset_src = (((index_type)                      \
		  ((type *)ref->u.a.dim[src_dim].v.vector)[idx])     \
		  - GFC_DIMENSION_LBOUND (src->dim[src_dim]))        \
		  * GFC_DIMENSION_STRIDE (src->dim[src_dim]);        \
	      break

	      switch (ref->u.a.dim[src_dim].v.kind)
		{
		KINDCASE (1, GFC_INTEGER_1);
		KINDCASE (2, GFC_INTEGER_2);
		KINDCASE (4, GFC_INTEGER_4);
#ifdef HAVE_GFC_INTEGER_8
		KINDCASE (8, GFC_INTEGER_8);
#endif
#ifdef HAVE_GFC_INTEGER_16
		KINDCASE (16, GFC_INTEGER_16);
#endif
		default:
		  caf_internal_error (vecrefunknownkind,
				      sizeof (vecrefunknownkind), stat,
				      NULL, 0);
		  return;
		}
#undef KINDCASE

	      get_for_ref (ref, i, dst_index, single_token, dst, src,
			   ds, sr + array_offset_src * ref->item_size,
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, stat);
	      dst_index[dst_dim] +=
		  GFC_DIMENSION_STRIDE (dst->dim[dst_dim]);
	    }
	  return;
	case CAF_ARR_REF_FULL:
//		  if (src->dim[dim]._stride == 1)
//		    {
	      /* Contiguous data can be copied faster.  */
//		    }

	  extent_src = GFC_DIMENSION_EXTENT (src->dim[src_dim]);
	  array_offset_src = 0;
	  dst_index[dst_dim] = 0;
	  for (index_type idx = 0; idx < extent_src;
	       ++idx, array_offset_src += src->dim[src_dim]._stride)
	    {
	      get_for_ref (ref, i, dst_index, single_token, dst, src,
			   ds, sr + array_offset_src * ref->item_size,
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, stat);
	      dst_index[dst_dim] +=
		  GFC_DIMENSION_STRIDE (dst->dim[dst_dim]);
	    }
	  return;
	case CAF_ARR_REF_RANGE:
	  extent_src = (ref->u.a.dim[src_dim].s.end
			- ref->u.a.dim[src_dim].s.start)
	      / ref->u.a.dim[src_dim].s.stride + 1;
	  array_offset_src = ref->u.a.dim[src_dim].s.start
	      - GFC_DIMENSION_LBOUND (src->dim[src_dim]);
	  dst_index[dst_dim] = 0;
	  for (index_type idx = 0; idx < extent_src; ++idx)
	    {
	      get_for_ref (ref, i, dst_index, single_token, dst, src,
			   ds, sr + array_offset_src * ref->item_size
			   * GFC_DIMENSION_STRIDE (src->dim[src_dim]),
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, stat);
	      dst_index[dst_dim] +=
		  GFC_DIMENSION_STRIDE (dst->dim[dst_dim]);
	      array_offset_src += ref->u.a.dim[src_dim].s.stride;
	    }
	  return;
	case CAF_ARR_REF_SINGLE:
	  array_offset_src = (ref->u.a.dim[src_dim].s.start
			       - src->dim[src_dim].lower_bound)
			     * GFC_DIMENSION_STRIDE (src->dim[src_dim]);
	  get_for_ref (ref, i, dst_index, single_token, dst, src, ds,
		       sr + array_offset_src * ref->item_size,
		       dst_kind, src_kind, dst_dim, src_dim + 1, 1,
		       stat);
	  return;
	case CAF_ARR_REF_OPEN_END:
	  extent_src = (GFC_DIMENSION_UBOUND (src->dim[src_dim])
			- ref->u.a.dim[src_dim].s.start)
	      / ref->u.a.dim[src_dim].s.stride + 1;
	  array_offset_src = ref->u.a.dim[src_dim].s.start
	      - GFC_DIMENSION_LBOUND (src->dim[src_dim]);
	  dst_index[dst_dim] = 0;
	  for (index_type idx = 0; idx < extent_src; ++idx)
	    {
	      get_for_ref (ref, i, dst_index, single_token, dst, src,
			   ds, sr + array_offset_src * ref->item_size
			   * GFC_DIMENSION_STRIDE (src->dim[src_dim]),
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, stat);
	      dst_index[dst_dim] +=
		  GFC_DIMENSION_STRIDE (dst->dim[dst_dim]);
	      array_offset_src += ref->u.a.dim[src_dim].s.stride;
	    }
	  return;
	case CAF_ARR_REF_OPEN_START:
	  extent_src = (ref->u.a.dim[src_dim].s.end
			- GFC_DIMENSION_LBOUND (src->dim[src_dim]))
	      / ref->u.a.dim[src_dim].s.stride + 1;
	  array_offset_src = 0;
	  dst_index[dst_dim] = 0;
	  for (index_type idx = 0; idx < extent_src; ++idx)
	    {
	      get_for_ref (ref, i, dst_index, single_token, dst, src,
			   ds, sr + array_offset_src * ref->item_size
			   * GFC_DIMENSION_STRIDE (src->dim[src_dim]),
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, stat);
	      dst_index[dst_dim] +=
		  GFC_DIMENSION_STRIDE (dst->dim[dst_dim]);
	      array_offset_src += ref->u.a.dim[src_dim].s.stride;
	    }
	  return;
	default:
	  caf_runtime_error(unreachable);
	}
      return;
    default:
      caf_runtime_error(unreachable);
    }
}


void
_gfortran_caf_get_by_ref (caf_token_t token,
			  int image_index __attribute__ ((unused)),
			  gfc_descriptor_t *dst, caf_reference_t *refs,
			  int dst_kind, int src_kind, bool may_require_tmp,
			  bool dst_reallocatable, int *stat)
{
  const char compidxoutofrange[] = "libcaf_single::caf_get_by_ref(): "
				   "component index out of range.\n";
  const char unknownreftype[] = "libcaf_single::caf_get_by_ref(): "
				"unknown reference type.\n";
  const char unknownarrreftype[] = "libcaf_single::caf_get_by_ref(): "
				   "unknown array reference type.\n";
  const char rankoutofrange[] = "libcaf_single::caf_get_by_ref(): "
				"rank out of range.\n";
  const char extentoutofrange[] = "libcaf_single::caf_get_by_ref(): "
				  "extent out of range.\n";
  const char cannotallocdst[] = "libcaf_single::caf_get_by_ref(): "
				"can not allocate memory.\n";
  const char nonallocextentmismatch[] = "libcaf_single::caf_get_by_ref(): "
      "extent of non-allocatable array mismatch.\n";
  size_t size, i;
  size_t dst_index[GFC_MAX_DIMENSIONS];
  int dst_rank = GFC_DESCRIPTOR_RANK (dst);
  int dst_cur_dim = 0;
  size_t src_size;
  caf_single_token_t single_token = TOKEN (token);
  void *memptr = single_token->memptr;
  gfc_descriptor_t *src = single_token->desc;
  caf_reference_t *riter = refs;
  long delta;
  /* Reallocation of dst.data is needed (e.g., array to small).  */
  bool realloc_needed;
  /* Reallocation of dst.data is required, because data is not alloced at
     all.  */
  bool realloc_required;
  realloc_needed = realloc_required = GFC_DESCRIPTOR_DATA (dst) == NULL;

  assert (!realloc_needed || (realloc_needed && dst_reallocatable));

  if (stat)
    *stat = 0;

  /* The first ref has to be a component ref.  */
  if (refs->type != CAF_REF_COMPONENT)
    {
      const char firstneedstobecompref[] = "libcaf_single::caf_get_by_ref(): "
	"first ref needs to be a component ref.\n";
      caf_internal_error (firstneedstobecompref, sizeof (firstneedstobecompref),
			  stat, NULL, 0);
      return;
    }

  /* Compute the size of the result.  In the beginning size just counts the
     number of elements.  */
  size = 1;
  while (riter)
    {
      switch (riter->type)
	{
	case CAF_REF_COMPONENT:
	  if (riter->u.c.idx >= 0)
	    {
	      /* Dynamically allocated component.  */
	      if (unlikely (single_token->num_comps < riter->u.c.idx))
		{
		  caf_internal_error (compidxoutofrange, sizeof (compidxoutofrange),
				      stat, NULL, 0);
		  return;
		}
	      single_token = single_token->components[riter->u.c.idx];
	      memptr = single_token->memptr;
	      src = single_token->desc;
	    }
	  else
	    {
	      memptr += riter->u.c.offset;
	      src = (gfc_descriptor_t *)memptr;
	    }
	  break;
	case CAF_REF_ARRAY:
	  for (i = 0; riter->u.a.mode[i] != CAF_ARR_REF_NONE; ++i)
	    {
	      switch (riter->u.a.mode[i])
		{
		case CAF_ARR_REF_VECTOR:
		  delta = riter->u.a.dim[i].v.nvec;
		  break;
		case CAF_ARR_REF_FULL:
		  delta = (src->dim[i]._ubound - src->dim[i].lower_bound + 1);
		  break;
		case CAF_ARR_REF_RANGE:
		  delta = (riter->u.a.dim[i].s.end - riter->u.a.dim[i].s.start)
		      / riter->u.a.dim[i].s.stride + 1;
		  break;
		case CAF_ARR_REF_SINGLE:
		  delta = 1;
		  break;
		case CAF_ARR_REF_OPEN_END:
		  delta = (src->dim[i]._ubound - riter->u.a.dim[i].s.start)
		      / riter->u.a.dim[i].s.stride + 1;
		  break;
		case CAF_ARR_REF_OPEN_START:
		  delta = (riter->u.a.dim[i].s.end - src->dim[i].lower_bound)
		      / riter->u.a.dim[i].s.stride + 1;
		  break;
		default:
		  caf_internal_error (unknownarrreftype,
				      sizeof (unknownarrreftype), stat,
				      NULL, 0);
		  return;
		}
	      if (delta <= 0)
		return;
	      if (delta != 1)
		{
		  /* Non-scalar dimension.  */
		  if (unlikely (dst_cur_dim >= dst_rank))
		    {
		      caf_internal_error (rankoutofrange,
					  sizeof (rankoutofrange),
					  stat, NULL, 0);
		      return;
		    }
		  if (realloc_required || realloc_needed
		      || GFC_DESCRIPTOR_EXTENT (dst, dst_cur_dim) != delta)
		    {
		      if (unlikely (!dst_reallocatable))
			{
			  caf_internal_error (nonallocextentmismatch,
					      sizeof (nonallocextentmismatch),
					      stat, NULL, 0);
			}
		      else if (! dst_reallocatable
			       && GFC_DESCRIPTOR_EXTENT (dst, dst_cur_dim)
				  != delta)
			{
			  caf_internal_error (extentoutofrange,
					      sizeof (extentoutofrange),
					      stat, NULL, 0);
			  return;
			}
		      realloc_needed = true;
		      GFC_DIMENSION_SET (dst->dim[dst_cur_dim], 1, delta, size);
		    }
		  ++dst_cur_dim;
		}
	      size *= (index_type)delta;
	    }
	  /* For now take the first entry from the array to proceed in size
	     calculation.
	     TODO: Take the actually first reffed item of the array, because
		   the first entry in the array may point to unallocated
		   data.  */
	  memptr = GFC_DESCRIPTOR_DATA (src);
	  break;
	default:
	  caf_internal_error (unknownreftype, sizeof (unknownreftype), stat,
			      NULL, 0);
	  return;
	}
      src_size = riter->item_size;
      riter = riter->next;
    }
  if (size == 0 || src_size == 0)
    return;
  /* Postcondition:
     - size contains the number of elements to store in the destination array,
     - src_size gives the size in bytes of each item in the destination array.
  */

  if (realloc_needed)
    {
      /* TODO: dst_dim > 1 */
      GFC_DESCRIPTOR_DATA (dst) = malloc (size * GFC_DESCRIPTOR_SIZE (dst));
      if (unlikely (GFC_DESCRIPTOR_DATA (dst) == NULL))
	{
	  caf_internal_error (cannotallocdst, sizeof (cannotallocdst), stat,
			      NULL, 0);
	  return;
	}
    }
#if 0
  if (rank == 0)
    {
      if (GFC_DESCRIPTOR_TYPE (dst) == GFC_DESCRIPTOR_TYPE (src)
	  && dst_kind == src_kind)
	{
	  memmove (GFC_DESCRIPTOR_DATA (dst), memptr,
		   dst_size > src_size ? src_size : dst_size);
	  if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER && dst_size > src_size)
	    {
	      if (dst_kind == 1)
		memset ((void*)(char*) GFC_DESCRIPTOR_DATA (dst) + src_size,
			' ', dst_size - src_size);
	      else /* dst_kind == 4.  */
		for (i = src_size/4; i < dst_size/4; i++)
		  ((int32_t*) GFC_DESCRIPTOR_DATA (dst))[i] = (int32_t) ' ';
	    }
	}
      else if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER && dst_kind == 1)
	assign_char1_from_char4 (dst_size, src_size, GFC_DESCRIPTOR_DATA (dst),
				 memptr);
      else if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER)
	assign_char4_from_char1 (dst_size, src_size, GFC_DESCRIPTOR_DATA (dst),
				 memptr);
      else
	convert_type (GFC_DESCRIPTOR_DATA (dst), GFC_DESCRIPTOR_TYPE (dst),
		      dst_kind, memptr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
      return;
    }

  if (may_require_tmp)
    {
      ptrdiff_t array_offset_sr, array_offset_dst;
      void *tmp = malloc (size*src_size);

      array_offset_dst = 0;
      for (i = 0; i < size; i++)
	{
	  ptrdiff_t array_offset_sr = 0;
	  ptrdiff_t stride = 1;
	  ptrdiff_t extent = 1;
	  for (j = 0; j < GFC_DESCRIPTOR_RANK (src)-1; j++)
	    {
	      array_offset_sr += ((i / (extent*stride))
				  % (src->dim[j]._ubound
				    - src->dim[j].lower_bound + 1))
				 * src->dim[j]._stride;
	      extent = (src->dim[j]._ubound - src->dim[j].lower_bound + 1);
	      stride = src->dim[j]._stride;
	    }
	  array_offset_sr += (i / extent) * src->dim[rank-1]._stride;
	  void *sr = (void *)((char *) MEMTOK (token) + offset
			  + array_offset_sr*GFC_DESCRIPTOR_SIZE (src));
          memcpy ((void *) ((char *) tmp + array_offset_dst), sr, src_size);
          array_offset_dst += src_size;
	}

      array_offset_sr = 0;
      for (i = 0; i < size; i++)
	{
	  ptrdiff_t array_offset_dst = 0;
	  ptrdiff_t stride = 1;
	  ptrdiff_t extent = 1;
	  for (j = 0; j < rank-1; j++)
	    {
	      array_offset_dst += ((i / (extent*stride))
				   % (dst->dim[j]._ubound
				      - dst->dim[j].lower_bound + 1))
				  * dst->dim[j]._stride;
	      extent = (dst->dim[j]._ubound - dst->dim[j].lower_bound + 1);
	      stride = dst->dim[j]._stride;
	    }
	  array_offset_dst += (i / extent) * dst->dim[rank-1]._stride;
	  void *dst = dst->base_addr
		      + array_offset_dst*GFC_DESCRIPTOR_SIZE (dst);
          void *sr = tmp + array_offset_sr;

	  if (GFC_DESCRIPTOR_TYPE (dst) == GFC_DESCRIPTOR_TYPE (src)
	      && dst_kind == src_kind)
	    {
	      memmove (dst, sr, dst_size > src_size ? src_size : dst_size);
	      if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER
	          && dst_size > src_size)
		{
		  if (dst_kind == 1)
		    memset ((void*)(char*) dst + src_size, ' ',
			    dst_size-src_size);
		  else /* dst_kind == 4.  */
		    for (k = src_size/4; k < dst_size/4; k++)
		      ((int32_t*) dst)[k] = (int32_t) ' ';
		}
	    }
	  else if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER && dst_kind == 1)
	    assign_char1_from_char4 (dst_size, src_size, dst, sr);
	  else if (GFC_DESCRIPTOR_TYPE (dst) == BT_CHARACTER)
	    assign_char4_from_char1 (dst_size, src_size, dst, sr);
	  else
	    convert_type (dst, GFC_DESCRIPTOR_TYPE (dst), dst_kind,
			  sr, GFC_DESCRIPTOR_TYPE (src), src_kind, stat);
          array_offset_sr += src_size;
	}

      free (tmp);
      return;
    }
#endif

  /* Reset the token.  */
  single_token = TOKEN (token);
  memptr = single_token->memptr;
  src = single_token->desc;
  memset(dst_index, 0, sizeof (dst_index));
  i = 0;
  while (i < size)
    {
      get_for_ref (refs, &i, dst_index, single_token, dst, src,
		   GFC_DESCRIPTOR_DATA (dst), memptr, dst_kind, src_kind, 0, 0,
		   1, stat);
    }
}


static void
send_by_ref (caf_reference_t *ref, size_t *i, size_t *src_index,
	     caf_single_token_t single_token, gfc_descriptor_t *dst,
	     gfc_descriptor_t *src, void *ds, void *sr,
	     int dst_kind, int src_kind, size_t dst_dim, size_t src_dim,
	     size_t num, size_t size, int *stat)
{
  const char vecrefunknownkind[] = "libcaf_single::caf_send_by_ref(): "
      "unknown kind in vector-ref.\n";
  ptrdiff_t extent_dst = 1, array_offset_dst = 0;

  if (unlikely (ref == NULL))
    /* May be we should issue an error here, because this case should not
       occur.  */
    return;

  if (ref->next == NULL)
    {
      size_t dst_size = GFC_DESCRIPTOR_SIZE (dst);
      ptrdiff_t array_offset_src = 0;;
      size_t dst_rank = GFC_DESCRIPTOR_RANK (dst);

      switch (ref->type)
	{
	case CAF_REF_COMPONENT:
	  if (ref->u.c.idx >= 0)
	    {
	      if (single_token->components[ref->u.c.idx] == NULL)
		{
		  dst = (gfc_descriptor_t *)
		      malloc (sizeof (gfc_descriptor_t));
		  GFC_DESCRIPTOR_DATA (dst) = NULL;
		  GFC_DESCRIPTOR_DTYPE (dst) =
		      GFC_DESCRIPTOR_DTYPE (src);
		  dst_size = GFC_DESCRIPTOR_SIZE (dst);
		  /* The component may be allocated now, because it is a
				 scalar.  */
		  _gfortran_caf_register_component (single_token,
						    CAF_REGTYPE_COARRAY_ALLOC,
						    ref->item_size,
						    ref->u.c.idx,
						    dst, stat, NULL,
						    0, 0);
		  /* In case of an error in allocation return.  When stat is
				 NULL, then register_component() terminates on error.  */
		  if (stat != NULL && *stat)
		    return;
		  /* Publish the allocated memory.  */
		  *((void **)(ds + ref->u.c.offset)) =
		      single_token->components[ref->u.c.idx]->memptr;
		  /* The memptr, descriptor and the token are set below.  */
		}
	      copy_data (single_token->components[ref->u.c.idx]->memptr, sr,
		  single_token->components[ref->u.c.idx]->desc, src,
		  dst_kind, src_kind, dst_size, ref->item_size, 1, stat);
	    }
	  else
	    copy_data (ds + ref->u.c.offset, sr, dst, src,
		       dst_kind, src_kind, dst_size, ref->item_size, 1, stat);
	  ++(*i);
	  return;
	case CAF_REF_ARRAY:
	  if (ref->u.a.mode[dst_dim] == CAF_ARR_REF_NONE)
	    {
	      for (size_t d = 0; d < dst_rank; ++d)
		array_offset_src += src_index[d];
	      copy_data (ds, sr + array_offset_src * ref->item_size, dst, src,
			 dst_kind, src_kind, dst_size, ref->item_size, num,
			 stat);
	      *i += num;
	      return;
	    }
	  else
	    {
	      /* Only when on the left most index switch the data pointer to
		 the array's data pointer.  */
	      if (dst_dim == 0)
		ds = GFC_DESCRIPTOR_DATA (dst);

	      break;
	    }
	default:
	  caf_runtime_error(unreachable);
	}
    }

  switch (ref->type)
    {
    case CAF_REF_COMPONENT:
      if (ref->u.c.idx >= 0)
	{
	  if (single_token->components[ref->u.c.idx] == NULL)
	    {
	      /* This component refs an unallocated array.  */
	      dst = (gfc_descriptor_t *)(ds + ref->u.c.offset);
	      /* Assume that the rank and the dimensions fit for copying src
		 to dst.  */
	      *dst = *src;
	      /* Null the data-pointer to make register_component allocate
		 its own memory.  */
	      GFC_DESCRIPTOR_DATA (dst) = NULL;

	      /* The size of the array is given by size.  */
	      _gfortran_caf_register_component (single_token,
						CAF_REGTYPE_COARRAY_ALLOC,
						size * ref->item_size,
						ref->u.c.idx,
						dst, stat, NULL,
						0, 0);
	      /* In case of an error in allocation return.  When stat is
		 NULL, then register_component() terminates on error.  */
	      if (stat != NULL && *stat)
		return;
	      /* Publish the allocated memory.  */
	      GFC_DESCRIPTOR_DATA (dst) =
		  single_token->components[ref->u.c.idx]->memptr;
	      /* The memptr, descriptor and the token are set below.  */
	    }
	  send_by_ref (ref->next, i, src_index,
		       single_token->components[ref->u.c.idx],
		       single_token->components[ref->u.c.idx]->desc, src,
		       single_token->components[ref->u.c.idx]->memptr, sr,
		       dst_kind, src_kind, 0, src_dim, 1, size, stat);
	}
      else
	send_by_ref (ref->next, i, src_index, single_token,
		     (gfc_descriptor_t *)(ds + ref->u.c.offset), src,
		     ds + ref->u.c.offset, sr, dst_kind, src_kind, 0, src_dim,
		     1, size, stat);
      return;
    case CAF_REF_ARRAY:
      if (ref->u.a.mode[dst_dim] == CAF_ARR_REF_NONE)
	{
	  send_by_ref (ref->next, i, src_index, single_token,
		       (gfc_descriptor_t *)ds, src, ds, sr, dst_kind, src_kind,
		       0, src_dim, 1, size, stat);
	  return;
	}
      switch (ref->u.a.mode[dst_dim])
	{
	case CAF_ARR_REF_VECTOR:
	  extent_dst = GFC_DIMENSION_EXTENT (dst->dim[dst_dim]);
	  array_offset_dst = 0;
	  src_index[src_dim] = 0;
	  for (size_t idx = 0; idx < ref->u.a.dim[dst_dim].v.nvec;
	       ++idx)
	    {
#define KINDCASE(kind, type) case kind:                                      \
	      array_offset_dst = (((index_type)                      \
		  ((type *)ref->u.a.dim[dst_dim].v.vector)[idx])     \
		  - GFC_DIMENSION_LBOUND (dst->dim[dst_dim]))        \
		  * GFC_DIMENSION_STRIDE (dst->dim[dst_dim]);        \
	      break

	      switch (ref->u.a.dim[dst_dim].v.kind)
		{
		KINDCASE (1, GFC_INTEGER_1);
		KINDCASE (2, GFC_INTEGER_2);
		KINDCASE (4, GFC_INTEGER_4);
#ifdef HAVE_GFC_INTEGER_8
		KINDCASE (8, GFC_INTEGER_8);
#endif
#ifdef HAVE_GFC_INTEGER_16
		KINDCASE (16, GFC_INTEGER_16);
#endif
		default:
		  caf_internal_error (vecrefunknownkind,
				      sizeof (vecrefunknownkind), stat,
				      NULL, 0);
		  return;
		}
#undef KINDCASE

	      send_by_ref (ref, i, src_index, single_token, dst, src,
			   ds + array_offset_dst * ref->item_size, sr,
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, size, stat);
	      src_index[src_dim] +=
		  GFC_DIMENSION_STRIDE (src->dim[src_dim]);
	    }
	  return;
	case CAF_ARR_REF_FULL:
	  extent_dst = GFC_DIMENSION_EXTENT (dst->dim[dst_dim]);
	  array_offset_dst = 0;
	  src_index[src_dim] = 0;
	  for (index_type idx = 0; idx < extent_dst;
	       ++idx, array_offset_dst += dst->dim[dst_dim]._stride)
	    {
	      send_by_ref (ref, i, src_index, single_token, dst, src,
			   ds + array_offset_dst * ref->item_size, sr,
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, size, stat);
	      src_index[src_dim] +=
		  GFC_DIMENSION_STRIDE (src->dim[src_dim]);
	    }
	  return;
	case CAF_ARR_REF_RANGE:
	  extent_dst = (ref->u.a.dim[dst_dim].s.end
			- ref->u.a.dim[dst_dim].s.start)
	      / ref->u.a.dim[dst_dim].s.stride + 1;
	  array_offset_dst = ref->u.a.dim[dst_dim].s.start
	      - GFC_DIMENSION_LBOUND (dst->dim[dst_dim]);
	  src_index[src_dim] = 0;
	  for (index_type idx = 0; idx < extent_dst; ++idx)
	    {
	      send_by_ref (ref, i, src_index, single_token, dst, src,
			   ds + array_offset_dst * ref->item_size
			   * GFC_DIMENSION_STRIDE (dst->dim[dst_dim]), sr,
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, size, stat);
	      src_index[src_dim] +=
		  GFC_DIMENSION_STRIDE (src->dim[src_dim]);
	      array_offset_dst += ref->u.a.dim[dst_dim].s.stride;
	    }
	  return;
	case CAF_ARR_REF_SINGLE:
	  array_offset_dst = (ref->u.a.dim[dst_dim].s.start
			       - dst->dim[dst_dim].lower_bound)
			     * GFC_DIMENSION_STRIDE (dst->dim[dst_dim]);
	  send_by_ref (ref, i, src_index, single_token, dst, src, ds
		       + array_offset_dst * ref->item_size, sr,
		       dst_kind, src_kind, dst_dim + 1, src_dim, 1,
		       size, stat);
	  return;
	case CAF_ARR_REF_OPEN_END:
	  extent_dst = (GFC_DIMENSION_UBOUND (dst->dim[dst_dim])
			- ref->u.a.dim[dst_dim].s.start)
	      / ref->u.a.dim[dst_dim].s.stride + 1;
	  array_offset_dst = ref->u.a.dim[dst_dim].s.start
	      - GFC_DIMENSION_LBOUND (dst->dim[dst_dim]);
	  src_index[src_dim] = 0;
	  for (index_type idx = 0; idx < extent_dst; ++idx)
	    {
	      send_by_ref (ref, i, src_index, single_token, dst, src,
			   ds + array_offset_dst * ref->item_size
			   * GFC_DIMENSION_STRIDE (src->dim[src_dim]), sr,
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, size, stat);
	      src_index[src_dim] +=
		  GFC_DIMENSION_STRIDE (src->dim[src_dim]);
	      array_offset_dst += ref->u.a.dim[dst_dim].s.stride;
	    }
	  return;
	case CAF_ARR_REF_OPEN_START:
	  extent_dst = (ref->u.a.dim[dst_dim].s.end
			- GFC_DIMENSION_LBOUND (dst->dim[dst_dim]))
	      / ref->u.a.dim[dst_dim].s.stride + 1;
	  array_offset_dst = 0;
	  src_index[src_dim] = 0;
	  for (index_type idx = 0; idx < extent_dst; ++idx)
	    {
	      send_by_ref (ref, i, src_index, single_token, dst, src,
			   ds + array_offset_dst * ref->item_size
			   * GFC_DIMENSION_STRIDE (src->dim[src_dim]), sr,
			   dst_kind, src_kind, dst_dim + 1, src_dim + 1,
			   1, size, stat);
	      src_index[src_dim] +=
		  GFC_DIMENSION_STRIDE (src->dim[src_dim]);
	      array_offset_dst += ref->u.a.dim[dst_dim].s.stride;
	    }
	  return;
	default:
	  caf_runtime_error(unreachable);
	}
      return;
    default:
      caf_runtime_error(unreachable);
    }
}


void
_gfortran_caf_send_by_ref (caf_token_t token,
			   int image_index __attribute__ ((unused)),
			   gfc_descriptor_t *src, caf_reference_t *refs,
			   int dst_kind, int src_kind, bool may_require_tmp,
			   bool dst_reallocatable, int *stat)
{
  const char compidxoutofrange[] = "libcaf_single::caf_send_by_ref(): "
				   "component index out of range.\n";
  const char unknownreftype[] = "libcaf_single::caf_send_by_ref(): "
				"unknown reference type.\n";
  const char unknownarrreftype[] = "libcaf_single::caf_send_by_ref(): "
				   "unknown array reference type.\n";
  const char rankoutofrange[] = "libcaf_single::caf_send_by_ref(): "
				"rank out of range.\n";
  const char realloconinnerref[] = "libcaf_single::caf_send_by_ref(): "
      "reallocation of array followed by component ref not allowed.\n";
  const char cannotallocdst[] = "libcaf_single::caf_send_by_ref(): "
				"can not allocate memory.\n";
  const char nonallocextentmismatch[] = "libcaf_single::caf_send_by_ref(): "
      "extent of non-allocatable array mismatch.\n";
  const char innercompref[] = "libcaf_single::caf_send_by_ref(): "
      "inner unallocated component detected.\n";
  size_t size, i;
  size_t dst_index[GFC_MAX_DIMENSIONS];
  int src_rank = GFC_DESCRIPTOR_RANK (src);
  int src_cur_dim = 0;
  size_t src_size;
  caf_single_token_t single_token = TOKEN (token);
  void *memptr = single_token->memptr;
  gfc_descriptor_t *dst = single_token->desc;
  caf_reference_t *riter = refs;
  long delta;
  /* Reallocation of dst.data is needed (e.g., array to small).  */
  bool realloc_needed;
  /* Note that the component is not allocated yet.  */
  index_type new_component_idx = -1;

  if (stat)
    *stat = 0;

  /* The first ref has to be a component ref.  */
  if (refs->type != CAF_REF_COMPONENT)
    {
      const char firstneedstobecompref[] = "libcaf_single::caf_send_by_ref(): "
	"first ref needs to be a component ref.\n";
      caf_internal_error (firstneedstobecompref, sizeof (firstneedstobecompref),
			  stat, NULL, 0);
      return;
    }

  /* Compute the size of the result.  In the beginning size just counts the
     number of elements.  */
  size = 1;
  while (riter)
    {
      switch (riter->type)
	{
	case CAF_REF_COMPONENT:
	  if (unlikely (new_component_idx != -1))
	    {
	      /* Allocating a component in the middle of a component ref is not
		 support.  We don't know the type to allocate.  */
	      caf_internal_error (innercompref, sizeof (innercompref), stat,
				  NULL, 0);
	      return;
	    }
	  if (riter->u.c.idx >= 0)
	    {
	      /* Dynamically allocated component.  */
	      if (unlikely (single_token->num_comps < riter->u.c.idx))
		{
		  caf_internal_error (compidxoutofrange,
				      sizeof (compidxoutofrange),
				      stat, NULL, 0);
		  return;
		}
	      if (single_token->components[riter->u.c.idx] == NULL)
		{
		  /* This component is not yet allocated.  Check that it is
		     allocatable here.  */
		  if (!dst_reallocatable)
		    {
		      caf_internal_error (cannotallocdst,
					  sizeof (cannotallocdst),
					  stat, NULL, 0);
		      return;
		    }
		  single_token = NULL;
		  memptr = NULL;
		  dst = NULL;
		  break;
		}
	      single_token = single_token->components[riter->u.c.idx];
	      memptr = single_token->memptr;
	      dst = single_token->desc;
	    }
	  else
	    {
	      /* Regular component.  */
	      memptr += riter->u.c.offset;
	      dst = (gfc_descriptor_t *)memptr;
	    }
	  break;
	case CAF_REF_ARRAY:
	  /* When the dst array needs to be allocated, then look at the
	     extent of the source array in the dimension dst_cur_dim.  */
	  for (i = 0; riter->u.a.mode[i] != CAF_ARR_REF_NONE; ++i)
	    {
	      switch (riter->u.a.mode[i])
		{
		case CAF_ARR_REF_VECTOR:
		  delta = riter->u.a.dim[i].v.nvec;
		  break;
		case CAF_ARR_REF_FULL:
		  if (dst)
		    delta = GFC_DIMENSION_EXTENT (dst->dim[i]);
		  else
		    delta = GFC_DIMENSION_EXTENT (src->dim[src_cur_dim]);
		  break;
		case CAF_ARR_REF_RANGE:
		  delta = (riter->u.a.dim[i].s.end - riter->u.a.dim[i].s.start)
		      / riter->u.a.dim[i].s.stride + 1;
		  break;
		case CAF_ARR_REF_SINGLE:
		  delta = 1;
		  break;
		case CAF_ARR_REF_OPEN_END:
		  if (dst)
		    delta = (dst->dim[i]._ubound - riter->u.a.dim[i].s.start)
		        / riter->u.a.dim[i].s.stride + 1;
		  else
		    delta = (src->dim[src_cur_dim]._ubound
			     - riter->u.a.dim[i].s.start)
			/ riter->u.a.dim[i].s.stride + 1;
		  break;
		case CAF_ARR_REF_OPEN_START:
		  if (dst)
		    delta = (riter->u.a.dim[i].s.end - dst->dim[i].lower_bound)
			/ riter->u.a.dim[i].s.stride + 1;
		  else
		    delta = (riter->u.a.dim[i].s.end
			     - src->dim[src_cur_dim].lower_bound)
			/ riter->u.a.dim[i].s.stride + 1;
		  break;
		default:
		  caf_internal_error (unknownarrreftype,
				      sizeof (unknownarrreftype), stat,
				      NULL, 0);
		  return;
		}

	      if (delta <= 0)
		return;
	      if (delta != 1)
		{
		  /* Non-scalar dimension.  */
		  if (unlikely (src_cur_dim >= src_rank))
		    {
		      caf_internal_error (rankoutofrange,
					  sizeof (rankoutofrange),
					  stat, NULL, 0);
		      return;
		    }
		  if (dst && GFC_DESCRIPTOR_EXTENT (dst, src_cur_dim) != delta)
		    {
		      if (unlikely (!dst_reallocatable))
			{
			  caf_internal_error (nonallocextentmismatch,
					      sizeof (nonallocextentmismatch),
					      stat, NULL, 0);
			}
		      else if (riter->next != NULL)
			{
			  caf_internal_error (realloconinnerref,
					      sizeof (realloconinnerref),
					      stat, NULL, 0);
			  return;
			}
		      realloc_needed = true;
		      GFC_DIMENSION_SET (dst->dim[src_cur_dim], 1, delta, size);
		    }
		  else if (!dst)
		    {
		      if (!dst_reallocatable)
			{
			  caf_internal_error (cannotallocdst,
					      sizeof (cannotallocdst),
					      stat, NULL, 0);
			  return;
			}
		      realloc_needed = true;
		    }
		  ++src_cur_dim;
		}
	      size *= (index_type)delta;
	    }
	  /* For now take the first entry from the array to proceed in size
	     calculation.
	     TODO: Take the actually first reffed item of the array, because
		   the first entry in the array may point to unallocated
		   data.  */
	  memptr = dst ? GFC_DESCRIPTOR_DATA (dst): NULL;
	  break;
	default:
	  caf_internal_error (unknownreftype, sizeof (unknownreftype), stat,
			      NULL, 0);
	  return;
	}
      src_size = riter->item_size;
      riter = riter->next;
    }
  if (size == 0 || src_size == 0)
    return;
  /* Postcondition:
     - size contains the number of elements to store in the destination array,
     - src_size gives the size in bytes of each item in the destination array.
  */

//  if (realloc_needed)
//    {
//      /* TODO: dst_dim > 1 */
//      GFC_DESCRIPTOR_DATA (dst) = malloc (size * GFC_DESCRIPTOR_SIZE (dst));
//      if (unlikely (GFC_DESCRIPTOR_DATA (dst) == NULL))
//	{
//	  caf_internal_error (cannotallocdst, sizeof (cannotallocdst), stat,
//			      NULL, 0);
//	  return;
//	}
//    }

  /* Reset the token.  */
  single_token = TOKEN (token);
  memptr = single_token->memptr;
  dst = single_token->desc;
  memset(dst_index, 0, sizeof (dst_index));
  i = 0;
  while (i < size)
    {
      send_by_ref (refs, &i, dst_index, single_token, dst, src,
		   memptr, GFC_DESCRIPTOR_DATA (src), dst_kind, src_kind, 0, 0,
		   1, size, stat);
    }
}


void
_gfortran_caf_sendget_by_ref (caf_token_t dst_token, int dst_image_index,
			      caf_reference_t *dst_refs, caf_token_t src_token,
			      int src_image_index,
			      caf_reference_t *src_refs, int dst_kind,
			      int src_kind, bool may_require_tmp, int *stat)
{
  gfc_array_void temp;

  _gfortran_caf_get_by_ref (src_token, src_image_index, &temp, src_refs,
			    dst_kind, src_kind, may_require_tmp, true, stat);

  _gfortran_caf_send_by_ref (dst_token, dst_image_index, &temp, dst_refs,
			     dst_kind, src_kind, may_require_tmp, true, stat);
}


void
_gfortran_caf_atomic_define (caf_token_t token, size_t offset,
			     int image_index __attribute__ ((unused)),
			     void *value, int *stat,
			     int type __attribute__ ((unused)), int kind)
{
  assert(kind == 4);

  uint32_t *atom = (uint32_t *) ((char *) MEMTOK (token) + offset);

  __atomic_store (atom, (uint32_t *) value, __ATOMIC_RELAXED);

  if (stat)
    *stat = 0;
}

void
_gfortran_caf_atomic_ref (caf_token_t token, size_t offset,
			  int image_index __attribute__ ((unused)),
			  void *value, int *stat,
			  int type __attribute__ ((unused)), int kind)
{
  assert(kind == 4);

  uint32_t *atom = (uint32_t *) ((char *) MEMTOK (token) + offset);

  __atomic_load (atom, (uint32_t *) value, __ATOMIC_RELAXED);

  if (stat)
    *stat = 0;
}


void
_gfortran_caf_atomic_cas (caf_token_t token, size_t offset,
			  int image_index __attribute__ ((unused)),
			  void *old, void *compare, void *new_val, int *stat,
			  int type __attribute__ ((unused)), int kind)
{
  assert(kind == 4);

  uint32_t *atom = (uint32_t *) ((char *) MEMTOK (token) + offset);

  *(uint32_t *) old = *(uint32_t *) compare;
  (void) __atomic_compare_exchange_n (atom, (uint32_t *) old,
				      *(uint32_t *) new_val, false,
				      __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  if (stat)
    *stat = 0;
}


void
_gfortran_caf_atomic_op (int op, caf_token_t token, size_t offset,
			 int image_index __attribute__ ((unused)),
			 void *value, void *old, int *stat,
			 int type __attribute__ ((unused)), int kind)
{
  assert(kind == 4);

  uint32_t res;
  uint32_t *atom = (uint32_t *) ((char *) MEMTOK (token) + offset);

  switch (op)
    {
    case GFC_CAF_ATOMIC_ADD:
      res = __atomic_fetch_add (atom, *(uint32_t *) value, __ATOMIC_RELAXED);
      break;
    case GFC_CAF_ATOMIC_AND:
      res = __atomic_fetch_and (atom, *(uint32_t *) value, __ATOMIC_RELAXED);
      break;
    case GFC_CAF_ATOMIC_OR:
      res = __atomic_fetch_or (atom, *(uint32_t *) value, __ATOMIC_RELAXED);
      break;
    case GFC_CAF_ATOMIC_XOR:
      res = __atomic_fetch_xor (atom, *(uint32_t *) value, __ATOMIC_RELAXED);
      break;
    default:
      __builtin_unreachable();
    }

  if (old)
    *(uint32_t *) old = res;

  if (stat)
    *stat = 0;
}

void
_gfortran_caf_event_post (caf_token_t token, size_t index, 
			  int image_index __attribute__ ((unused)), 
			  int *stat, char *errmsg __attribute__ ((unused)), 
			  int errmsg_len __attribute__ ((unused)))
{
  uint32_t value = 1;
  uint32_t *event = (uint32_t *) ((char *) MEMTOK (token) + index*sizeof(uint32_t));
  __atomic_fetch_add (event, (uint32_t) value, __ATOMIC_RELAXED);
  
  if(stat)
    *stat = 0;
}

void
_gfortran_caf_event_wait (caf_token_t token, size_t index, 
			  int until_count, int *stat,
			  char *errmsg __attribute__ ((unused)), 
			  int errmsg_len __attribute__ ((unused)))
{
  uint32_t *event = (uint32_t *) ((char *) MEMTOK (token) + index*sizeof(uint32_t));
  uint32_t value = (uint32_t)-until_count;
   __atomic_fetch_add (event, (uint32_t) value, __ATOMIC_RELAXED);
  
   if(stat)
    *stat = 0;    
}

void
_gfortran_caf_event_query (caf_token_t token, size_t index, 
			   int image_index __attribute__ ((unused)), 
			   int *count, int *stat)
{
  uint32_t *event = (uint32_t *) ((char *) MEMTOK (token) + index*sizeof(uint32_t));
  __atomic_load (event, (uint32_t *) count, __ATOMIC_RELAXED);
  
  if(stat)
    *stat = 0;
}

void
_gfortran_caf_lock (caf_token_t token, size_t index,
		    int image_index __attribute__ ((unused)),
		    int *aquired_lock, int *stat, char *errmsg, int errmsg_len)
{
  const char *msg = "Already locked";
  bool *lock = &((bool *) MEMTOK (token))[index];

  if (!*lock)
    {
      *lock = true;
      if (aquired_lock)
	*aquired_lock = (int) true;
      if (stat)
	*stat = 0;
      return;
    }

  if (aquired_lock)
    {
      *aquired_lock = (int) false;
      if (stat)
	*stat = 0;
    return;
    }


  if (stat)
    {
      *stat = 1;
      if (errmsg_len > 0)
	{
	  int len = ((int) sizeof (msg) > errmsg_len) ? errmsg_len
						      : (int) sizeof (msg);
	  memcpy (errmsg, msg, len);
	  if (errmsg_len > len)
	    memset (&errmsg[len], ' ', errmsg_len-len);
	}
      return;
    }
  _gfortran_caf_error_stop_str (msg, (int32_t) strlen (msg));
}


void
_gfortran_caf_unlock (caf_token_t token, size_t index,
		      int image_index __attribute__ ((unused)),
		      int *stat, char *errmsg, int errmsg_len)
{
  const char *msg = "Variable is not locked";
  bool *lock = &((bool *) MEMTOK (token))[index];

  if (*lock)
    {
      *lock = false;
      if (stat)
	*stat = 0;
      return;
    }

  if (stat)
    {
      *stat = 1;
      if (errmsg_len > 0)
	{
	  int len = ((int) sizeof (msg) > errmsg_len) ? errmsg_len
						      : (int) sizeof (msg);
	  memcpy (errmsg, msg, len);
	  if (errmsg_len > len)
	    memset (&errmsg[len], ' ', errmsg_len-len);
	}
      return;
    }
  _gfortran_caf_error_stop_str (msg, (int32_t) strlen (msg));
}
