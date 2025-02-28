/* { dg-do compile { target { powerpc*-*-* } } } */
/* { dg-skip-if "do not override -mcpu" { powerpc*-*-* } { "-mcpu=*" } { "-mcpu=power9" } } */
/* { dg-require-effective-target powerpc_p9vector_ok } */
/* { dg-options "-mcpu=power9" } */

#include <altivec.h>

__vector int
get_data_class_flags (__vector float *p, unsigned int condition_flag)
{
  __vector float source = *p;

  return vec_test_data_class (source, condition_flag); /* { dg-error "argument 2 must be a 7-bit unsigned literal" } */
}
