/* { dg-do compile { target { arm_thumb2 || arm_thumb1_movt_ok } } } */
/* { dg-options "-O2" } */

long long
movdi (int a)
{
  return 0xF0F0;
}

/* { dg-final { scan-assembler-times "movw\tr0, #61680" 1 } } */
