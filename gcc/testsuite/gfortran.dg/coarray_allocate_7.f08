! { dg-do run }
! { dg-options "-fcoarray=lib -lcaf_single -fdump-tree-original" }

! Contributed by Damian Rouson

program main

  implicit none

  type mytype
    integer, allocatable :: indices(:)
  end type

  type(mytype), save :: object[*]
  integer :: i,me

  me=this_image()
  object%indices=[(i,i=1,me)]
  if ( size(object%indices) /= 1 ) call abort()
  if ( object%indices(1) /= 1 ) call abort()
end program

! { dg-final { scan-tree-dump-times "_gfortran_caf_register_component \\(caf_token.0, 1, D.\[0-9\]{4}, 0, &\\(\\(struct mytype\\) \\*object\\).indices.data, 0B, 0B, 0\\);" 2 "original" } }
! { dg-final { scan-tree-dump-times "_gfortran_caf_deregister_component \\(caf_token.0, 0, &\\(\\(struct mytype\\) \\*object\\).indices.data, 0B, 0B, 0\\);" 2 "original" } }

