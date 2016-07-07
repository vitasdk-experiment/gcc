! { dg-do run }
! { dg-options "-fcoarray=lib -lcaf_single -fdump-tree-original" }

program alloc_comp
  implicit none

  type coords
    real,allocatable :: x(:)
    real,allocatable :: y(:)
    real,allocatable :: z(:)
  end type

  integer :: me,np,n,i
  type(coords) :: coo[*]

  me = this_image(); np = num_images()
  n = 100

  allocate(coo%x(n),coo%y(n),coo%z(n))

  coo%y = me

  do i=1, n
        coo%y(i) = coo%y(i) + i
  end do

  sync all

  if(me == 1 .and. coo[np]%y(10) /= 11 ) call abort()

  deallocate(coo%x)

end program
