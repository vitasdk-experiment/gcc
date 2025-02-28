! { dg-do run }
! { dg-options "-fcoarray=lib -lcaf_single" }
!
! Contributed by Reinhold Bader
!

program pmup
  implicit none
  type t
    integer :: b, a
  end type t

  CLASS(*), allocatable :: a(:)[:]
  integer :: ii

  !! --- ONE --- 
  allocate(real :: a(3)[*])
  IF (this_image() == num_images()) THEN
    SELECT TYPE (a)
      TYPE IS (real)
      a(:)[1] = 2.0
    END SELECT
  END IF
  SYNC ALL

  IF (this_image() == 1) THEN
    SELECT TYPE (a)
      TYPE IS (real)
        IF (ALL(A(:)[1] == 2.0)) THEN
          !WRITE(*,*) 'OK'
        ELSE
          WRITE(*,*) 'FAIL'
          call abort()
        END IF
      TYPE IS (t)
        ii = a(1)[1]%a
        call abort()
      CLASS IS (t)
        ii = a(1)[1]%a
        call abort()
    END SELECT
  END IF

  !! --- TWO --- 
  deallocate(a)
  allocate(t :: a(3)[*])
  IF (this_image() == num_images()) THEN
    SELECT TYPE (a)
      TYPE IS (t)
      a(:)[1]%a = 4.0
    END SELECT
  END IF
  SYNC ALL

  IF (this_image() == 1) THEN
    SELECT TYPE (a)
   TYPE IS (real)
      ii = a(1)[1]
      call abort()
    TYPE IS (t)
      IF (ALL(A(:)[1]%a == 4.0)) THEN
        !WRITE(*,*) 'OK'
      ELSE
        WRITE(*,*) 'FAIL'
        call abort()
      END IF
    CLASS IS (t)
      ii = a(1)[1]%a
      call abort()
    END SELECT
  END IF
end program
