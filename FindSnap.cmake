# - Find the Snap (Stanford Network Analysis Platform) Package
# Will define
#  Snap_FOUND        - Snap was found on the system
#  Snap_INCLUDE_DIRS - The Snap include directories
#  Snap_LIBRARIES    - The libraries needed to use Snap
#  Snap_DEFINITIONS  - Compiler switches required for using Snap

set (Snap_ROOT_DIR $ENV{WORKSPACE_ROOT}/opt/Snap)
MESSAGE(STATUS "Snap_ROOT_DIR: " ${Snap_ROOT_DIR}) # print which directory CMake is looking in.

find_path(Snap_INCLUDE_DIR
        NAMES snap-core snap-adv snap-exp
        PATHS ${Snap_ROOT_DIR}/snap-core ${Snap_ROOT_DIR}/snap-adv ${Snap_ROOT_DIR}/snap-exp
        DOC "The Snap include directory")

find_library(Snap_LIBRARY
        NAMES Snap
        PATHS ${Snap_ROOT_DIR}/snap-core/libsnap.a
        DOC "The Snap library")

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LOGGING_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(Snap DEFAULT_MSG Snap_INCLUDE_DIR Snap_LIBRARY)

if (Snap_FOUND)
    set(Snap_LIBRARIES ${Snap_LIBRARY})
    set(Snap_INCLUDE_DIRS ${Snap_ROOT_DIR}/snap-core)
    set(Snap_DEFINITIONS)
endif()

# Tell cmake GUIs to ignore the "local" variables.
mark_as_advanced(Snap_ROOT_DIR Snap_INCLUDE_DIR Snap_LIBRARY)