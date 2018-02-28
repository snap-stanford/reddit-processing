# - Find the Snap (Stanford Network Analysis Platform) Package
# Will define
#  SNAP_FOUND        - Snap was found on the system
#  SNAP_INCLUDE_DIRS - The Snap include directories
#  SNAP_LIBRARIES    - The libraries needed to use Snap
#  SNAP_DEFINITIONS  - Compiler switches required for using Snap
include(FindPackageHandleStandardArgs)

set(SNAP_ROOT_DIR $ENV{WORKSPACE_ROOT}/opt/Snap)
message(STATUS "SNAP_ROOT_DIR: " ${SNAP_ROOT_DIR}) # print which directory CMake is looking in.

find_path(SNAP_CORE
        NAMES "Snap.h"
        PATH_SUFFIXES snap-core
        HINTS ${SNAP_ROOT_DIR}
        DOC "The Snap include directory")
message(STATUS "Snap core: " ${SNAP_CORE})

find_path(SNAP_GLIB_CORE
        NAMES "base.h"
        PATH_SUFFIXES glib-core
        HINTS ${SNAP_ROOT_DIR}
        DOC "The Snap GLib include directory")
message(STATUS "Glib core: " ${SNAP_GLIB_CORE})

find_library(SNAP_LIBRARY
        NAMES libsnap.a
        PATHS ${SNAP_ROOT_DIR}/snap-core
        DOC "The Snap library")
message(STATUS "Snap Library: " ${SNAP_LIBRARY})

find_package_handle_standard_args(SNAP DEFAULT_MSG
        SNAP_CORE
        SNAP_GLIB_CORE
        SNAP_LIBRARY)

if (SNAP_FOUND)
    set(SNAP_LIBRARIES ${SNAP_LIBRARY})
    set(SNAP_INCLUDE_DIRS ${SNAP_CORE} ${SNAP_GLIB_CORE})
    set(SNAP_DEFINITIONS)
endif()

# Tell cmake GUIs to ignore the "local" variables.
mark_as_advanced(SNAP_ROOT_DIR SNAP_INCLUDE_DIR SNAP_LIBRARY)