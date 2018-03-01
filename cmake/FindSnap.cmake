# - Find the Snap (Stanford Network Analysis Platform) Package
# Will define
#  Snap_FOUND        - Snap was found on the system
#  Snap_INCLUDE_DIRS - The Snap include directories
#  Snap_LIBRARIES    - The libraries needed to use Snap
#  Snap_DEFINITIONS  - Compiler switches required for using Snap

find_path(Snap_ROOT_DIR
        NAMES "snap-core" "glib-core"
        HINTS $ENV{WORKSPACE_ROOT}/opt/Snap $ENV{WORKSPACE_ROOT}/afs/cs.stanford.edu/u/jdeaton/repos/snap
        DOC "Snap root directory")

find_path(Snap_CORE
        NAMES "Snap.h"
        PATH_SUFFIXES snap-core
        HINTS ${Snap_ROOT_DIR}
        DOC "The Snap include directory")

find_path(Snap_GLIB_CORE
        NAMES "base.h"
        PATH_SUFFIXES glib-core
        HINTS ${Snap_ROOT_DIR}
        DOC "The Snap GLib include directory")

find_library(Snap_LIBRARY
        NAMES libsnap.a
        HINTS ${Snap_ROOT_DIR}/snap-core
        DOC "The Snap library")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snap DEFAULT_MSG
        Snap_ROOT_DIR
        Snap_CORE
        Snap_GLIB_CORE
        Snap_LIBRARY)

if (Snap_FOUND)
    set(Snap_LIBRARIES ${Snap_LIBRARY})
    set(Snap_INCLUDE_DIRS ${Snap_CORE} ${Snap_GLIB_CORE})
    set(Snap_DEFINITIONS)
else()
    message(FATAL_ERROR "Package: Snap not found")
endif (Snap_FOUND)

# Tell cmake GUIs to ignore the "local" variables.
mark_as_advanced(Snap_ROOT_DIR Snap_INCLUDE_DIR Snap_LIBRARY)
