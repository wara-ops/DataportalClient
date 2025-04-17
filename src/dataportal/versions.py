# This file is added to the src/dataportal directory as a pre_build and pre_test hook

# The version of the dataportal client. If a git tag is provided, it should match this version.
__version__ = 'v1.0.4'

# The version that the dataportal API must be compatible with, i.e. within same major and larger minor versions.
# The system tests uses this version for the test portal.
__api_version__ = 'v1.0.0'