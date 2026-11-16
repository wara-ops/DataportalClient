# This file is added to the src/dataportal directory as a pre_build and pre_test hook.
#
# It holds ONLY the API version the client is compatible with, because that is a
# deliberate, hand-maintained decision. The client's own version is not here: it is
# derived from the git tag at build time (see [tool.pdm.version] in pyproject.toml)
# and read at runtime from the installed distribution metadata as dataportal.__version__.

# The version that the dataportal API must be compatible with, i.e. within same major and larger minor versions.
# The system tests uses this version for the test portal.
__api_version__ = 'v1.3.2'
