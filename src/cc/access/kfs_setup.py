#
# $Id: kfs_setup.py 1552 2011-01-06 22:21:54Z sriramr $
#
# Use the distutils setup function to build and install the KFS module.
# Execute this as:
#  python kfs_setup.py ~/code/kfs/build/lib/ build
# and this will build kfs.so in ./build/.../kfs.so
# This needs to be installed /usr/lib64/python/site-packages or in an
# alternate location; see COMPILING for instructions
# In addition, ~/code/kfs/build/lib needs to be in the LD_LIBRARY_PATH
# After installation, python apps can access kfs.
#
from distutils.core import setup, Extension
import sys

kfs_lib_dir = sys.argv[1]
del sys.argv[1]

kfsext = Extension('kfs',
		include_dirs = ['/home/srao/p4trees/kfssort/kosmosfs/src/cc/', '/usr/local/include/boost-1_37/'],
		libraries = ['kfsClient'],
		library_dirs = [kfs_lib_dir],
        runtime_library_dirs = ['/home/qmr_ksort/sortmaster/lib'],
		sources = ['KfsModulePy.cc'])

setup(name = "kfs", version = "0.3",
	description="KFS client module",
	author="Blake Lewis and Sriram Rao",
	ext_modules = [kfsext])
