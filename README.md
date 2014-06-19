iquestFuse
==========

A Filesystem in USErspace (FUSE) filesystem to view data
objects in an iRODS zone based on queries, in a manner similar to that
provided by the iRODS i-command "iquest" (question (query)) utility.


Usage
-----

```
# mount
mkdir mountpoint
iquest_fuse mountpoint ...

# unmount
fusermount -u mountpoint 
```

To debug, you can mount with the iquestFuse client in the foreground using the `-d` option. 
```
iquest_fuse -d mountpoint ...
```


Prerequisites
-------------
iRODS - tested with 3.1

FUSE (http://fuse.sourceforge.net/) - tested with 2.9.3

Compiling
---------

This directory should be placed within the source tree of an iRODS 
source distribution which has been configured and compiled. 

iquestFuse should then be compiled, passing the location of a FUSE 
installation as the fuseHomeDir variable. 

```
# configure and compile iRODS
cd iRODS
./scripts/configure
make
# checkout iquestFuse under clients/
cd clients
git clone https://github.com/wtsi-hgi/iquestFuse.git
# compile iquestFuse
cd iquestFuse
make fuseHomeDir=/path/to/fuse-2.9.3
```


Notes
-----

1. The usage of a range of years within a copyright statement
contained within this distribution should be interpreted as being
equivalent to a list of years including the first and last year
specified and all consecutive years between them. For example, a
copyright statement that reads 'Copyright (c) 2005, 2007- 2009,
2011-2012' should be interpreted as being identical to a statement
that reads 'Copyright (c) 2005, 2007, 2008, 2009, 2011, 2012' and a
copyright statement that reads "Copyright (c) 2005-2012' should be
interpreted as being identical to a statement that reads 'Copyright
(c) 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012'."

