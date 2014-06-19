iquestFuse
==========

A Filesystem in USErspace (FUSE) filesystem to view data
objects in an iRODS zone based on queries, in a manner similar to that
provided by the iRODS i-command "iquest" (question (query)) utility.


Usage
-----

```bash
# mount
iquest_fuse ... mountpoint

# unmount
fusermount -u mountpoint 
```

Compiling
---------

This directory should be placed within the source tree of an iRODS 
source distribution in order to be compiled. The current version was 
developed against iRODS 3.1.


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

