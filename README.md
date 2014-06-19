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
iquest_fuse mountpoint

# unmount
fusermount -u mountpoint 
```

To debug, you can mount with the iquestFuse client in the foreground using the `-d` option. 
```
iquest_fuse -d mountpoint
```

Make sure you have authenticated to iRODS (e.g. using iinit or kinit) before running `iquest_fuse` 
or you will get an error. 

Also make sure you have a working iRODS user environment file, either in `~/.irodsEnv` or in a file referenced by the `irodsEnvFile` environment variable. 

If you run iquest_fuse as above with no other options, it will mount the collection specified as irodsHome in your environment file. To mount with a different base directory, use the `-c` option. 

For example, to mount the root collection, use:
```
iquest_fuse mountpoint -c /
```

After mounting a collection, you should be able to access the collection as you would with the standard irodsFs client. In addition, in each collection there is a special virtual directory created (by default named `Q`, but you can change this using the `-i` option to `iquest_fuse`) which you can use to perform a metadata search. This directory will *not* appear in directory listings by default (because it would be a disaster if one were to run a directory traversal (such as `find`) on it. 

The virtual `Q` directory contains a list of all available metadata keys (including attributes and user AVUs), which are also indicated as virtual directories. Inside those virtual directories is another set of directories which are the available values. Inside each of those directories you should find all of the data objects and collections that match the query. 


Prerequisites
-------------
iRODS - tested and working with 3.1

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

