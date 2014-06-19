/*****************************************************************************
 * Copyright (c) 2012 Genome Research Ltd. 
 *
 * Author: Joshua C. Randall <jcrandall@alum.mit.edu>
 *
 * This file is part of iquestFuse.
 *
 * iquestFuse is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/

/*****************************************************************************
 *** Portions of This file are substantially based on iFuseOper.h which is ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*****************************************************************************
 * iquestFuseOperations.h
 * 
 * Declarations of FUSE filesystem operations for iquestFuse.
 *****************************************************************************/
#ifndef IQUEST_FUSE_OPERATIONS_H
#define IQUEST_FUSE_OPERATIONS_H

#include <sys/statvfs.h>

//#include "rodsClient.h"
void *iquest_fuse_init(struct fuse_conn_info *conn);
void iquest_fuse_destroy(void *data);
int iquest_fuse_getattr(const char *path, struct stat *stbuf);
int iquest_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int iquest_fuse_open(const char *path, struct fuse_file_info *fi);
int iquest_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);



int irodsGetattr(const char *path, struct stat *stbuf);
int irodsReadlink(const char *path, char *buf, size_t size);
int irodsGetdir (const char *, char *, size_t);	/* Deprecated */
int irodsMknod(const char *path, mode_t mode, dev_t rdev);
int irodsMkdir(const char *path, mode_t mode);
int irodsUnlink(const char *path);
int irodsRmdir(const char *path);
int irodsSymlink(const char *from, const char *to);
int irodsRename(const char *from, const char *to);
int irodsLink(const char *from, const char *to);
int irodsChmod(const char *path, mode_t mode);
int irodsChown(const char *path, uid_t uid, gid_t gid);
int irodsTruncate(const char *path, off_t size);
int irodsFlush(const char *path, struct fuse_file_info *fi);
int irodsUtimens (const char *path, const struct timespec ts[]);
int irodsOpen(const char *path, struct fuse_file_info *fi);
int irodsRead(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int irodsWrite(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int irodsStatfs(const char *path, struct statvfs *stbuf);
int irodsRelease(const char *path, struct fuse_file_info *fi);
int irodsFsync (const char *path, int isdatasync, struct fuse_file_info *fi);
int irodsOpendir (const char *, struct fuse_file_info *);
int irodsReaddir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);


#endif	/* IQUEST_FUSE_OPERATIONS_H */
