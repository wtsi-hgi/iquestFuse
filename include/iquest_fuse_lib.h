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
 *** Portions of This file are substantially based on iFuseLib.h which is  ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*****************************************************************************
 *** Portions of This file are substantially based on iFuseOper.h which is ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*****************************************************************************
 * Declarations of library/helper functions for iquestFuse.
 *****************************************************************************/
#ifndef IQUEST_FUSE_HELPER_H
#define IQUEST_FUSE_HELPER_H

#include <sys/statvfs.h>

#include "rodsClient.h"
#include "rodsPath.h"

#define CACHE_FUSE_PATH         1
#ifdef CACHE_FUSE_PATH
#define CACHE_FILE_FOR_READ     1
#define CACHE_FILE_FOR_NEWLY_CREATED     1
#endif

#define MAX_BUF_CACHE   2
#define MAX_IFUSE_DESC   512
#define MAX_READ_CACHE_SIZE   (1024*1024)	/* 1 mb */
#define MAX_NEWLY_CREATED_CACHE_SIZE   (4*1024*1024)	/* 4 mb */
#define HIGH_NUM_CONN	5	/* high water mark */
#define MAX_NUM_CONN	10

#define NUM_NEWLY_CREATED_SLOT	5
#define MAX_NEWLY_CREATED_TIME	5	/* in sec */

#define FUSE_CACHE_DIR	"/tmp/fuseCache"

#define IRODS_FREE		0
#define IRODS_INUSE	1 


typedef struct BufCache {
    rodsLong_t beginOffset;
    rodsLong_t endOffset;
    void *buf;
} bufCache_t;

typedef enum { 
    NO_FILE_CACHE,
    HAVE_READ_CACHE,
    HAVE_NEWLY_CREATED_CACHE,
} readCacheState_t;

typedef struct ConnReqWait {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int state;
    struct ConnReqWait *next;
} connReqWait_t;

typedef struct IFuseDesc {
  iquest_fuse_irods_conn_t *irods_conn;    
  bufCache_t  bufCache[MAX_BUF_CACHE];
  int actCacheInx;    /* (cacheInx + 1) currently active. 0 means no cache */
  int inuseFlag;      /* 0 means not in use */
  int iFd;    /* irods client fd */
  int newFlag;
  int createMode;
  rodsLong_t offset;
  rodsLong_t bytesWritten;
  char *objPath;
  char *localPath;
  readCacheState_t locCacheState;
  pthread_mutex_t lock;
} iFuseDesc_t;

#define NUM_PATH_HASH_SLOT	201
#define CACHE_EXPIRE_TIME	600	/* 10 minutes before expiration */

typedef struct PathCache {
    char* filePath;
    char* locCachePath;
    struct stat stbuf;
    uint cachedTime;
    struct PathCache *prev;
    struct PathCache *next;
    void *pathCacheQue;
    readCacheState_t locCacheState;
} pathCache_t;

typedef struct PathCacheQue {
    pathCache_t *top;
    pathCache_t *bottom;
} pathCacheQue_t;

typedef struct specialPath {
    char *path;
    int len;
} specialPath_t;

typedef struct newlyCreatedFile {
    int descInx;
    int inuseFlag;      /* 0 means not in use */
    char filePath[MAX_NAME_LEN];
    struct stat stbuf;
    uint cachedTime;
} newlyCreatedFile_t;


int iquestParseRodsPathStr (char *inPath, char *outPath);

int
initIFuseDesc ();
int
allocIFuseDesc ();
int
lockDesc (int descInx);
int
unlockDesc (int descInx);
int
irods_connInuse (iquest_fuse_irods_conn_t *irods_conn);
int
freeIFuseDesc (int descInx);
int get_iquest_fuse_irods_conn_by_path(iquest_fuse_irods_conn_t **irods_conn, iquest_fuse_t *iqf, char *localPath);
int
fillIFuseDesc (int descInx, iquest_fuse_irods_conn_t *irods_conn, int iFd, char *objPath,
char *localPath);
int
ifuseWrite (char *path, int descInx, char *buf, size_t size,
off_t offset);
int
ifuseRead (char *path, int descInx, char *buf, size_t size, 
off_t offset);
int
ifuseLseek (char *path, int descInx, off_t offset);
 int get_iquest_fuse_irods_conn(iquest_fuse_irods_conn_t **irods_conn, iquest_fuse_t *iqf);
int
useIFuseConn (iquest_fuse_irods_conn_t *irods_conn);
int
useFreeIFuseConn (iquest_fuse_irods_conn_t *irods_conn);
int
_useIFuseConn (iquest_fuse_irods_conn_t *irods_conn);
int
unuseIFuseConn (iquest_fuse_irods_conn_t *irods_conn);
int
relIFuseConn (iquest_fuse_irods_conn_t *irods_conn);
int
_relIFuseConn (iquest_fuse_irods_conn_t *irods_conn);

void conn_manager(iquest_fuse_t *iqf);
int disconnect_all (iquest_fuse_t *iqf);
int signal_conn_manager(iquest_fuse_t *iqf);
int get_conn_count(iquest_fuse_t *iqf);

int
iFuseDescInuse ();
int
checkFuseDesc (int descInx);
int
initPathCache ();
int
getHashSlot (int value, int numHashSlot);
int
matchPathInPathSlot (pathCacheQue_t *pathCacheQue, char *inPath,
pathCache_t **outPathCache);
int
chkCacheExpire (pathCacheQue_t *pathCacheQue);
int
addPathToCache (char *inPath, pathCacheQue_t *pathQueArray,
struct stat *stbuf, pathCache_t **outPathCache);
int
_addPathToCache (char *inPath, pathCacheQue_t *pathQueArray,
struct stat *stbuf, pathCache_t **outPathCache);
int
addToCacheSlot (char *inPath, pathCacheQue_t *pathCacheQue,
struct stat *stbuf, pathCache_t **outPathCache);
int
pathSum (char *inPath);
int
matchPathInPathCache (char *inPath, pathCacheQue_t *pathQueArray,
pathCache_t **outPathCache);
int
_matchPathInPathCache (char *inPath, pathCacheQue_t *pathQueArray,
pathCache_t **outPathCache);
int
isSpecialPath (char *inPath);
int
rmPathFromCache (char *inPath, pathCacheQue_t *pathQueArray);
int
_rmPathFromCache (char *inPath, pathCacheQue_t *pathQueArray);
int
addNewlyCreatedToCache (char *path, int descInx, int mode,
pathCache_t **tmpPathCache);
int
closeNewlyCreatedCache (newlyCreatedFile_t *newlyCreatedFile);
int
closeIrodsFd (iquest_fuse_irods_conn_t *irods_conn, int fd);
int
getDescInxInNewlyCreatedCache (char *path, int flags);
int fill_dir_stat(struct stat *stbuf, uint ctime, uint mtime, uint atime);
int fill_file_stat(struct stat *stbuf, uint mode, rodsLong_t size, uint ctime, uint mtime, uint atime);
int
irodsMknodWithCache (char *path, mode_t mode, char *cachePath);
int iquest_fuse_open_with_read_cache (iquest_fuse_irods_conn_t *irods_conn, char *path, int flags);
int
freePathCache (pathCache_t *tmpPathCache);
int
getFileCachePath (char *inPath, char *cacehPath);
int
setAndMkFileCacheDir ();
int 
updatePathCacheStat (pathCache_t *tmpPathCache);
int
ifuseClose (char *path, int descInx);
int
_ifuseClose (char *path, int descInx);
int dataObjCreateByFusePath (iquest_fuse_irods_conn_t *irods_conn, char *path, int mode, char *outIrodsPath);
int
ifusePut (iquest_fuse_irods_conn_t *irods_conn, char *path, char *locCachePath, int mode,
rodsLong_t srcSize);
int
freeFileCache (pathCache_t *tmpPathCache);
int ifuseReconnect (iquest_fuse_irods_conn_t *irods_conn);
int ifuseConnect (iquest_fuse_irods_conn_t *irods_conn);
int
getNewlyCreatedDescByPath (char *path);
int renmeOpenedIFuseDesc(iquest_fuse_t *iqf, pathCache_t *fromPathCache, char *to);
int map_irods_auth_errors(int irods_err, int fuse_err);

int _iquest_fuse_irods_getattr(iquest_fuse_irods_conn_t *irods_conn, const char *path, struct stat *stbuf, pathCache_t **out_pathCache);

int iquest_parse_rods_path_str(iquest_fuse_t *iqf, char *in_path, char *out_path);
int iquest_zone_hint_from_rods_path(iquest_fuse_t *iqf, char *rods_path, char *zone_hint);
int iquest_parse_fuse_path(iquest_fuse_t *iqf, char *path, char **rods_path, iquest_fuse_query_cond_t **query_cond, char **query_part_attr, char **post_query_path);
void iquest_fuse_t_destroy(iquest_fuse_t *iqf);
void iquest_fuse_conf_t_destroy(iquest_fuse_conf_t *conf);
int iquest_readdir_coll(iquest_fuse_t *iqf, const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);

int iquest_query_attr_exists(iquest_fuse_t *iqf, char *query_zone, iquest_fuse_query_cond_t *query_cond, char *attr);
int iquest_query_and_fill_attr_list(iquest_fuse_t *iqf, char *query_zone, iquest_fuse_query_cond_t *query_cond, void *buf, fuse_fill_dir_t filler);
int iquest_query_and_fill_value_list(iquest_fuse_t *iqf, char *query_zone, iquest_fuse_query_cond_t *query_cond, char *attr, void *buf, fuse_fill_dir_t filler);

int iquest_genquery_add_where_str(genQueryInp_t *genQueryInp, char *where_attr, char *where_op, char *where_val);
int iquest_genquery_add_select_str(genQueryInp_t *genQueryInp, char *select);

int iquest_where_cond_add(inxValPair_t *where_cond, char *where_attr, char *where_op, char *where_val);
int _iquest_where_cond_add(inxValPair_t *where_cond, char *where_attr, char *where_op, char *where_val);

void * malloc_and_zero_or_exit(int size);

#endif	/* IQUEST_FUSE_HELPER_H */
