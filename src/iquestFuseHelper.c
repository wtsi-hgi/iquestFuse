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
 *** Portions of This file are substantially based on iFuseLib.c which is  ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*****************************************************************************
 *** Portions of This file are substantially based on iFuseOper.c which is ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*****************************************************************************
 * iquestFuseHelper.c
 * 
 * Implementations of helper functions for iquestFuse.
 *****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <assert.h>

#include "iquestFuse.h"
#include "iquestFuseOperations.h"
#include "iquestFuseHelper.h"

#include "miscUtil.h"

//extern iquest_fuse_irods_conn_t *ConnHead;
extern iFuseDesc_t IFuseDesc[];
extern pathCacheQue_t NonExistPathArray[];
extern pathCacheQue_t PathArray[];
//TODO shove this in private data
//extern rodsEnv IquestRodsEnv;


#include <pthread.h>
static pthread_mutex_t DescLock;
static pthread_mutex_t ConnLock;
static pthread_mutex_t PathCacheLock;
static pthread_mutex_t NewlyCreatedOprLock;
pthread_t ConnManagerThr;
pthread_mutex_t ConnManagerLock;
pthread_cond_t ConnManagerCond;

char FuseCacheDir[MAX_NAME_LEN];

/* some global variables */
iFuseDesc_t IFuseDesc[MAX_IFUSE_DESC];
int IFuseDescInuseCnt = 0;
//iquest_fuse_irods_conn_t *ConnHead = NULL;
connReqWait_t *ConnReqWaitQue = NULL;

static int ConnManagerStarted = 0;

pathCacheQue_t NonExistPathArray[NUM_PATH_HASH_SLOT];
pathCacheQue_t PathArray[NUM_PATH_HASH_SLOT];
newlyCreatedFile_t NewlyCreatedFile[NUM_NEWLY_CREATED_SLOT];
char *ReadCacheDir = NULL;

//TODO what is this???
static specialPath_t SpecialPath[] = {
    {"/tls", 4},
    {"/i686", 5},
    {"/sse2", 5},
    {"/lib", 4},
    {"/librt.so.1", 11},
    {"/libacl.so.1", 12},
    {"/libselinux.so.1", 16},
    {"/libc.so.6", 10},
    {"/libpthread.so.0", 16},
    {"/libattr.so.1", 13},
};

static int NumSpecialPath = sizeof (SpecialPath) / sizeof (specialPath_t);

void iquest_fuse_t_destroy(iquest_fuse_t *iqf) {
  rodsLog(LOG_DEBUG, "iquest_fuse_t_destroy: freeing memory for dynamically allocated portions of iquest_fuse_t");
  if(iqf->conf != NULL) {
    iquest_fuse_conf_t_destroy(iqf->conf);
  }
  if(iqf->rodsEnv != NULL) {
    //TODO it seems like there should be an iRODS library function that does this? 
    rodsLog(LOG_DEBUG, "iquest_fuse_t_destroy: freeing memory for dynamically allocated portions of rodsEnv");
    if(iqf->rodsEnv->rodsServerDn != NULL) {
      rodsLog(LOG_DEBUG, "iquest_fuse_t_destroy: calling free(iqf->rodsEnv->rodsServerDn)");
      free(iqf->rodsEnv->rodsServerDn);
    }
  }
}

void iquest_fuse_conf_t_destroy(iquest_fuse_conf_t *conf) {
  rodsLog(LOG_DEBUG, "iquest_fuse_conf_t_destroy: freeing memory for dynamically allocated portions of iquest_fuse_conf_t");
  if(conf->base_query != NULL) {
    rodsLog(LOG_DEBUG, "iquest_fuse_conf_t_destroy: calling free(conf->base_query)");
    free(conf->base_query);
  }
  if(conf->irods_zone != NULL) {
    rodsLog(LOG_DEBUG, "iquest_fuse_conf_t_destroy: calling free(conf->irods_zone)");
    free(conf->irods_zone);
  }
  if(conf->indicator != NULL) {
    rodsLog(LOG_DEBUG, "iquest_fuse_conf_t_destroy: calling free(conf->indicator)");
    free(conf->indicator);
  }
}

int get_conn_count(iquest_fuse_t *iqf) {
    iquest_fuse_irods_conn_t *tmp_irods_conn;
    int connCnt = 0;
    tmp_irods_conn = iqf->irods_conn_head;
    while (tmp_irods_conn != NULL) {
	connCnt++;
	tmp_irods_conn = tmp_irods_conn->next;
    }
    return connCnt;
}


int
initPathCache ()
{
    bzero (NonExistPathArray, sizeof (NonExistPathArray));
    bzero (PathArray, sizeof (PathArray));
    bzero (NewlyCreatedFile, sizeof (NewlyCreatedFile));
    return (0);
}

int
isSpecialPath (char *in_path)
{
    int len;
    char *endPtr;
    int i;

    if (in_path == NULL) {
        rodsLog (LOG_ERROR,
          "isSpecialPath: input in_path is NULL");
        return (SYS_INTERNAL_NULL_INPUT_ERR);
    }

    len = strlen (in_path);
    endPtr = in_path + len;
    for (i = 0; i < NumSpecialPath; i++) {
	if (len < SpecialPath[i].len) continue;
	if (strcmp (SpecialPath[i].path, endPtr - SpecialPath[i].len) == 0)
	    return (1);
    }
    return 0;
}

int
matchPathInPathCache (char *in_path, pathCacheQue_t *pathQueArray,
pathCache_t **out_pathCache)
{
    int status;
    pthread_mutex_lock (&PathCacheLock);
    status = _matchPathInPathCache (in_path, pathQueArray, out_pathCache);
    pthread_mutex_unlock (&PathCacheLock);
    return status;
}

int
_matchPathInPathCache (char *in_path, pathCacheQue_t *pathQueArray,
pathCache_t **out_pathCache)
{
    int mysum, myslot;
    int status;
    pathCacheQue_t *myque;

    if (in_path == NULL) {
        rodsLog (LOG_ERROR,
          "matchPathInPathCache: input in_path is NULL");
        return (SYS_INTERNAL_NULL_INPUT_ERR);
    }

    mysum = pathSum (in_path);
    myslot = getHashSlot (mysum, NUM_PATH_HASH_SLOT);
    myque = &pathQueArray[myslot];

    chkCacheExpire (myque);
    status = matchPathInPathSlot (myque, in_path, out_pathCache);

    return status;
}

int
addPathToCache (char *in_path, pathCacheQue_t *pathQueArray,
struct stat *stbuf, pathCache_t **out_pathCache)
{
    int status;

    pthread_mutex_lock (&PathCacheLock);
    status = _addPathToCache (in_path, pathQueArray, stbuf, out_pathCache);
    pthread_mutex_unlock (&PathCacheLock);
    return status;
}

int
_addPathToCache (char *in_path, pathCacheQue_t *pathQueArray,
struct stat *stbuf, pathCache_t **out_pathCache)
{
    pathCacheQue_t *pathCacheQue;
    int mysum, myslot;
    int status;

    /* XXXX if (isSpecialPath ((char *) in_path) != 1) return 0; */
    mysum = pathSum (in_path);
    myslot = getHashSlot (mysum, NUM_PATH_HASH_SLOT);
    pathCacheQue = &pathQueArray[myslot];
    status = addToCacheSlot (in_path, pathCacheQue, stbuf, out_pathCache);

    return (status);
}

int
rmPathFromCache (char *in_path, pathCacheQue_t *pathQueArray)
{
    int status;
    pthread_mutex_lock (&PathCacheLock);
    status = _rmPathFromCache (in_path, pathQueArray);
    pthread_mutex_unlock (&PathCacheLock);
    return status;
}

int
_rmPathFromCache (char *in_path, pathCacheQue_t *pathQueArray)
{
    pathCacheQue_t *pathCacheQue;
    int mysum, myslot;
    pathCache_t *tmpPathCache;

    /* XXXX if (isSpecialPath ((char *) in_path) != 1) return 0; */
    mysum = pathSum (in_path);
    myslot = getHashSlot (mysum, NUM_PATH_HASH_SLOT);
    pathCacheQue = &pathQueArray[myslot];

    tmpPathCache = pathCacheQue->top;
    while (tmpPathCache != NULL) {
        if (strcmp (tmpPathCache->filePath, in_path) == 0) {
            if (tmpPathCache->prev == NULL) {
                /* top */
                pathCacheQue->top = tmpPathCache->next;
            } else {
                tmpPathCache->prev->next = tmpPathCache->next;
            }
            if (tmpPathCache->next == NULL) {
		/* bottom */
		pathCacheQue->bottom = tmpPathCache->prev;
	    } else {
		tmpPathCache->next->prev = tmpPathCache->prev;
	    }
	    freePathCache (tmpPathCache);
	    return 1;
	}
        tmpPathCache = tmpPathCache->next;
    }
    return 0;
}

int
getHashSlot (int value, int numHashSlot)
{
    int mySlot = value % numHashSlot;

    return (mySlot);
}

int
matchPathInPathSlot (pathCacheQue_t *pathCacheQue, char *in_path, 
pathCache_t **out_pathCache)
{
    pathCache_t *tmpPathCache;

    *out_pathCache = NULL;
    if (pathCacheQue == NULL) {
        rodsLog (LOG_ERROR,
          "matchPathInPathSlot: input pathCacheQue is NULL");
        return (SYS_INTERNAL_NULL_INPUT_ERR);
    }
    tmpPathCache = pathCacheQue->top;
    while (tmpPathCache != NULL) {
	if (strcmp (tmpPathCache->filePath, in_path) == 0) {
	    *out_pathCache = tmpPathCache;
	    return 1;
	}
	tmpPathCache = tmpPathCache->next;
    }
    return (0);
}

int
chkCacheExpire (pathCacheQue_t *pathCacheQue)
{
    pathCache_t *tmpPathCache;

    uint curTime = time (0);
    if (pathCacheQue == NULL) {
        rodsLog (LOG_ERROR,
          "chkCacheExpire: input pathCacheQue is NULL");
        return (SYS_INTERNAL_NULL_INPUT_ERR);
    }
    tmpPathCache = pathCacheQue->top;
    while (tmpPathCache != NULL) {
	if (curTime >= tmpPathCache->cachedTime + CACHE_EXPIRE_TIME) {
	    /* cache expired */
	    pathCacheQue->top = tmpPathCache->next;
	    freePathCache (tmpPathCache);
	    tmpPathCache = pathCacheQue->top;
            if (tmpPathCache != NULL) {
                tmpPathCache->prev = NULL;
            } else {
		pathCacheQue->bottom = NULL;
		return (0);
	    }
	} else {
	    /* not expired */
	    return (0);
	}
    }
    return (0);
}
	     
int
addToCacheSlot (char *in_path, pathCacheQue_t *pathCacheQue, 
struct stat *stbuf, pathCache_t **out_pathCache)
{
    pathCache_t *tmpPathCache;
    
    if (pathCacheQue == NULL || in_path == NULL) {
        rodsLog (LOG_ERROR,
          "addToCacheSlot: input pathCacheQue or in_path is NULL");
        return (SYS_INTERNAL_NULL_INPUT_ERR);
    }
    tmpPathCache = (pathCache_t *) malloc (sizeof (pathCache_t));
    if (out_pathCache != NULL) *out_pathCache = tmpPathCache;
    bzero (tmpPathCache, sizeof (pathCache_t));
    tmpPathCache->filePath = strdup (in_path);
    tmpPathCache->cachedTime = time (0);
    tmpPathCache->pathCacheQue = pathCacheQue;
    if (stbuf != NULL) {
	tmpPathCache->stbuf = *stbuf;
    }
    /* queue it to the bottom */
    if (pathCacheQue->top == NULL) {
	pathCacheQue->top = pathCacheQue->bottom = tmpPathCache;
    } else {
	pathCacheQue->bottom->next = tmpPathCache;
	tmpPathCache->prev = pathCacheQue->bottom;
	pathCacheQue->bottom = tmpPathCache;
    }
    return (0);
}

int
pathSum (char *in_path)
{
    int len, i;
    int mysum = 0;

    if (in_path == NULL) {
        rodsLog (LOG_ERROR,
          "pathSum: input in_path is NULL");
        return (SYS_INTERNAL_NULL_INPUT_ERR);
    }
    len = strlen (in_path);

    for (i = 0; i < len; i++) {
	mysum += in_path[i];
    }

    return mysum; 
}

int
initIFuseDesc ()
{
    pthread_mutex_init (&DescLock, NULL);
    pthread_mutex_init (&ConnLock, NULL);
    pthread_mutex_init (&NewlyCreatedOprLock, NULL);
    pthread_mutex_init (&PathCacheLock, NULL);
    pthread_mutex_init (&ConnManagerLock, NULL);
    pthread_cond_init (&ConnManagerCond, NULL);
    // JMC - overwrites objects construction? -  memset (IFuseDesc, 0, sizeof (iFuseDesc_t) * MAX_IFUSE_DESC);
    return (0);
}

int
allocIFuseDesc ()
{
    int i;

    pthread_mutex_lock (&DescLock);
    for (i = 3; i < MAX_IFUSE_DESC; i++) {
        if (IFuseDesc[i].inuseFlag <= IRODS_FREE) {
	    pthread_mutex_init (&IFuseDesc[i].lock, NULL);
            IFuseDesc[i].inuseFlag = IRODS_INUSE;
	    IFuseDescInuseCnt++;
            pthread_mutex_unlock (&DescLock);
            return (i);
        };
    }
    pthread_mutex_unlock (&DescLock);
    rodsLog (LOG_ERROR, 
      "allocIFuseDesc: Out of iFuseDesc");

    return (SYS_OUT_OF_FILE_DESC);
}

int 
lockDesc (int descInx)
{
    int status;

    if (descInx < 3 || descInx >= MAX_IFUSE_DESC) {
        rodsLog (LOG_ERROR,
         "lockDesc: descInx %d out of range", descInx);
        return (SYS_FILE_DESC_OUT_OF_RANGE);
    }
    status = pthread_mutex_lock (&IFuseDesc[descInx].lock);
    return status;
}

int
unlockDesc (int descInx)
{
    int status;

    if (descInx < 3 || descInx >= MAX_IFUSE_DESC) {
        rodsLog (LOG_ERROR,
         "unlockDesc: descInx %d out of range", descInx);
        return (SYS_FILE_DESC_OUT_OF_RANGE);
    }
    status = pthread_mutex_unlock (&IFuseDesc[descInx].lock);
    return status;
}

int
irods_connInuse (iquest_fuse_irods_conn_t *irods_conn)
{
    int i;
    int inuseCnt = 0;

    if (irods_conn == NULL) return 0;
    pthread_mutex_lock (&DescLock);
    for (i = 3; i < MAX_IFUSE_DESC; i++) {
	if (inuseCnt >= IFuseDescInuseCnt) break;
        if (IFuseDesc[i].inuseFlag == IRODS_INUSE) {
	    inuseCnt++;
	    if (IFuseDesc[i].irods_conn != NULL && 
	      IFuseDesc[i].irods_conn == irods_conn) {
                pthread_mutex_unlock (&DescLock);
                return 1;
	    }
	}
    }
    pthread_mutex_unlock (&DescLock);
    return (0);
}

int
freePathCache (pathCache_t *tmpPathCache)
{
    if (tmpPathCache == NULL) return 0;
    if (tmpPathCache->filePath != NULL) free (tmpPathCache->filePath);
    if (tmpPathCache->locCacheState != NO_FILE_CACHE &&
      tmpPathCache->locCachePath != NULL) {
	freeFileCache (tmpPathCache);
    }
    free (tmpPathCache);
    return (0);
}

int
freeFileCache (pathCache_t *tmpPathCache)
{
    unlink (tmpPathCache->locCachePath);
    free (tmpPathCache->locCachePath);
    tmpPathCache->locCachePath = NULL;
    tmpPathCache->locCacheState = NO_FILE_CACHE;
    return 0;
}

int
freeIFuseDesc (int descInx)
{
    int i;
    iquest_fuse_irods_conn_t *tmp_irods_conn = NULL;

    if (descInx < 3 || descInx >= MAX_IFUSE_DESC) {
        rodsLog (LOG_ERROR,
         "freeIFuseDesc: descInx %d out of range", descInx);
        return (SYS_FILE_DESC_OUT_OF_RANGE);
    }

    pthread_mutex_lock (&DescLock);
    for (i = 0; i < MAX_BUF_CACHE; i++) {
        if (IFuseDesc[descInx].bufCache[i].buf != NULL) {
	    free (IFuseDesc[descInx].bufCache[i].buf);
	}
    }
    if (IFuseDesc[descInx].objPath != NULL)
	free (IFuseDesc[descInx].objPath);

    if (IFuseDesc[descInx].localPath != NULL)
	free (IFuseDesc[descInx].localPath);
    pthread_mutex_destroy (&IFuseDesc[descInx].lock);
    tmp_irods_conn = IFuseDesc[descInx].irods_conn;
    if (tmp_irods_conn != NULL) {
	IFuseDesc[descInx].irods_conn = NULL;
    }
    memset (&IFuseDesc[descInx], 0, sizeof (iFuseDesc_t));
    IFuseDescInuseCnt--;

    pthread_mutex_unlock (&DescLock);
    /* have to do it outside the lock bacause _relIFuseConn lock it */
    if (tmp_irods_conn != NULL)
	_relIFuseConn (tmp_irods_conn);

    return (0);
}

int 
checkFuseDesc (int descInx)
{
    if (descInx < 3 || descInx >= MAX_IFUSE_DESC) {
        rodsLog (LOG_ERROR,
         "checkFuseDesc: descInx %d out of range", descInx);
        return (SYS_FILE_DESC_OUT_OF_RANGE);
    }

    if (IFuseDesc[descInx].inuseFlag != IRODS_INUSE) {
        rodsLog (LOG_ERROR,
         "checkFuseDesc: descInx %d is not inuse", descInx);
        return (SYS_BAD_FILE_DESCRIPTOR);
    }
    if (IFuseDesc[descInx].iFd <= 0) {
        rodsLog (LOG_ERROR,
         "checkFuseDesc:  iFd %d of descInx %d <= 0", 
	  IFuseDesc[descInx].iFd, descInx);
        return (SYS_BAD_FILE_DESCRIPTOR);
    }

    return (0);
}

int
fillIFuseDesc (int descInx, iquest_fuse_irods_conn_t *irods_conn, int iFd, char *objPath,
char *localPath)
{ 
    IFuseDesc[descInx].irods_conn = irods_conn;
    IFuseDesc[descInx].iFd = iFd;
    if (objPath != NULL) {
        /* rstrcpy (IFuseDesc[descInx].objPath, objPath, MAX_NAME_LEN); */
        IFuseDesc[descInx].objPath = strdup (objPath);
    }
    if (localPath != NULL) {
        /* rstrcpy (IFuseDesc[descInx].localPath, localPath, MAX_NAME_LEN); */
        IFuseDesc[descInx].localPath = strdup (localPath);
    }
    return (0);
}

int
ifuseClose (char *path, int descInx)
{
    int lockFlag;
    int status;

    if (IFuseDesc[descInx].irods_conn != NULL &&
      IFuseDesc[descInx].irods_conn->conn != NULL) {
        useIFuseConn (IFuseDesc[descInx].irods_conn);
	lockFlag = 1;
    } else {
	lockFlag = 0;
    }
    status = _ifuseClose (path, descInx);
    if (lockFlag == 1) {
	unuseIFuseConn (IFuseDesc[descInx].irods_conn);
    }
    return status;
}

int
_ifuseClose (char *path, int descInx)
{
    int status = 0;
    int savedStatus = 0;
    int goodStat = 0;

    if (IFuseDesc[descInx].locCacheState == NO_FILE_CACHE) {
	status = closeIrodsFd (IFuseDesc[descInx].irods_conn, 
	  IFuseDesc[descInx].iFd);
    } else {	/* cached */
        if (IFuseDesc[descInx].newFlag > 0 || 
	  IFuseDesc[descInx].locCacheState == HAVE_NEWLY_CREATED_CACHE) {
            pathCache_t *tmpPathCache;
            /* newly created. Just update the size */
            if (matchPathInPathCache ((char *) path, PathArray,
             &tmpPathCache) == 1 && tmpPathCache->locCachePath != NULL) {
                status = updatePathCacheStat (tmpPathCache);
                if (status >= 0) goodStat = 1;
		status = ifusePut (IFuseDesc[descInx].irods_conn, 
		  path, tmpPathCache->locCachePath,
		  IFuseDesc[descInx].createMode, 
		  tmpPathCache->stbuf.st_size);
                if (status < 0) {
                    rodsLog (LOG_ERROR,
                      "ifuseClose: ifusePut of %s error, status = %d",
                       path, status);
                    savedStatus = -EBADF;
                }
		if (tmpPathCache->stbuf.st_size > MAX_READ_CACHE_SIZE) {
		    /* too big to keep */
		    freeFileCache (tmpPathCache);
		}	
            } else {
                /* should not be here. but cache may be removed that we
		 * may have to deal with it */
                rodsLog (LOG_ERROR,
                  "ifuseClose: IFuseDesc indicated a newly created cache, but does not exist for %s",
                   path);
		savedStatus = -EBADF;
	    }
	}
	status = close (IFuseDesc[descInx].iFd);
	if (status < 0) {
	    status = (errno ? (-1 * errno) : -1);
	} else {
	    status = savedStatus;
	}
    }

    if (IFuseDesc[descInx].bytesWritten > 0 && goodStat == 0) 
        rmPathFromCache ((char *) path, PathArray);
    return (status);
}

int ifusePut (iquest_fuse_irods_conn_t *irods_conn, char *path, char *locCachePath, int mode, rodsLong_t srcSize) {
  dataObjInp_t dataObjInp;
  int status;
  rcComm_t *conn;

  conn = irods_conn->conn;
  memset (&dataObjInp, 0, sizeof (dataObjInp));
  status = iquest_parse_rods_path_str(irods_conn->iqf, (char *) (path + 1), 
			     dataObjInp.objPath);
  if (status < 0) {
    rodsLogError (LOG_ERROR, status,
		  "ifusePut: iquest_parse_rods_path_str of %s error", path);
    /* use ENOTDIR for this type of error */
    return -ENOTDIR;
  }
  dataObjInp.dataSize = srcSize;
  dataObjInp.createMode = mode;
  dataObjInp.openFlags = O_RDWR;
  addKeyVal (&dataObjInp.condInput, FORCE_FLAG_KW, "");
  addKeyVal (&dataObjInp.condInput, DATA_TYPE_KW, "generic");
  if (strlen (irods_conn->iqf->rodsEnv->rodsDefResource) > 0) {
    addKeyVal (&dataObjInp.condInput, DEST_RESC_NAME_KW,
	       irods_conn->iqf->rodsEnv->rodsDefResource);
  }
  
  status = rcDataObjPut (conn, &dataObjInp, locCachePath);
  return (status);
}

int
ifuseWrite (char *path, int descInx, char *buf, size_t size,
off_t offset)
{
    int status, myError;
    char irodsPath[MAX_NAME_LEN];
    openedDataObjInp_t dataObjWriteInp;
    bytesBuf_t dataObjWriteInpBBuf;


    bzero (&dataObjWriteInp, sizeof (dataObjWriteInp));
    if (IFuseDesc[descInx].locCacheState == NO_FILE_CACHE) {
        dataObjWriteInpBBuf.buf = (void *) buf;
        dataObjWriteInpBBuf.len = size;
        dataObjWriteInp.l1descInx = IFuseDesc[descInx].iFd;
        dataObjWriteInp.len = size;

        if (IFuseDesc[descInx].irods_conn != NULL && 
	  IFuseDesc[descInx].irods_conn->conn != NULL) {
	    useIFuseConn (IFuseDesc[descInx].irods_conn);
            status = rcDataObjWrite (IFuseDesc[descInx].irods_conn->conn, 
	      &dataObjWriteInp, &dataObjWriteInpBBuf);
	    unuseIFuseConn (IFuseDesc[descInx].irods_conn);
            if (status < 0) {
                if ((myError = getErrno (status)) > 0) {
                    return (-myError);
                } else {
                    return -ENOENT;
                }
            } else if (status != (int) size) {
                rodsLog (LOG_ERROR,
		  "ifuseWrite: IFuseDesc[descInx].conn for %s is NULL", path);
                return -ENOENT;
	    }
	} else {
            rodsLog (LOG_ERROR,
              "ifuseWrite: IFuseDesc[descInx].conn for %s is NULL", path);
            return -ENOENT;
        }
        IFuseDesc[descInx].offset += status;
    } else {
        status = write (IFuseDesc[descInx].iFd, buf, size);

        if (status < 0) return (errno ? (-1 * errno) : -1);
        IFuseDesc[descInx].offset += status;
	if (IFuseDesc[descInx].offset >= MAX_NEWLY_CREATED_CACHE_SIZE) {
	    int irodsFd; 
	    int status1;
	    struct stat stbuf;
	    char *mybuf;
	    rodsLong_t myoffset;

	    /* need to write it to iRODS */
	    if (IFuseDesc[descInx].irods_conn != NULL &&
	      IFuseDesc[descInx].irods_conn->conn != NULL) {
		useIFuseConn (IFuseDesc[descInx].irods_conn);
	        irodsFd = dataObjCreateByFusePath (
		  IFuseDesc[descInx].irods_conn,
		  path, IFuseDesc[descInx].createMode, irodsPath);
		unuseIFuseConn (IFuseDesc[descInx].irods_conn);
	    } else {
                rodsLog (LOG_ERROR,
                  "ifuseWrite: IFuseDesc[descInx].conn for %s is NULL", path);
		irodsFd = -ENOENT;
	    }
	    if (irodsFd < 0) {
                rodsLog (LOG_ERROR,
                  "ifuseWrite: dataObjCreateByFusePath of %s error, stat=%d",
                 path, irodsFd);
		close (IFuseDesc[descInx].iFd);
		rmPathFromCache ((char *) path, PathArray);
                return -ENOENT;
	    }
	    status1 = fstat (IFuseDesc[descInx].iFd, &stbuf);
            if (status1 < 0) {
                rodsLog (LOG_ERROR,
                  "ifuseWrite: fstat of %s error, errno=%d",
                 path, errno);
		close (IFuseDesc[descInx].iFd);
		rmPathFromCache ((char *) path, PathArray);
		return (errno ? (-1 * errno) : -1);
	    }
	    mybuf = (char *) malloc (stbuf.st_size);
	    lseek (IFuseDesc[descInx].iFd, 0, SEEK_SET);
	    status1 = read (IFuseDesc[descInx].iFd, mybuf, stbuf.st_size);
            if (status1 < 0) {
                rodsLog (LOG_ERROR,
                  "ifuseWrite: read of %s error, errno=%d",
                 path, errno);
		close (IFuseDesc[descInx].iFd);
                rmPathFromCache ((char *) path, PathArray);
                free(mybuf);	// cppcheck - Memory leak: mybuf
                return (errno ? (-1 * errno) : -1);
            }
            dataObjWriteInpBBuf.buf = (void *) mybuf;
            dataObjWriteInpBBuf.len = stbuf.st_size;
            dataObjWriteInp.l1descInx = irodsFd;
            dataObjWriteInp.len = stbuf.st_size;

	    if (IFuseDesc[descInx].irods_conn != NULL &&
	      IFuseDesc[descInx].irods_conn->conn != NULL) {
		useIFuseConn (IFuseDesc[descInx].irods_conn);
                status1 = rcDataObjWrite (IFuseDesc[descInx].irods_conn->conn, 
		  &dataObjWriteInp, &dataObjWriteInpBBuf);
		unuseIFuseConn (IFuseDesc[descInx].irods_conn);
	    } else {
                rodsLog (LOG_ERROR,
                 "ifuseWrite: IFuseDesc[descInx].conn for %s is NULL", path);
                status1 = -ENOENT;
            }
	    free (mybuf);
            close (IFuseDesc[descInx].iFd);
            rmPathFromCache ((char *) path, PathArray);

            if (status1 < 0) {
                rodsLog (LOG_ERROR,
                  "ifuseWrite: rcDataObjWrite of %s error, status=%d",
                 path, status1);
                if ((myError = getErrno (status1)) > 0) {
                    status1 = (-myError);
                } else {
                    status1 = -ENOENT;
                }
		IFuseDesc[descInx].iFd = 0;
                return (status1);
	    } else {
		IFuseDesc[descInx].iFd = irodsFd;
		IFuseDesc[descInx].locCacheState = NO_FILE_CACHE;
		IFuseDesc[descInx].newFlag = 0;
	    }
	    /* one last thing - seek to the right offset */
            myoffset = IFuseDesc[descInx].offset;
	    IFuseDesc[descInx].offset = 0;
            if ((status1 = ifuseLseek ((char *) path, descInx, myoffset)) 
	      < 0) {
                rodsLog (LOG_ERROR,
                  "ifuseWrite: ifuseLseek of %s error, status=%d",
                 path, status1);
                if ((myError = getErrno (status1)) > 0) {
                    return (-myError);
                } else {
                    return -ENOENT;
                }
	    }
	}
    }
    IFuseDesc[descInx].bytesWritten += status;

    return status;
}

int
ifuseRead (char *path, int descInx, char *buf, size_t size, 
off_t offset)
{
    int status;

    if (IFuseDesc[descInx].locCacheState == NO_FILE_CACHE) {
        openedDataObjInp_t dataObjReadInp;
	bytesBuf_t dataObjReadOutBBuf;
	int myError;

        bzero (&dataObjReadInp, sizeof (dataObjReadInp));
        dataObjReadOutBBuf.buf = buf;
        dataObjReadOutBBuf.len = size;
        dataObjReadInp.l1descInx = IFuseDesc[descInx].iFd;
        dataObjReadInp.len = size;

	if (IFuseDesc[descInx].irods_conn != NULL &&
	  IFuseDesc[descInx].irods_conn->conn != NULL) {
	    useIFuseConn (IFuseDesc[descInx].irods_conn);
            status = rcDataObjRead (IFuseDesc[descInx].irods_conn->conn, 
	      &dataObjReadInp, &dataObjReadOutBBuf);
	    unuseIFuseConn (IFuseDesc[descInx].irods_conn);
            if (status < 0) {
                if ((myError = getErrno (status)) > 0) {
                    return (-myError);
                } else {
                    return -ENOENT;
                }
            }
	} else {
            rodsLog (LOG_ERROR,
              "ifusRead: IFuseDesc[descInx].conn for %s is NULL", path);
            status = -ENOENT;
        }
    } else {
	status = read (IFuseDesc[descInx].iFd, buf, size);

	if (status < 0) return (errno ? (-1 * errno) : -1);
    }
    IFuseDesc[descInx].offset += status;

    return status;
}

int
ifuseLseek (char *path, int descInx, off_t offset)
{
    int status;

    if (IFuseDesc[descInx].offset != offset) {
	if (IFuseDesc[descInx].locCacheState == NO_FILE_CACHE) {
            openedDataObjInp_t dataObjLseekInp;
            fileLseekOut_t *dataObjLseekOut = NULL;

	    bzero (&dataObjLseekInp, sizeof (dataObjLseekInp));
            dataObjLseekInp.l1descInx = IFuseDesc[descInx].iFd;
            dataObjLseekInp.offset = offset;
            dataObjLseekInp.whence = SEEK_SET;

	    if (IFuseDesc[descInx].irods_conn != NULL &&
	      IFuseDesc[descInx].irods_conn->conn != NULL) {
		useIFuseConn (IFuseDesc[descInx].irods_conn);
                status = rcDataObjLseek (IFuseDesc[descInx].irods_conn->conn, 
		  &dataObjLseekInp, &dataObjLseekOut);
		unuseIFuseConn (IFuseDesc[descInx].irods_conn);
                if (dataObjLseekOut != NULL) free (dataObjLseekOut);
	    } else {
                rodsLog (LOG_ERROR,
                  "ifuseLseek: IFuseDesc[descInx].conn for %s is NULL", path);
                status = -ENOENT;
            }
	} else {
	    rodsLong_t lstatus;
	    lstatus = lseek (IFuseDesc[descInx].iFd, offset, SEEK_SET);
	    if (lstatus >= 0) {
		status = 0;
	    } else {
		status = lstatus;
	    }
	}

        if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "ifuseLseek: lseek of %s error", path);
            return status;
        } else {
            IFuseDesc[descInx].offset = offset;
        }

    }
    return (0);
}

/* 
 * getIFuseConnByPath - try to use the same conn as opened desc of the
 * same path 
 */
int get_iquest_fuse_irods_conn_by_path(iquest_fuse_irods_conn_t **irods_conn, iquest_fuse_t *iqf, char *localPath) {
  int i, status;
  int inuseCnt = 0;
  pthread_mutex_lock (&DescLock);
  for (i = 3; i < MAX_IFUSE_DESC; i++) {
    if (inuseCnt >= IFuseDescInuseCnt) break;
    if (IFuseDesc[i].inuseFlag == IRODS_INUSE) {
      inuseCnt++;
      if (IFuseDesc[i].irods_conn != NULL &&
	  IFuseDesc[i].irods_conn->conn != NULL &&
	  strcmp (localPath, IFuseDesc[i].localPath) == 0) {
	*irods_conn = IFuseDesc[i].irods_conn;
	pthread_mutex_lock (&ConnLock);
	pthread_mutex_unlock (&DescLock);
	_useIFuseConn (*irods_conn);
	return 0;
      }
    }
  }
  /* no match. just assign one */
  pthread_mutex_unlock (&DescLock);
  status = get_iquest_fuse_irods_conn(irods_conn, iqf);
  
  return status;
}

/*
 * get a connection, creating one if necessary. 
 * modifies its first argument
 */
int get_iquest_fuse_irods_conn(iquest_fuse_irods_conn_t **irods_conn, iquest_fuse_t *iqf) {
    int status;
    iquest_fuse_irods_conn_t *tmp_irods_conn;
    int inuseCnt;

    *irods_conn = NULL;

    while (*irods_conn == NULL) {
        pthread_mutex_lock (&ConnLock);
        /* get a free IFuseConn */

	inuseCnt = 0;
        tmp_irods_conn = iqf->irods_conn_head;
        while (tmp_irods_conn != NULL) {
	    if (tmp_irods_conn->status == IRODS_FREE && 
	      tmp_irods_conn->conn != NULL) {
	        useFreeIFuseConn (tmp_irods_conn);
	        *irods_conn = tmp_irods_conn;
		return 0;;
	    }
	    inuseCnt++;
	    tmp_irods_conn = tmp_irods_conn->next;
        }
        if (inuseCnt >= MAX_NUM_CONN) {
	    connReqWait_t myConnReqWait, *tmpConnReqWait;
	    /* find one that is not in use */
            tmp_irods_conn = iqf->irods_conn_head;
            while (tmp_irods_conn != NULL) {
                if (tmp_irods_conn->inuseCnt == 0 && 
	          tmp_irods_conn->pendingCnt == 0 && tmp_irods_conn->conn != NULL) {
                    _useIFuseConn (tmp_irods_conn);
                    *irods_conn = tmp_irods_conn;
                    return 0;
                }
                tmp_irods_conn = tmp_irods_conn->next;
            }
	    /* have to wait */
            struct timespec timeout;
	    bzero (&myConnReqWait, sizeof (myConnReqWait));
            pthread_mutex_init (&myConnReqWait.mutex, NULL);
            pthread_cond_init (&myConnReqWait.cond, NULL);
	    /* queue it to the bottom */
	    if (ConnReqWaitQue == NULL) {
	        ConnReqWaitQue = &myConnReqWait;
	    } else {
	        tmpConnReqWait = ConnReqWaitQue;
	        while (tmpConnReqWait->next != NULL) {
		    tmpConnReqWait = tmpConnReqWait->next;
	        }
	        tmpConnReqWait->next = &myConnReqWait;
	    }
	    while (myConnReqWait.state == 0) {
	      bzero (&timeout, sizeof (timeout));
                timeout.tv_sec = time (0) + IQF_CONN_REQ_SLEEP_TIME;
                pthread_mutex_unlock (&ConnLock);
                pthread_mutex_lock (&myConnReqWait.mutex);
                pthread_cond_timedwait (&myConnReqWait.cond, 
		  &myConnReqWait.mutex, &timeout);
                pthread_mutex_unlock (&myConnReqWait.mutex);
	    }
	    pthread_mutex_destroy (&myConnReqWait.mutex);
	    /* start from begining */
	    continue;
        }

        pthread_mutex_unlock (&ConnLock);
        /* 
	 * get here when nothing free. make a new connection. 
	 */
        tmp_irods_conn = (iquest_fuse_irods_conn_t *) malloc (sizeof (iquest_fuse_irods_conn_t));
        if (tmp_irods_conn == NULL) {
            return SYS_MALLOC_ERR;
        }
        bzero (tmp_irods_conn, sizeof (iquest_fuse_irods_conn_t));
	tmp_irods_conn->iqf = iqf;
	
        pthread_mutex_init (&tmp_irods_conn->lock, NULL);

        status = ifuseConnect (tmp_irods_conn);
        if (status < 0) {
	  rodsLogError ( LOG_ERROR, status, "connection error");
	  if (status == KRB_ERROR_ACQUIRING_CREDS) {
	    return -ENOKEY;
	  }
	  return -EPERM;
	}

        useIFuseConn (tmp_irods_conn);

        *irods_conn = tmp_irods_conn;
        pthread_mutex_lock (&ConnLock);
        /* queue it on top */
        tmp_irods_conn->next = iqf->irods_conn_head;
        iqf->irods_conn_head = tmp_irods_conn;

        pthread_mutex_unlock (&ConnLock);
	break;
    }	/* while *irods_conn */

    if (ConnManagerStarted < HIGH_NUM_CONN && 
      ++ConnManagerStarted == HIGH_NUM_CONN) {
	/* don't do it the first time */

        status = pthread_create (&ConnManagerThr, pthread_attr_default,(void *(*)(void *)) conn_manager, iqf);
        if (status < 0) {
            rodsLog (LOG_ERROR, "pthread_create failure, status = %d", status);
	    ConnManagerStarted --;	/* try again */
	}
    }
    return 0;
}

int
useIFuseConn (iquest_fuse_irods_conn_t *irods_conn)
{
    int status;
    if (irods_conn == NULL || irods_conn->conn == NULL)
        return USER__NULL_INPUT_ERR;
    pthread_mutex_lock (&ConnLock);
    status = _useIFuseConn (irods_conn);
    return status;
}

/* queIFuseConn - lock ConnLock before calling */ 
int
_useIFuseConn (iquest_fuse_irods_conn_t *irods_conn)
{
    if (irods_conn == NULL || irods_conn->conn == NULL) 
	return USER__NULL_INPUT_ERR;
    irods_conn->actTime = time (NULL);
    irods_conn->status = IRODS_INUSE;
    irods_conn->pendingCnt++;

    pthread_mutex_unlock (&ConnLock);
    pthread_mutex_lock (&irods_conn->lock);
    pthread_mutex_lock (&ConnLock);

    irods_conn->inuseCnt++;
    irods_conn->pendingCnt--;

    pthread_mutex_unlock (&ConnLock);
    return 0;
}

int
useFreeIFuseConn (iquest_fuse_irods_conn_t *irods_conn)
{
    if (irods_conn == NULL) return USER__NULL_INPUT_ERR;
    irods_conn->actTime = time (NULL);
    irods_conn->status = IRODS_INUSE;
    irods_conn->inuseCnt++;
    pthread_mutex_unlock (&ConnLock);
    pthread_mutex_lock (&irods_conn->lock);
    return 0;
}

int
unuseIFuseConn (iquest_fuse_irods_conn_t *irods_conn)
{
    if (irods_conn == NULL || irods_conn->conn == NULL)
        return USER__NULL_INPUT_ERR;
    pthread_mutex_lock (&ConnLock);
    irods_conn->actTime = time (NULL);
    irods_conn->inuseCnt--;
    pthread_mutex_unlock (&ConnLock);
    pthread_mutex_unlock (&irods_conn->lock);
    return 0;
}

/*
 * Attempts to connect the iRODS client connection in irods_conn
 * irods_conn->rodsEnv must already be specified before calling this. 
 */
int ifuseConnect (iquest_fuse_irods_conn_t *irods_conn) { 
    int status;
    rErrMsg_t errMsg;
    rodsEnv *myRodsEnv;
    myRodsEnv = irods_conn->iqf->rodsEnv;

    irods_conn->conn = rcConnect (myRodsEnv->rodsHost, myRodsEnv->rodsPort,
      myRodsEnv->rodsUserName, myRodsEnv->rodsZone, NO_RECONN, &errMsg);

    if (irods_conn->conn == NULL) {
	/* try one more */
        irods_conn->conn = rcConnect (myRodsEnv->rodsHost, myRodsEnv->rodsPort,
          myRodsEnv->rodsUserName, myRodsEnv->rodsZone, NO_RECONN, &errMsg);
	if (irods_conn->conn == NULL) {
            rodsLogError (LOG_ERROR, errMsg.status,
              "ifuseConnect: rcConnect failure %s", errMsg.msg);
            if (errMsg.status < 0) {
                return (errMsg.status);
            } else {
                return (-1);
	    }
        }
    }

    status = clientLogin (irods_conn->conn);
    if (status != 0) {
      if(getCanonicalAuthScheme() == AUTHSCHEME_KRB) {
	/* don't disconnect if using kerberos -- tickets could be renewed later, making a future request succeed */
      } else {
	rcDisconnect (irods_conn->conn);
	irods_conn->conn=NULL;
      }
    }
    return (status);
}

int
relIFuseConn (iquest_fuse_irods_conn_t *irods_conn)
{
    int status;

    if (irods_conn == NULL) return USER__NULL_INPUT_ERR;
    unuseIFuseConn (irods_conn);
    status = _relIFuseConn (irods_conn);
    return status;
}
 
/* _relIFuseConn - call when calling from freeIFuseDesc where unuseIFuseConn
 * is not called */
int
_relIFuseConn (iquest_fuse_irods_conn_t *irods_conn)
{
    if (irods_conn == NULL) return USER__NULL_INPUT_ERR;
    pthread_mutex_lock (&ConnLock);
    irods_conn->actTime = time (NULL);
    if (irods_conn->conn == NULL) {
        /* unlock it before calling irods_connInuse which locks DescLock */
        pthread_mutex_unlock (&ConnLock);
        if (irods_connInuse (irods_conn) == 0) {
            pthread_mutex_lock (&ConnLock);
            irods_conn->status = IRODS_FREE;
            pthread_mutex_unlock (&ConnLock);
        }
    } else if (irods_conn->pendingCnt + irods_conn->inuseCnt <= 0) {
        /* unlock it before calling irods_connInuse which locks DescLock */
        pthread_mutex_unlock (&ConnLock);
        if (irods_connInuse (irods_conn) == 0) {
            pthread_mutex_lock (&ConnLock);
            irods_conn->status = IRODS_FREE;
            pthread_mutex_unlock (&ConnLock);
	}
    } else {
        pthread_mutex_unlock (&ConnLock);
    }
    signal_conn_manager(irods_conn->iqf);
    return 0;
}

int signal_conn_manager(iquest_fuse_t *iqf) {
    int connCnt;
    pthread_mutex_lock (&ConnLock);
    connCnt = get_conn_count(iqf);
    pthread_mutex_unlock (&ConnLock);
    if (connCnt > HIGH_NUM_CONN) {
        pthread_mutex_lock (&ConnManagerLock);
	pthread_cond_signal (&ConnManagerCond);
        pthread_mutex_unlock (&ConnManagerLock);
    }
    return 0;
}

int disconnect_all (iquest_fuse_t *iqf) {
    iquest_fuse_irods_conn_t *tmp_irods_conn;
    pthread_mutex_lock (&ConnLock);
    tmp_irods_conn = iqf->irods_conn_head;
    while (tmp_irods_conn != NULL) {
	if (tmp_irods_conn->conn != NULL) {
	    rcDisconnect (tmp_irods_conn->conn);
	    tmp_irods_conn->conn=NULL;
	}
	tmp_irods_conn = tmp_irods_conn->next;
    }
    return 0;
}

void conn_manager (iquest_fuse_t *iqf) {
    time_t curTime;
    iquest_fuse_irods_conn_t *tmp_irods_conn, *savedIFuseConn;
    iquest_fuse_irods_conn_t *prevIquestFuseConn;
    struct timespec timeout;
    int freeCnt = 0;

    while (1) {
	int connCnt;
        curTime = time(NULL);

        pthread_mutex_lock(&ConnLock);

        tmp_irods_conn = iqf->irods_conn_head;
	connCnt = 0;
	prevIquestFuseConn = NULL;
        while (tmp_irods_conn != NULL) {
	    if (tmp_irods_conn->status == IRODS_FREE) freeCnt ++;
	    if (curTime - tmp_irods_conn->actTime > IQF_CONN_TIMEOUT) {
		if (tmp_irods_conn->status == IRODS_FREE) {
		    /* can be disconnected */
		    if (tmp_irods_conn->conn != NULL) {
		        rcDisconnect (tmp_irods_conn->conn);
			tmp_irods_conn->conn=NULL;
		    }
		    pthread_mutex_unlock (&tmp_irods_conn->lock);
		    pthread_mutex_destroy (&tmp_irods_conn->lock);
		    if (prevIquestFuseConn == NULL) {
			/* top */
			iqf->irods_conn_head = tmp_irods_conn->next;
		    } else {
			prevIquestFuseConn->next = tmp_irods_conn->next;
		    }
		    savedIFuseConn = tmp_irods_conn;
		    tmp_irods_conn = tmp_irods_conn->next;
		    free (savedIFuseConn);
		    continue;
		} 
	    }
	    connCnt++;
	    prevIquestFuseConn = tmp_irods_conn;
	    tmp_irods_conn = tmp_irods_conn->next;
	}
	if (MAX_NUM_CONN - connCnt > freeCnt) 
            freeCnt = MAX_NUM_CONN - connCnt;

	/* exceed high water mark for number of connection ? */
	if (connCnt > HIGH_NUM_CONN) {
            tmp_irods_conn = iqf->irods_conn_head;
            prevIquestFuseConn = NULL;
            while (tmp_irods_conn != NULL && connCnt > HIGH_NUM_CONN) {
		if (tmp_irods_conn->status == IRODS_FREE) {
                    /* can be disconnected */
                    if (tmp_irods_conn->conn != NULL) {
                        rcDisconnect (tmp_irods_conn->conn);
	    		tmp_irods_conn->conn=NULL;
                    }
                    pthread_mutex_unlock (&tmp_irods_conn->lock);
                    pthread_mutex_destroy (&tmp_irods_conn->lock);
                    if (prevIquestFuseConn == NULL) {
                        /* top */
                        iqf->irods_conn_head = tmp_irods_conn->next;
                    } else {
                        prevIquestFuseConn->next = tmp_irods_conn->next;
                    }
                    savedIFuseConn = tmp_irods_conn;
                    tmp_irods_conn = tmp_irods_conn->next;
                    free (savedIFuseConn);
		    connCnt--;
                    continue;
                }
                prevIquestFuseConn = tmp_irods_conn;
                tmp_irods_conn = tmp_irods_conn->next;
            }
	}
	/* wake up the ConnReqWaitQue if freeCnt > 0 */
        while (freeCnt > 0  && ConnReqWaitQue != NULL) {
            /* signal one in the wait queue */
            connReqWait_t *myConnReqWait;
            myConnReqWait = ConnReqWaitQue;
	    myConnReqWait->state = 1;
            ConnReqWaitQue = myConnReqWait->next;
            pthread_mutex_unlock (&ConnLock);
            pthread_mutex_lock (&myConnReqWait->mutex);
            pthread_cond_signal (& myConnReqWait->cond);
            pthread_mutex_unlock (&myConnReqWait->mutex);
            pthread_mutex_lock (&ConnLock);
            freeCnt--;
        }
        pthread_mutex_unlock (&ConnLock);
	bzero (&timeout, sizeof (timeout));
	timeout.tv_sec = time (0) + IQF_CONN_MANAGER_SLEEP_TIME;
	pthread_mutex_lock (&ConnManagerLock);
	pthread_cond_timedwait (&ConnManagerCond, &ConnManagerLock, &timeout);
	pthread_mutex_unlock (&ConnManagerLock);

    }
}

/* have to do this after get_iquest_fuse_irods_conn - lock */
int
ifuseReconnect (iquest_fuse_irods_conn_t *irods_conn)
{
    int status = 0;

    if (irods_conn == NULL || irods_conn->conn == NULL) 
	return USER__NULL_INPUT_ERR;
    rodsLog (LOG_DEBUG, "ifuseReconnect: reconnecting");
    rcDisconnect (irods_conn->conn);
    irods_conn->conn=NULL;
    status = ifuseConnect (irods_conn);
    return status;
}

int
addNewlyCreatedToCache (char *path, int descInx, int mode, 
pathCache_t **tmpPathCache)
{
    int i;
    int newlyInx = -1;
    uint cachedTime = time (0);
    pthread_mutex_lock (&NewlyCreatedOprLock);
    for (i = 0; i < NUM_NEWLY_CREATED_SLOT; i++) {
	if (newlyInx < 0 && NewlyCreatedFile[i].inuseFlag == IRODS_FREE) { 
	    newlyInx = i;
	    NewlyCreatedFile[i].inuseFlag = IRODS_INUSE;
	} else if (NewlyCreatedFile[i].inuseFlag == IRODS_INUSE) {
	    if (cachedTime - NewlyCreatedFile[i].cachedTime  >= 
	      MAX_NEWLY_CREATED_TIME) {
		closeNewlyCreatedCache (&NewlyCreatedFile[i]);
	        if (newlyInx < 0) {
		    newlyInx = i;
		} else {
		    NewlyCreatedFile[i].inuseFlag = IRODS_FREE;
		}
	    }
	}
    }
    if (newlyInx < 0) {
	/* have to close one */
	newlyInx = NUM_NEWLY_CREATED_SLOT - 2;
        closeNewlyCreatedCache (&NewlyCreatedFile[newlyInx]);
	NewlyCreatedFile[newlyInx].inuseFlag = IRODS_INUSE;
    }
    rstrcpy (NewlyCreatedFile[newlyInx].filePath, path, MAX_NAME_LEN);
    NewlyCreatedFile[newlyInx].descInx = descInx;
    NewlyCreatedFile[newlyInx].cachedTime = cachedTime;
    IFuseDesc[descInx].newFlag = 1;    /* XXXXXXX use newlyInx ? */
    fillFileStat (&NewlyCreatedFile[newlyInx].stbuf, mode, 0, cachedTime, 
      cachedTime, cachedTime);
    addPathToCache (path, PathArray, &NewlyCreatedFile[newlyInx].stbuf, 
      tmpPathCache);
    pthread_mutex_unlock (&NewlyCreatedOprLock);
    return (0);
}

int closeIrodsFd (iquest_fuse_irods_conn_t *irods_conn, int fd) {
  int status;
  rcComm_t *conn;
  conn = irods_conn->conn;
  openedDataObjInp_t dataObjCloseInp;
  
  bzero (&dataObjCloseInp, sizeof (dataObjCloseInp));
  dataObjCloseInp.l1descInx = fd;
  status = rcDataObjClose (conn, &dataObjCloseInp);
  return (status);
}

int
closeNewlyCreatedCache (newlyCreatedFile_t *newlyCreatedFile)
{
    int status = 0;

    if (newlyCreatedFile == NULL) return USER__NULL_INPUT_ERR;
    if (strlen (newlyCreatedFile->filePath) > 0) {
	int descInx = newlyCreatedFile->descInx;
	
	/* should not call irodsRelease because it will call
	 * get_iquest_fuse_irods_conn which will result in deadlock 
	 * irodsRelease (newlyCreatedFile->filePath, &fi); */
        if (checkFuseDesc (descInx) < 0) return -EBADF;
        status = ifuseClose ((char *) newlyCreatedFile->filePath, descInx);
        freeIFuseDesc (descInx);
	bzero (newlyCreatedFile, sizeof (newlyCreatedFile_t));
    }
    return (status);
}

int
getDescInxInNewlyCreatedCache (char *path, int flags)
{
    int descInx = -1;
    int i;
    pthread_mutex_lock (&NewlyCreatedOprLock);
    for (i = 0; i < NUM_NEWLY_CREATED_SLOT; i++) {
        if (strcmp (path, NewlyCreatedFile[i].filePath) == 0) {
	    if ((flags & O_RDWR) == 0 && (flags & O_WRONLY) == 0) {
	        closeNewlyCreatedCache (&NewlyCreatedFile[i]);
	        descInx = -1;
	    } else if (checkFuseDesc (NewlyCreatedFile[i].descInx) >= 0) {
	        descInx = NewlyCreatedFile[i].descInx;
	        bzero (&NewlyCreatedFile[i], sizeof (newlyCreatedFile_t));
	    } else {
	        bzero (&NewlyCreatedFile[i], sizeof (newlyCreatedFile_t));
	        descInx = -1;
	    }
	    break;
	}
    }
    pthread_mutex_unlock (&NewlyCreatedOprLock);
    return descInx;
}

int
fillFileStat (struct stat *stbuf, uint mode, rodsLong_t size, uint ctime,
uint mtime, uint atime)
{
    if (mode >= 0100)
        stbuf->st_mode = S_IFREG | mode;
    else
        stbuf->st_mode = S_IFREG | IQF_DEFAULT_FILE_MODE;
    stbuf->st_size = size;

    stbuf->st_blksize = IQF_FILE_BLOCK_SIZE;
    stbuf->st_blocks = (stbuf->st_size / IQF_FILE_BLOCK_SIZE) + 1;

    stbuf->st_nlink = 1;
    stbuf->st_ino = random ();
    stbuf->st_ctime = ctime;
    stbuf->st_mtime = mtime;
    stbuf->st_atime = atime;
    stbuf->st_uid = getuid();
    stbuf->st_gid = getgid();

    return 0;
}

int
fillDirStat (struct stat *stbuf, uint ctime, uint mtime, uint atime)
{
    stbuf->st_mode = S_IFDIR | IQF_DEFAULT_DIR_MODE;
    stbuf->st_size = IQF_DIR_SIZE;

    stbuf->st_nlink = 2;
    stbuf->st_ino = random ();
    stbuf->st_ctime = ctime;
    stbuf->st_mtime = mtime;
    stbuf->st_atime = atime;
    stbuf->st_uid = getuid();
    stbuf->st_gid = getgid();

    return 0;
}

int 
updatePathCacheStat (pathCache_t *tmpPathCache)
{
    int status;

    if (tmpPathCache->locCacheState != NO_FILE_CACHE &&
      tmpPathCache->locCachePath != NULL) {
	struct stat stbuf;
	status = stat (tmpPathCache->locCachePath, &stbuf);
	if (status < 0) {
	    return (errno ? (-1 * errno) : -1);
	} else {
	    /* update the size */
	    tmpPathCache->stbuf.st_size = stbuf.st_size; 
	    return 0; 
	}
    } else {
	return 0;
    }
}

/* need to call get_iquest_fuse_irods_conn before calling irodsMknodWithCache */
int
irodsMknodWithCache (char *path, mode_t mode, char *cachePath)
{
    int status;
    int fd;

    if ((status = getFileCachePath (path, cachePath)) < 0)
        return status;

    /* fd = creat (cachePath, mode); WRONLY */
    fd = open (cachePath, O_CREAT|O_EXCL|O_RDWR, mode);
    if (fd < 0) {
        rodsLog (LOG_ERROR,
          "irodsMknodWithCache: local cache creat error for %s, errno = %d",
          cachePath, errno);
        return(errno ? (-1 * errno) : -1);
    } else {
        return fd;
    }
}

/* need to call get_iquest_fuse_irods_conn before calling irodsOpenWithReadCache */
int
irodsOpenWithReadCache (iquest_fuse_irods_conn_t *irods_conn, char *path, int flags)
{
    pathCache_t *tmpPathCache = NULL;
    struct stat stbuf;
    int status;
    dataObjInp_t dataObjInp;
    char cachePath[MAX_NAME_LEN];
    int fd, descInx;

    /* do only O_RDONLY (0) */
    if ((flags & (O_WRONLY | O_RDWR)) != 0) return -1;

    if (_iquest_fuse_irods_getattr(irods_conn, path, &stbuf, &tmpPathCache) < 0 ||
      tmpPathCache == NULL) return -1;

    /* too big to cache */
    if (stbuf.st_size > MAX_READ_CACHE_SIZE) return -1;	

    if (tmpPathCache->locCachePath == NULL) {

        rodsLog (LOG_DEBUG, "irodsOpenWithReadCache: caching %s", path);

        memset (&dataObjInp, 0, sizeof (dataObjInp));
        if ((status = getFileCachePath (path, cachePath)) < 0) 
	    return status;
        /* get the file to local cache */
	status = iquest_parse_rods_path_str(irods_conn->iqf, (char *) (path + 1), 
          dataObjInp.objPath);
        if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "irodsOpenWithReadCache: iquest_parse_rods_path_str of %s error", path);
            /* use ENOTDIR for this type of error */
            return -ENOTDIR;
        }
        dataObjInp.openFlags = flags;
        dataObjInp.dataSize = stbuf.st_size;

        status = rcDataObjGet (irods_conn->conn, &dataObjInp, cachePath);

        if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "irodsOpenWithReadCache: rcDataObjGet of %s error", 
	      dataObjInp.objPath);

	    return status; 
	}
        tmpPathCache->locCachePath = strdup (cachePath);
        tmpPathCache->locCacheState = HAVE_READ_CACHE;
    } else {
        rodsLog (LOG_DEBUG, "irodsOpenWithReadCache: read cache match for %s",
          path);
    }

    fd = open (tmpPathCache->locCachePath, flags);
    if (fd < 0) {
        rodsLog (LOG_ERROR,
          "irodsOpenWithReadCache: local cache open error for %s, errno = %d",
          tmpPathCache->locCachePath, errno);
	return(errno ? (-1 * errno) : -1);
    }

    descInx = allocIFuseDesc ();
    if (descInx < 0) {
        rodsLogError (LOG_ERROR, descInx,
          "irodsOpenWithReadCache: allocIFuseDesc of %s error", path);
        close(fd);	// cppcheck - Resource leak: fd
        return -ENOENT;
    }
    fillIFuseDesc (descInx, irods_conn, fd, dataObjInp.objPath,
      (char *) path);
    IFuseDesc[descInx].locCacheState = HAVE_READ_CACHE;

    return descInx;
}

int
getFileCachePath (char *in_path, char *cacehPath)
{
    char myDir[MAX_NAME_LEN], myFile[MAX_NAME_LEN];
    struct stat statbuf;

    if (in_path == NULL || cacehPath == NULL) {
        rodsLog (LOG_ERROR,
          "getFileCachePath: input in_path or cacehPath is NULL");
        return (SYS_INTERNAL_NULL_INPUT_ERR);
    }
    splitPathByKey (in_path, myDir, myFile, '/');

    while (1)
    {
        snprintf (cacehPath, MAX_NAME_LEN, "%s/%s.%d", FuseCacheDir,
          myFile, (int) random ());
        if (stat (cacehPath, &statbuf) < 0) break;
    }
    return 0;
}

int
setAndMkFileCacheDir ()
{
    char *tmpStr, *tmpDir;
    struct passwd *myPasswd;
    int status;

    myPasswd = getpwuid(getuid());

    if ((tmpStr = getenv ("FuseCacheDir")) != NULL && strlen (tmpStr) > 0) {
	tmpDir = tmpStr;
    } else {
	tmpDir = FUSE_CACHE_DIR;
    }

    snprintf (FuseCacheDir, MAX_NAME_LEN, "%s/%s.%d", tmpDir,
      myPasswd->pw_name, getpid());

    if ((status = mkdirR ("/", FuseCacheDir, IQF_DEFAULT_DIR_MODE)) < 0) {
        rodsLog (LOG_ERROR,
          "setAndMkFileCacheDir: mkdirR of %s error. status = %d", 
	  FuseCacheDir, status);
    }

    return (status);

}

int
dataObjCreateByFusePath (iquest_fuse_irods_conn_t *irods_conn, char *path, int mode, 
char *outIrodsPath)
{
    dataObjInp_t dataObjInp;
    int status;
    rcComm_t *conn;
    conn = irods_conn->conn;

    memset (&dataObjInp, 0, sizeof (dataObjInp));
    status = iquest_parse_rods_path_str(irods_conn->iqf, (char *) (path + 1), 
      dataObjInp.objPath);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status,
          "dataObjCreateByFusePath: iquest_parse_rods_path_str of %s error", path);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    if (strlen (irods_conn->iqf->rodsEnv->rodsDefResource) > 0) {
        addKeyVal (&dataObjInp.condInput, RESC_NAME_KW,
          irods_conn->iqf->rodsEnv->rodsDefResource);
    }

    addKeyVal (&dataObjInp.condInput, DATA_TYPE_KW, "generic");
    /* dataObjInp.createMode = DEF_FILE_CREATE_MODE; */
    dataObjInp.createMode = mode;
    dataObjInp.openFlags = O_RDWR;
    dataObjInp.dataSize = -1;

    status = rcDataObjCreate (conn, &dataObjInp);
    clearKeyVal (&dataObjInp.condInput);
    if (status >= 0 && outIrodsPath != NULL)
	rstrcpy (outIrodsPath, dataObjInp.objPath, MAX_NAME_LEN);

    return status;
}

int
getNewlyCreatedDescByPath (char *path)
{
    int i;
    int inuseCnt = 0;

    pthread_mutex_lock (&DescLock);
    for (i = 3; i < MAX_IFUSE_DESC; i++) {
	if (inuseCnt >= IFuseDescInuseCnt) break;
        if (IFuseDesc[i].inuseFlag == IRODS_INUSE) { 
            inuseCnt++;
	    if (IFuseDesc[i].locCacheState != HAVE_NEWLY_CREATED_CACHE ||
	      IFuseDesc[i].localPath == NULL) {
	        continue;
	    }
	    if (strcmp (IFuseDesc[i].localPath, path) == 0) { 
                pthread_mutex_unlock (&DescLock);
                return (i);
            }
	}
    }
    pthread_mutex_unlock (&DescLock);
    return (-1);
}

int renmeOpenedIFuseDesc (iquest_fuse_t *iqf, pathCache_t *fromPathCache, char *to) {
    int descInx;
    int status;
    pathCache_t *tmpPathCache = NULL;

    if ((descInx = getNewlyCreatedDescByPath (
      (char *)fromPathCache->filePath)) >= 3) {
        rmPathFromCache ((char *) to, PathArray);
        rmPathFromCache ((char *) to, NonExistPathArray);
	addPathToCache ((char *) to, PathArray, &fromPathCache->stbuf, 
	  &tmpPathCache);
        tmpPathCache->locCachePath = fromPathCache->locCachePath;
	fromPathCache->locCachePath = NULL;
        tmpPathCache->locCacheState = HAVE_NEWLY_CREATED_CACHE;
	fromPathCache->locCacheState = NO_FILE_CACHE;
	if (IFuseDesc[descInx].objPath != NULL) 
	    free (IFuseDesc[descInx].objPath);
	IFuseDesc[descInx].objPath = (char *) malloc (MAX_NAME_LEN);
        status = iquest_parse_rods_path_str(iqf, (char *) (to + 1),
          IFuseDesc[descInx].objPath);
        if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "renmeOpenedIFuseDesc: iquest_parse_rods_path_str of %s error", to);
            return -ENOTDIR;
        }
	if (IFuseDesc[descInx].localPath != NULL) 
	    free (IFuseDesc[descInx].localPath);
        IFuseDesc[descInx].localPath = strdup (to);
	return 0;
    } else {
	return -ENOTDIR;
    }
}

/*
 * Takes an iRODS error and a fuse_err and returns fuse_err
 * unless irods_err is an authentication error, in which case 
 * it returns an appropriate fuse_err (e.g. -ENOKEY or -EKEYEXPIRED)
 */
int map_irods_auth_errors(int irods_err, int fuse_err) {
  if(irods_err == KRB_ERROR_INIT_SECURITY_CONTEXT) {
    rodsLogError(LOG_DEBUG, irods_err, "map_irods_auth_errors");
    rodsLog(LOG_DEBUG, "map_irods_auth_errors: returning -ENOKEY");
    return(-ENOKEY);
  }
  if(irods_err == SYS_AUTH_EXPIRED) {
    rodsLogError(LOG_DEBUG, irods_err, "map_irods_auth_errors");
    rodsLog(LOG_DEBUG, "map_irods_auth_errors: returning -EKEYEXPIRED");
    return(-EKEYEXPIRED);
  }
  return(fuse_err);
}



int _iquest_fuse_irods_getattr(iquest_fuse_irods_conn_t *irods_conn, const char *path, struct stat *stbuf, pathCache_t **out_pathCache) {
    int status;
    dataObjInp_t dataObjInp;
    rodsObjStat_t *rodsObjStatOut = NULL;
#ifdef CACHE_FUSE_PATH
    pathCache_t *nonExistPathCache;
    pathCache_t *tmpPathCache;
#endif

    rodsLog (LOG_DEBUG, "_iquest_fuse_irods_getattr: %s", path);

#ifdef CACHE_FUSE_PATH 
    if (out_pathCache != NULL) *out_pathCache = NULL;
    if (matchPathInPathCache( (char *) path, NonExistPathArray, &nonExistPathCache) == 1) {
        rodsLog (LOG_DEBUG, "_iquest_fuse_irods_getattr: a match for non existing path %s", 
	  path);
        return -ENOENT;
    }

    if (matchPathInPathCache ((char *) path, PathArray, &tmpPathCache) == 1) {
        rodsLog (LOG_DEBUG, "_iquest_fuse_irods_getattr: a match for path %s", path);
	status = updatePathCacheStat (tmpPathCache);
	if (status < 0) {
	    /* we have a problem */
	    rmPathFromCache ((char *) path, PathArray);
	} else {
	    *stbuf = tmpPathCache->stbuf;
	    if (out_pathCache != NULL) *out_pathCache = tmpPathCache;
	    return (0);
	}
    }
#endif

    memset (stbuf, 0, sizeof (struct stat));
    memset (&dataObjInp, 0, sizeof (dataObjInp));
    status = iquest_parse_rods_path_str(irods_conn->iqf, (char *) (path + 1), dataObjInp.objPath);
    if (status < 0) {
	rodsLogError (LOG_ERROR, status, 
	  "_iquest_fuse_irods_getattr: iquest_parse_rods_path_str of %s error", path);
	/* use ENOTDIR for this type of error */
	return -ENOTDIR;
    }
    rodsLog(LOG_DEBUG, "_iquest_fuse_irods_getattr: calling rcObjStat");
    status = rcObjStat(irods_conn->conn, &dataObjInp, &rodsObjStatOut);
    if (status < 0) {
        if (isReadMsgError (status)) {
	  rodsLogError(LOG_DEBUG, "_iquest_fuse_irods_getattr: rcObjStat of %s error, attempting to reconnect", path);
	  ifuseReconnect (irods_conn);
	  status = rcObjStat (irods_conn->conn, &dataObjInp, &rodsObjStatOut);
	}
	if (status < 0) {
	    if (status != USER_FILE_DOES_NOT_EXIST) {
                rodsLogError (LOG_ERROR, status, 
	          "_iquest_fuse_irods_getattr: rcObjStat of %s error", path);
	    }
#ifdef CACHE_FUSE_PATH
            addPathToCache ((char *) path, NonExistPathArray, stbuf, NULL);
#endif
	    
	    return map_irods_auth_errors(status, -ENOENT);
	}
    }

    if (rodsObjStatOut->objType == COLL_OBJ_T) {
	fillDirStat (stbuf, 
	  atoi (rodsObjStatOut->createTime), atoi (rodsObjStatOut->modifyTime),
	  atoi (rodsObjStatOut->modifyTime));
    } else if (rodsObjStatOut->objType == UNKNOWN_OBJ_T) {
#ifdef CACHE_FUSE_PATH
        addPathToCache ((char *) path, NonExistPathArray, stbuf, NULL);
#endif
        if (rodsObjStatOut != NULL) freeRodsObjStat (rodsObjStatOut);
            return -ENOENT;
    } else {
	fillFileStat (stbuf, rodsObjStatOut->dataMode, rodsObjStatOut->objSize,
	  atoi (rodsObjStatOut->createTime), atoi (rodsObjStatOut->modifyTime),
	  atoi (rodsObjStatOut->modifyTime));
    }

    if (rodsObjStatOut != NULL)
        freeRodsObjStat (rodsObjStatOut);

#ifdef CACHE_FUSE_PATH
    addPathToCache ((char *) path, PathArray, stbuf, out_pathCache);
#endif
    return 0;
}


int iquest_parse_rods_path_str(iquest_fuse_t *iqf, char *in_path, char *out_path) {
  int status;
  rodsLog(LOG_DEBUG, "iquest_parse_rods_path_str: calling parseRodsPathStr in_path=%s", in_path);
  status = parseRodsPathStr(in_path, iqf->rodsEnv, out_path);
  rodsLog(LOG_DEBUG, "iquest_parse_rods_path_str: returned from parseRodsPathStr with out_path=%s status=%i", out_path, status);
  
  return status;
}


#if 0
int queryAndShowStrCond(rcComm_t *conn, char *hint, char *format, 
		    char *selectConditionString, int noDistinctFlag,
                    char *zoneArgument, int noPageFlag)
{
/*
  NoDistinctFlag is 1 if the user is requesting 'distinct' to be skipped.
 */

  genQueryInp_t genQueryInp;
  int i;
  genQueryOut_t *genQueryOut = NULL;

  memset (&genQueryInp, 0, sizeof (genQueryInp_t));
  i = fillGenQueryInpFromStrCond(selectConditionString, &genQueryInp);
  if (i < 0)
    return(i);

  if (noDistinctFlag) {
     genQueryInp.options = NO_DISTINCT;
  }

  if (zoneArgument!=0 && zoneArgument[0]!='\0') {
     addKeyVal (&genQueryInp.condInput, ZONE_KW, zoneArgument);
     printf("Zone is %s\n",zoneArgument);
  }

  genQueryInp.maxRows= MAX_SQL_ROWS;
  genQueryInp.continueInx=0;
  i = rcGenQuery (conn, &genQueryInp, &genQueryOut);
  if (i < 0)
    return(i);

  i = printGenQueryOut(stdout, format, hint, genQueryOut);
  if (i < 0)
    return(i);


  while (i==0 && genQueryOut->continueInx > 0) {
     if (noPageFlag==0) {
	char inbuf[100];
	printf("Continue? [Y/n]");
	fgets(inbuf, 90, stdin);
	if (strncmp(inbuf, "n", 1)==0) break;
     }
     genQueryInp.continueInx=genQueryOut->continueInx;
     i = rcGenQuery (conn, &genQueryInp, &genQueryOut);
     if (i < 0)
	return(i);
     i = printGenQueryOut(stdout, format,hint,  genQueryOut);
     if (i < 0)
	return(i);
  }

  return(0);

}
#endif


int iquest_parse_fuse_path(const char *path, char *rodspath, char *query) {
  char *sep = "\/";
  rodsLog(LOG_DEBUG, "iquestParseFusePath: %s", path);

  //  strtok_r(path, sep, 
  
  return 0;
}

/*
 * ReadDir when no query is present in the path
 */
int iquest_readdir_coll(iquest_fuse_t *iqf, const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    char collPath[MAX_NAME_LEN];
    collHandle_t collHandle;
    collEnt_t collEnt;
    int status = -1;
    int connstat = -1;
#ifdef CACHE_FUSE_PATH
    struct stat stbuf;
    pathCache_t *tmpPathCache;
#endif
    /* don't know why we need this. the example have them */
    (void) offset;
    (void) fi;
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsReaddir: %s", path);

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    status = iquest_parse_rods_path_str(iqf, (char *) (path + 1), collPath);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status, 
	  "irodsReaddir: iquest_parse_rods_path_str of %s error", path);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    connstat = get_iquest_fuse_irods_conn(&irods_conn, iqf);
    if (connstat != 0) return connstat;

    rodsLog (LOG_DEBUG, "irodsReaddir: calling rclOpenCollection for %s", collPath);
    status = rclOpenCollection (irods_conn->conn, collPath, 0, &collHandle);

    if (status < 0) {
        if (isReadMsgError (status)) {
	    ifuseReconnect (irods_conn);
            status = rclOpenCollection (irods_conn->conn, collPath, 0, 
	      &collHandle);
	}
	if (status < 0) {
            rodsLog (LOG_ERROR,
              "irodsReaddir: rclOpenCollection of %s error. status = %d",
              collPath, status);
                relIFuseConn (irods_conn);
		return map_irods_auth_errors(status, -ENOENT);
	}
    }
    while ((status = rclReadCollection (irods_conn->conn, &collHandle, &collEnt))
      >= 0) {
	char myDir[MAX_NAME_LEN], mySubDir[MAX_NAME_LEN];
#ifdef CACHE_FUSE_PATH
	char childPath[MAX_NAME_LEN];

	bzero (&stbuf, sizeof (struct stat));
#endif
        if (collEnt.objType == DATA_OBJ_T) {
	    filler (buf, collEnt.dataName, NULL, 0);
#ifdef CACHE_FUSE_PATH
	    if (strcmp (path, "/") == 0) {
	        snprintf (childPath, MAX_NAME_LEN, "/%s", collEnt.dataName);
	    } else {
	        snprintf (childPath, MAX_NAME_LEN, "%s/%s", 
		  path, collEnt.dataName);
	    }
            if (matchPathInPathCache ((char *) childPath, PathArray, 
	      &tmpPathCache) != 1) {
	        fillFileStat (&stbuf, collEnt.dataMode, collEnt.dataSize,
	          atoi (collEnt.createTime), atoi (collEnt.modifyTime), 
	          atoi (collEnt.modifyTime));
	        addPathToCache (childPath, PathArray, &stbuf, &tmpPathCache);
	    }
#endif
        } else if (collEnt.objType == COLL_OBJ_T) {
	    splitPathByKey (collEnt.collName, myDir, mySubDir, '/');
	    filler (buf, mySubDir, NULL, 0);
#ifdef CACHE_FUSE_PATH
            if (strcmp (path, "/") == 0) {
                snprintf (childPath, MAX_NAME_LEN, "/%s", mySubDir);
            } else {
	        snprintf (childPath, MAX_NAME_LEN, "%s/%s", path, mySubDir);
	    }
            if (matchPathInPathCache ((char *) childPath, PathArray, 
              &tmpPathCache) != 1) {
	        fillDirStat (&stbuf, 
	          atoi (collEnt.createTime), atoi (collEnt.modifyTime), 
	          atoi (collEnt.modifyTime));
	        addPathToCache (childPath, PathArray, &stbuf, &tmpPathCache);
	    }
#endif
        }
    }
    rclCloseCollection (&collHandle);
    relIFuseConn (irods_conn);
    return map_irods_auth_errors(status, 0);
}
