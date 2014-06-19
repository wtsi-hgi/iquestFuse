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
 *** Portions of This file are substantially based on iFuseOper.c which is ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*****************************************************************************
 * iquestFuseOperations.c
 * 
 * Implementations of FUSE filesystem operations for iquestFuse.
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


void *iquest_fuse_init(struct fuse_conn_info *conn) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;
  return iqf;
}

void iquest_fuse_destroy(void *data) {
  iquest_fuse_t *iqf = data;
  rodsLog(LOG_DEBUG, "iquest_fuse_destroy: destroying iquest_fuse");
  iquest_fuse_t_destroy(iqf);
}

int iquest_fuse_getattr(const char *path, struct stat *stbuf) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;
  
  int connstat = -1;
  iquest_fuse_irods_conn_t *irods_conn = NULL;  
  connstat = get_iquest_fuse_irods_conn(&irods_conn, iqf);
  if(connstat != 0) return connstat;

  int status = -1;
  rodsLog(LOG_DEBUG, "iquest_fuse_getattr: calling _iquest_fuse_irods_getattr");
  status = _iquest_fuse_irods_getattr(irods_conn, path, stbuf, NULL);
  relIFuseConn(irods_conn);
  return(status);
}

int iquest_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;

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
  //    (void) offset;
  //    (void) fi;
  iquest_fuse_irods_conn_t *irods_conn = NULL;
  
  rodsLog (LOG_DEBUG, "irodsReaddir: %s", path);
  char *coll, *query;
  iquest_parse_fuse_path(path, coll, query);
  
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

int iquest_fuse_open(const char *path, struct fuse_file_info *fi) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;
  dataObjInp_t dataObjInp;
  int status = -1;
  int connstat = -1;
  int fd;
  int descInx;
  iquest_fuse_irods_conn_t *irods_conn = NULL;
  
  rodsLog (LOG_DEBUG, "irodsOpen: %s, flags = %d", path, fi->flags);
  
#ifdef CACHE_FUSE_PATH
  if ((descInx = getDescInxInNewlyCreatedCache ((char *) path, fi->flags)) 
      > 0) {
    rodsLog (LOG_DEBUG, "irodsOpen: a match for %s", path);
    fi->fh = descInx;
    return (0);
  }
#endif
  connstat = get_iquest_fuse_irods_conn_by_path(&irods_conn, iqf, (char *) path);
  if (connstat != 0) return connstat;
#ifdef CACHE_FILE_FOR_READ
  if ((descInx = irodsOpenWithReadCache (irods_conn, 
					 (char *) path, fi->flags)) > 0) {
    rodsLog (LOG_DEBUG, "irodsOpen: a match for %s", path);
    fi->fh = descInx;
    relIFuseConn (irods_conn);
    return (0);
  }
#endif
  memset (&dataObjInp, 0, sizeof (dataObjInp));
  status = iquest_parse_rods_path_str(iqf, (char *) (path + 1) , dataObjInp.objPath);
  if (status < 0) {
    rodsLogError (LOG_ERROR, status, 
		  "irodsOpen: iquest_parse_rods_path_str of %s error", path);
    /* use ENOTDIR for this type of error */
    relIFuseConn (irods_conn);
    return -ENOTDIR;
  }
  
  dataObjInp.openFlags = fi->flags;
  
  fd = rcDataObjOpen (irods_conn->conn, &dataObjInp);
  
  if (fd < 0) {
    if (isReadMsgError (fd)) {
      ifuseReconnect (irods_conn);
      fd = rcDataObjOpen (irods_conn->conn, &dataObjInp);
    }
    relIFuseConn (irods_conn);
    if (fd < 0) {
      rodsLogError (LOG_ERROR, status,
		    "irodsOpen: rcDataObjOpen of %s error, status = %d", path, fd);
      return map_irods_auth_errors(status, -ENOENT);
    }
  }
#ifdef CACHE_FUSE_PATH
  rmPathFromCache ((char *) path, NonExistPathArray);
#endif
  descInx = allocIFuseDesc ();
  if (descInx < 0) {
    relIFuseConn (irods_conn);
    rodsLogError (LOG_ERROR, descInx,
		  "irodsOpen: allocIFuseDesc of %s error", path);
    return -ENOENT;
  }
  fillIFuseDesc (descInx, irods_conn, fd, dataObjInp.objPath, 
		 (char *) path);
  relIFuseConn (irods_conn);
  fi->fh = descInx;
  return(0);
}

int iquest_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  int descInx;
  int status, myError;
  
  rodsLog (LOG_DEBUG, "irodsRead: %s", path);
  
  descInx = fi->fh;
  
  if (checkFuseDesc (descInx) < 0) {
    return -EBADF;
  }
  lockDesc (descInx);
  if ((status = ifuseLseek ((char *) path, descInx, offset)) < 0) {
    unlockDesc (descInx);
    if ((myError = getErrno (status)) > 0) {
      return (-myError);
    } else {
      return -ENOENT;
    }
  }
  
  if (size <= 0) {
    unlockDesc (descInx);
	return 0;
  }
  
  status = ifuseRead ((char *) path, descInx, buf, size, offset);
  
  unlockDesc (descInx);
  
  return status;
}


#if 0

int 
irodsReadlink (const char *path, char *buf, size_t size)
{
    rodsLog (LOG_DEBUG, "irodsReadlink: %s", path);
    return (0);
}


int 
irodsMknod (const char *path, mode_t mode, dev_t rdev)
{
#ifdef CACHE_FUSE_PATH
    int descInx;
    pathCache_t *tmpPathCache = NULL;
#endif
    struct stat stbuf;
    int status = -1;
    int connstat = -1;
#ifdef CACHE_FILE_FOR_NEWLY_CREATED
    char cachePath[MAX_NAME_LEN];
#endif
    char irodsPath[MAX_NAME_LEN];
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsMknod: %s", path);


    if (iquest_fuse_getattr(path, &stbuf) >= 0)
        return -EEXIST;

#ifdef CACHE_FILE_FOR_NEWLY_CREATED
    status = irodsMknodWithCache ((char *)path, mode, cachePath);
    irodsPath[0] = '\0';
#endif 	/* CACHE_FILE_FOR_NEWLY_CREATED */
    connstat = get_iquest_fuse_irods_conn(&irods_conn, iqf);
    if (connstat != 0) return connstat;

    if (status < 0) {
	status = dataObjCreateByFusePath (irods_conn, (char *) path, 
	  mode, irodsPath);

        if (status < 0) {
            if (isReadMsgError (status)) {
		ifuseReconnect (irods_conn);
	        status = dataObjCreateByFusePath (irods_conn, 
		  (char *) path, mode, irodsPath);
	    }
	    if (status < 0) {
                rodsLogError (LOG_ERROR, status,
                  "irodsMknod: rcDataObjCreate of %s error", path);
                relIFuseConn (irods_conn);
                return -ENOENT;
	    }
	}
    }
#ifdef CACHE_FUSE_PATH
    rmPathFromCache ((char *) path, NonExistPathArray);
    descInx = allocIFuseDesc ();

    if (descInx < 0) {
        rodsLogError (LOG_ERROR, descInx,
          "irodsMknod: allocIFuseDesc of %s error", path);
	closeIrodsFd (irods_conn, status);
        relIFuseConn (irods_conn);
        return 0;
    }
    fillIFuseDesc (descInx, irods_conn, status, irodsPath,
      (char *) path);
    relIFuseConn (irods_conn);
    addNewlyCreatedToCache ((char *) path, descInx, mode, &tmpPathCache);
#ifdef CACHE_FILE_FOR_NEWLY_CREATED
    tmpPathCache->locCachePath = strdup (cachePath);
    tmpPathCache->locCacheState = HAVE_NEWLY_CREATED_CACHE;
    IFuseDesc[descInx].locCacheState = HAVE_NEWLY_CREATED_CACHE;
    IFuseDesc[descInx].createMode = mode;
#endif

#else   /* CACHE_FUSE_PATH */ 
    closeIrodsFd (irods_conn, status);
    relIFuseConn (irods_conn);
#endif  /* CACHE_FUSE_PATH */ 

    return (0);
}

int 
irodsMkdir (const char *path, mode_t mode)
{
    collInp_t collCreateInp;
    int status = -1;
    int connstat = -1;
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsMkdir: %s", path);

    memset (&collCreateInp, 0, sizeof (collCreateInp));

    status = iquest_parse_rods_path_str(iqf, (char *) (path + 1) , collCreateInp.collName);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status, 
	  "irodsMkdir: iquest_parse_rods_path_str of %s error", path);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    connstat = get_iquest_fuse_irods_conn (&irods_conn, iqf);
    if (connstat != 0) return connstat;
    status = rcCollCreate (irods_conn->conn, &collCreateInp);

    if (status < 0) {
	if (isReadMsgError (status)) {
	    ifuseReconnect (irods_conn);
            status = rcCollCreate (irods_conn->conn, &collCreateInp);
	}
        relIFuseConn (irods_conn);
	if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "irodsMkdir: rcCollCreate of %s error", path);
	    return map_irods_auth_errors(status, -ENOENT);
	}
#ifdef CACHE_FUSE_PATH
    } else {
	struct stat stbuf;
	uint mytime = time (0);
	bzero (&stbuf, sizeof (struct stat));
        fillDirStat (&stbuf, mytime, mytime, mytime);
        addPathToCache ((char *) path, PathArray, &stbuf, NULL);
	rmPathFromCache ((char *) path, NonExistPathArray);
#endif
        relIFuseConn (irods_conn);
    }

    return (0);
}

int 
irodsUnlink (const char *path)
{
    dataObjInp_t dataObjInp;
    int status = -1;
    int connstat = -1;
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsUnlink: %s", path);

    memset (&dataObjInp, 0, sizeof (dataObjInp));

    status = iquest_parse_rods_path_str(iqf, (char *) (path + 1) , dataObjInp.objPath);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status, 
	  "irodsUnlink: iquest_parse_rods_path_str of %s error", path);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    addKeyVal (&dataObjInp.condInput, FORCE_FLAG_KW, "");

    connstat = get_iquest_fuse_irods_conn (&irods_conn, iqf);
    if (connstat != 0) return connstat;
    status = rcDataObjUnlink (irods_conn->conn, &dataObjInp);
    if (status >= 0) {
#ifdef CACHE_FUSE_PATH
	rmPathFromCache ((char *) path, PathArray);
#endif
	status = 0;
    } else {
	if (isReadMsgError (status)) {
	    ifuseReconnect (irods_conn);
            status = rcDataObjUnlink (irods_conn->conn, &dataObjInp);
	}
	if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "irodsUnlink: rcDataObjUnlink of %s error", path);
	    status = map_irods_auth_errors(status, -ENOENT);
	}
    } 
    relIFuseConn (irods_conn);

    clearKeyVal (&dataObjInp.condInput);

    return (status);
}

int 
irodsRmdir (const char *path)
{
    collInp_t collInp;
    int status = -1;
    int connstat  = -1;
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsRmdir: %s", path);

    memset (&collInp, 0, sizeof (collInp));

    status = iquest_parse_rods_path_str(iqf, (char *) (path + 1) , collInp.collName);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status, 
	  "irodsRmdir: iquest_parse_rods_path_str of %s error", path);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    addKeyVal (&collInp.condInput, FORCE_FLAG_KW, "");

    connstat = get_iquest_fuse_irods_conn (&irods_conn, iqf);
    if (connstat != 0) return connstat;
    status = rcRmColl (irods_conn->conn, &collInp, 0);
    if (status >= 0) {
#ifdef CACHE_FUSE_PATH
        rmPathFromCache ((char *) path, PathArray);
#endif
        status = 0;
    } else {
	if (isReadMsgError (status)) {
	    ifuseReconnect (irods_conn);
            status = rcRmColl (irods_conn->conn, &collInp, 0);
	}
	if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "irodsRmdir: rcRmColl of %s error", path);
	    status = map_irods_auth_errors(status, -ENOENT);
	}
    }

    relIFuseConn (irods_conn);

    clearKeyVal (&collInp.condInput);

    return (status);
}

int 
irodsSymlink (const char *from, const char *to)
{
    rodsLog (LOG_DEBUG, "irodsSymlink: %s to %s", from, to);
    return (0);
}

int 
irodsRename (const char *from, const char *to)
{
    dataObjCopyInp_t dataObjRenameInp;
    int status = -1;
    int connstat = -1;
    pathCache_t *fromPathCache;
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsRename: %s to %s", from, to);

#ifdef CACHE_FILE_FOR_NEWLY_CREATED
    if (matchPathInPathCache ((char *) from, PathArray, &fromPathCache) == 1 &&
      fromPathCache->locCacheState == HAVE_NEWLY_CREATED_CACHE &&
      fromPathCache->locCachePath != NULL) {
        status = renmeOpenedIFuseDesc (fromPathCache, (char *) to);
        if (status >= 0) {
	    rmPathFromCache ((char *) from, PathArray);
            return (0);
        }
    }
#endif

    /* test rcDataObjRename */

    memset (&dataObjRenameInp, 0, sizeof (dataObjRenameInp));

    status = iquest_parse_rods_path_str(iqf, (char *) (from + 1) , dataObjRenameInp.srcDataObjInp.objPath);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status, 
	  "irodsRename: iquest_parse_rods_path_str of %s error", from);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    status = iquest_parse_rods_path_str(iqf, (char *) (to + 1) , dataObjRenameInp.destDataObjInp.objPath);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status, 
	  "irodsRename: iquest_parse_rods_path_str of %s error", to);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    addKeyVal (&dataObjRenameInp.destDataObjInp.condInput, FORCE_FLAG_KW, "");

    dataObjRenameInp.srcDataObjInp.oprType =
      dataObjRenameInp.destDataObjInp.oprType = RENAME_UNKNOWN_TYPE;

    connstat = get_iquest_fuse_irods_conn (&irods_conn, iqf);
    if (connstat != 0) return connstat;
    status = rcDataObjRename (irods_conn->conn, &dataObjRenameInp);

    if (status == CAT_NAME_EXISTS_AS_DATAOBJ || 
      status == SYS_DEST_SPEC_COLL_SUB_EXIST) {
        rcDataObjUnlink (irods_conn->conn, &dataObjRenameInp.destDataObjInp);
        status = rcDataObjRename (irods_conn->conn, &dataObjRenameInp);
    }

    if (status >= 0) {
#ifdef CACHE_FUSE_PATH
	pathCache_t *tmpPathCache;
        rmPathFromCache ((char *) to, PathArray);
        if (matchPathInPathCache ((char *) from, PathArray,
          &tmpPathCache) == 1) {
	    addPathToCache ((char *) to, PathArray, &tmpPathCache->stbuf,
	      &tmpPathCache);
            rmPathFromCache ((char *) from, PathArray);
	}
	rmPathFromCache ((char *) to, NonExistPathArray);
#endif
        status = 0;
    } else {
	if (isReadMsgError (status)) {
	    ifuseReconnect (irods_conn);
            status = rcDataObjRename (irods_conn->conn, &dataObjRenameInp);
	}
	if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "irodsRename: rcDataObjRename of %s to %s error", from, to);
	    status = map_irods_auth_errors(status, -ENOENT);
	}
    }
    relIFuseConn (irods_conn);

    return (status);
}

int 
irodsLink (const char *from, const char *to)
{
    rodsLog (LOG_DEBUG, "irodsLink: %s to %s");
    return (0);
}

int 
irodsChmod (const char *path, mode_t mode)
{
    int status = -1;
    int connstat = -1;
    modDataObjMeta_t modDataObjMetaInp;
    keyValPair_t regParam;
    dataObjInfo_t dataObjInfo;
    char dataMode[SHORT_STR_LEN];
    int descInx;
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsChmod: %s", path);

#ifdef CACHE_FILE_FOR_NEWLY_CREATED
    if ((descInx = getNewlyCreatedDescByPath ((char *)path)) >= 3) {
	/* has not actually been created yet */
	IFuseDesc[descInx].createMode = mode;
	return (0);
    }
#endif
    memset (&regParam, 0, sizeof (regParam));
    snprintf (dataMode, SHORT_STR_LEN, "%d", mode);
    addKeyVal (&regParam, DATA_MODE_KW, dataMode);
    addKeyVal (&regParam, ALL_KW, "");

    memset(&dataObjInfo, 0, sizeof(dataObjInfo));

    status = iquest_parse_rods_path_str(iqf, (char *) (path + 1) , dataObjInfo.objPath);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status,
          "irodsChmod: iquest_parse_rods_path_str of %s error", path);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    modDataObjMetaInp.regParam = &regParam;
    modDataObjMetaInp.dataObjInfo = &dataObjInfo;

    connstat = get_iquest_fuse_irods_conn (&irods_conn, iqf);
    if (connstat != 0) return connstat;
    status = rcModDataObjMeta(irods_conn->conn, &modDataObjMetaInp);
    if (status >= 0) {
#ifdef CACHE_FUSE_PATH
        pathCache_t *tmpPathCache;

        if (matchPathInPathCache ((char *) path, PathArray,
          &tmpPathCache) == 1) {
            tmpPathCache->stbuf.st_mode &= 0xfffffe00;
	    tmpPathCache->stbuf.st_mode |= (mode & 0777);
	}
#endif
        status = 0;
    } else {
	if (isReadMsgError (status)) {
	    ifuseReconnect (irods_conn);
            status = rcModDataObjMeta(irods_conn->conn, &modDataObjMetaInp);
	}
	if (status < 0) {
            rodsLogError(LOG_ERROR, status, 
	      "irodsChmod: rcModDataObjMeta failure");
	    status = map_irods_auth_errors(status, -ENOENT);
	}
    }

    relIFuseConn (irods_conn);
    clearKeyVal (&regParam);

    return(status);
}

int 
irodsChown (const char *path, uid_t uid, gid_t gid)
{
    rodsLog (LOG_DEBUG, "irodsChown: %s", path);
    return (0);
}

int 
irodsTruncate (const char *path, off_t size)
{
    dataObjInp_t dataObjInp;
    int status = -1;
    int connstat = -1;
    pathCache_t *tmpPathCache;
    iquest_fuse_irods_conn_t *irods_conn = NULL;

    rodsLog (LOG_DEBUG, "irodsTruncate: %s", path);

#ifdef CACHE_FILE_FOR_NEWLY_CREATED
    if (matchPathInPathCache ((char *) path, PathArray, &tmpPathCache) == 1 &&
      tmpPathCache->locCacheState == HAVE_NEWLY_CREATED_CACHE &&
      tmpPathCache->locCachePath != NULL) {
        status = truncate (tmpPathCache->locCachePath, size);
	if (status >= 0) {
	    updatePathCacheStat (tmpPathCache);
	    return (0);
	}
    }
#endif

    memset (&dataObjInp, 0, sizeof (dataObjInp));
    status = iquest_parse_rods_path_str(iqf, (char *) (path + 1) , dataObjInp.objPath);
    if (status < 0) {
        rodsLogError (LOG_ERROR, status, 
	  "irodsTruncate: iquest_parse_rods_path_str of %s error", path);
        /* use ENOTDIR for this type of error */
        return -ENOTDIR;
    }

    dataObjInp.dataSize = size;

    connstat = get_iquest_fuse_irods_conn (&irods_conn, iqf);
    if (connstat != 0) return connstat;
    status = rcDataObjTruncate (irods_conn->conn, &dataObjInp);
    if (status >= 0) {
#ifdef CACHE_FUSE_PATH
        pathCache_t *tmpPathCache;

        if (matchPathInPathCache ((char *) path, PathArray,
          &tmpPathCache) == 1) {
            tmpPathCache->stbuf.st_size = size;
        }
#endif
        status = 0;
    } else {
	if (isReadMsgError (status)) {
	    ifuseReconnect (irods_conn);
            status = rcDataObjTruncate (irods_conn->conn, &dataObjInp);
	}
	if (status < 0) {
            rodsLogError (LOG_ERROR, status,
              "irodsTruncate: rcDataObjTruncate of %s error", path);
	    status = map_irods_auth_errors(status, -ENOENT);
	}
    }
    relIFuseConn (irods_conn);

    return (status);
}

int 
irodsFlush (const char *path, struct fuse_file_info *fi)
{
    rodsLog (LOG_DEBUG, "irodsFlush: %s", path);
    return (0);
}

int 
irodsUtimens (const char *path, const struct timespec ts[2])
{
    rodsLog (LOG_DEBUG, "irodsUtimens: %s", path);
    return (0);
}



int 
irodsWrite (const char *path, const char *buf, size_t size, off_t offset, 
struct fuse_file_info *fi)
{
    int descInx;
    int status, myError;

    rodsLog (LOG_DEBUG, "irodsWrite: %s", path);

    descInx = fi->fh;

    if (checkFuseDesc (descInx) < 0) {
        return -EBADF;
    }

    lockDesc (descInx);
    if ((status = ifuseLseek ((char *) path, descInx, offset)) < 0) {
        unlockDesc (descInx);
        if ((myError = getErrno (status)) > 0) {
            return (-myError);
        } else {
            return -ENOENT;
        }
    }

    if (size <= 0) {
        unlockDesc (descInx);
        return 0;
    }

    status = ifuseWrite ((char *) path, descInx, (char *)buf, size, offset);
    unlockDesc (descInx);

    return status;
}

int 
irodsStatfs (const char *path, struct statvfs *stbuf)
{
    int status;

    rodsLog (LOG_DEBUG, "irodsStatfs: %s", path);

    if (stbuf == NULL)
	return (0);

    
    /* just fake some number */
    status = statvfs ("/", stbuf);

    stbuf->f_bsize = IQF_FILE_BLOCK_SIZE;
    stbuf->f_blocks = 2000000000;
    stbuf->f_bfree = stbuf->f_bavail = 1000000000;
    stbuf->f_files = 200000000;
    stbuf->f_ffree = stbuf->f_favail = 100000000;
    stbuf->f_fsid = 777;
    stbuf->f_namemax = MAX_NAME_LEN;

    return (0);
}

int 
irodsRelease (const char *path, struct fuse_file_info *fi)
{
    int descInx;
    int status, myError;

    rodsLog (LOG_DEBUG, "irodsRelease: %s", path);

    descInx = fi->fh;

    if (checkFuseDesc (descInx) < 0) {
        return -EBADF;
    }

    status = ifuseClose ((char *) path, descInx);

    freeIFuseDesc (descInx);

    if (status < 0) {
        if ((myError = getErrno (status)) > 0) {
            return (-myError);
        } else {
            return -ENOENT;
        }
    } else {
        return (0);
    }
}

int 
irodsFsync (const char *path, int isdatasync, struct fuse_file_info *fi)
{
    rodsLog (LOG_DEBUG, "irodsFsync: %s", path);
    return (0);
}

#endif
