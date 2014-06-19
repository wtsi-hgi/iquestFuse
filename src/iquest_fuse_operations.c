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
 * Implementations of FUSE filesystem operations for iquestFuse.
 *****************************************************************************/

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <assert.h>

#include "iquest_fuse.h"
#include "iquest_fuse_operations.h"
#include "iquest_fuse_lib.h"

#include "miscUtil.h"

//extern iquest_fuse_irods_conn_t *ConnHead;

extern iFuseDesc_t IFuseDesc[];
extern pathCacheQue_t NonExistPathArray[];
extern pathCacheQue_t PathArray[];


void *iquest_fuse_init(struct fuse_conn_info *conn) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;
  
  if(iqf->conf->require_conn > 0) {
    /*
     * Try to make an iRODS connection now and optionally exit with error if it is not connected
     */
    int connstat = -1;
    iquest_fuse_irods_conn_t *irods_conn = NULL;
    connstat = get_iquest_fuse_irods_conn(&irods_conn, iqf);
    if(connstat != 0) {
      rodsLogError(LOG_ERROR, connstat, "iquest_fuse_init");
      rodsLog(LOG_ERROR, "iRODS connection failure and require-conn option in effect");
      exit(4);
    }
  }
  
  return iqf;
}

void iquest_fuse_destroy(void *data) {
  iquest_fuse_t *iqf = data;
  rodsLog(LOG_DEBUG, "iquest_fuse_destroy: destroying iquest_fuse");
  iquest_fuse_t_destroy(iqf);
}

int iquest_fuse_getattr(const char *path, struct stat *stbuf) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;
  char coll_path[MAX_NAME_LEN];
  char zone_hint[MAX_NAME_LEN];
  char *coll = NULL;
  iquest_fuse_query_cond_t *query_cond = NULL;
  char *query_part_attr = NULL;
  char *pqpath = NULL;
  int status = -1;
  char *_path;
  int query_mode;

  /* make local copy of path */
  _path = strdup(path);
  query_mode = iquest_parse_fuse_path(iqf, _path, &coll, &query_cond, &query_part_attr, &pqpath);

  /* canonicalize path */
  status = iquest_parse_rods_path_str(iqf, coll, coll_path);
  if( status != 0 ) {
    rodsLogError(LOG_ERROR, status, "iquest_fuse_getattr: iquest_parse_rods_path_str");
    return status;
  }
  rodsLog(LOG_DEBUG, "iquest_fuse_getattr: iquest_parse_rods_path_str returned coll_path [%s]", coll_path);
  
  /* get zone from coll_path */
  status = iquest_zone_hint_from_rods_path(iqf, coll_path, zone_hint);
  if( status != 0 ) {
    rodsLogError(LOG_ERROR, status, "iquest_fuse_getattr: iquest_zone_hint_from_rods_path");
    return status;
  }
  rodsLog(LOG_DEBUG, "iquest_fuse_getattr: iquest_zone_hint_from_rods_path returned zone_hint [%s]", zone_hint);

  if( query_mode < -1) {
    /* error */
    rodsLog(LOG_ERROR, "iquest_fuse_getattr: error parsing path");
    status = query_mode;
  } else if( query_mode >= 2 ) {
    /* partial query: (Q was the last thing specified) */
    rodsLog(LOG_DEBUG, "iquest_fuse_getattr: path contains a partial query");
    /* Q is always a dir */
    fill_dir_stat(stbuf, 0, 0, 0); 
    status = 0;
  } else if( query_mode >= 1) {
    /* partial query: have attribute but no value */
    rodsLog(LOG_DEBUG, "iquest_fuse_getattr: path contains a partial query (have attribute but no value)");
    status = iquest_query_attr_exists(iqf, zone_hint, query_cond, query_part_attr);
    if( status != 0 ) {
      rodsLogError(LOG_DEBUG, status, "iquest_fuse_getattr: attr [%s] does not exist", query_part_attr);
    } else {
      /* attr exists */
      rodsLog(LOG_DEBUG, "iquest_fuse_getattr: attr [%s] exists", query_part_attr);
      fill_dir_stat(stbuf, 0, 0, 0); 
    }
  } else if( query_mode <= 0 ) {
    /* completed or no query */
    if( query_cond->where_cond->len >= 1 ) {
      /* had one or more complete queries in path */
       if( strcmp(pqpath, "") == 0 ) {
	 /* have completed query with no post-query path (e.g. this is the query value specification) */
	 rodsLog(LOG_DEBUG, "iquest_fuse_getattr: path contains [%d] complete queries and nothing more", query_cond->where_cond->len);
	 //TODO check if query value is valid
	 fill_dir_stat(stbuf, 0, 0, 0); 
	 status = 0;
       } else {
	 rodsLog(LOG_DEBUG, "iquest_fuse_getattr: path contains [%d] complete queries and a post-query path", query_cond->where_cond->len);
	 //TODO get the actual query results and the real data object stats
	 //fill_dir_stat(stbuf, 0, 0, 0);
	 fill_file_stat(stbuf, 0, 0, 0, 0, 0); 
	 /*
	   fill_file_stat(stbuf, rodsObjStatOut->dataMode, rodsObjStatOut->objSize,
	   atoi (rodsObjStatOut->createTime), atoi (rodsObjStatOut->modifyTime),
	   atoi (rodsObjStatOut->modifyTime));
	 */
	 status = 0;
       }
     } else {
      /* no query in path */
      rodsLog(LOG_DEBUG, "iquest_fuse_getattr: path does not contain any queries");
      
      int connstat = -1;
      iquest_fuse_irods_conn_t *irods_conn = NULL;  
      connstat = get_iquest_fuse_irods_conn(&irods_conn, iqf);
      if(connstat != 0) return connstat;
      
      rodsLog(LOG_DEBUG, "iquest_fuse_getattr: calling _iquest_fuse_irods_getattr");
      status = _iquest_fuse_irods_getattr(irods_conn, path, stbuf, NULL);
      relIFuseConn(irods_conn);
    }
  } else {
    rodsLog(LOG_ERROR, "iquest_fuse_getattr: reached an impossible point");
    status = -1;
  }
  free(coll);
  free(query_cond);
  free(query_part_attr);
  free(pqpath);
  return(status);
}

int iquest_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;

  char coll_path[MAX_NAME_LEN];
  char zone_hint[MAX_NAME_LEN];
  collHandle_t collHandle;
  collEnt_t collEnt;
  int status = -1;
  int connstat = -1;
  struct stat stbuf;
#ifdef CACHE_FUSE_PATH
  pathCache_t *tmpPathCache;
#endif
  /* don't know why we need this. the example have them */
  //    (void) offset;
  //    (void) fi;
  iquest_fuse_irods_conn_t *irods_conn = NULL;
  char *coll = NULL; 
  iquest_fuse_query_cond_t *query_cond = NULL;
  char *query_part_attr = NULL;
  char *pqpath = NULL;
  char *_path;
  int query_mode;
  
  rodsLog (LOG_DEBUG, "iquest_fuse_readdir: %s", path);

  /* make local copy of path */
  _path = strdup(path);

  /* parse fuse path into query parts, irods collection, etc */
  query_mode = iquest_parse_fuse_path(iqf, _path, &coll, &query_cond, &query_part_attr, &pqpath);
  rodsLog(LOG_DEBUG, "iquest_fuse_readdir: back from iquest_parse_fuse_path with coll [%s] query_part_attr [%s] pqpath [%s]", coll, query_part_attr, pqpath);

  status = iquest_parse_rods_path_str(iqf, coll, coll_path);
  if( status != 0 ) {
    rodsLogError(LOG_ERROR, status, "iquest_fuse_readdir: iquest_parse_rods_path_str");
    return status;
  }
  rodsLog(LOG_DEBUG, "iquest_fuse_readdir: iquest_parse_rods_path_str returned coll_path [%s]", coll_path);

  status = iquest_zone_hint_from_rods_path(iqf, coll_path, zone_hint);
  if( status != 0 ) {
    rodsLogError(LOG_ERROR, status, "iquest_fuse_readdir: iquest_zone_hint_from_rods_path");
    return status;
  }
  rodsLog(LOG_DEBUG, "iquest_fuse_readdir: iquest_zone_hint_from_rods_path returned zone_hint [%s]", zone_hint);

  if( query_mode < -1) {
    /* error */
    rodsLog(LOG_ERROR, "iquest_fuse_readdir: error parsing path");
    free(coll);
    free(query_cond);
    free(query_part_attr);
    free(pqpath);
    free(_path);
    return query_mode;
  } else if( query_mode >= 2 ) {
    /* partial query: listing of possible attributes */
    rodsLog(LOG_DEBUG, "iquest_fuse_readdir: path contains a partial query, listing possible attributes");
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    iquest_query_and_fill_attr_list(iqf, zone_hint, query_cond, buf, filler);
    
    free(coll);
    free(query_cond);
    free(query_part_attr);
    free(pqpath);
    free(_path);
    return 0;
  } else if( query_mode >= 1) {
    /* partial query: listing of possible values */
    rodsLog(LOG_DEBUG, "iquest_fuse_readdir: path contains a partial query, listing possible values");
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    
    rodsLog(LOG_DEBUG, "iquest_fuse_readdir: calling iquest_query_and_fill_value_list with query_part_attr [%s]", query_part_attr);
    iquest_query_and_fill_value_list(iqf, zone_hint, query_cond, query_part_attr, buf, filler);

    free(coll);
    free(query_cond);
    free(query_part_attr);
    free(pqpath);
    free(_path);
    return 0;
  } else if( query_mode <= 0 && query_cond->where_cond->len > 0 ) {
    /* completed query */
    rodsLog(LOG_DEBUG, "iquest_fuse_readdir: path contains a complete query");
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    /* add query indicator to directory listing if show_indicator is set */
    if(iqf->conf->show_indicator > 0) {
      bzero(&stbuf, sizeof (struct stat));
      fill_dir_stat(&stbuf, 1349696964, 1349696964, 1349696964);
      filler(buf, iqf->conf->indicator, &stbuf, 0);
    }

    //TODO 

    free(coll);
    free(query_cond);
    free(query_part_attr);
    free(pqpath);
    free(_path);
    return 0;
  } else if(query_mode <= 0 && query_cond->where_cond->len == 0 ) {
    /* no query in path */
    rodsLog(LOG_DEBUG, "iquest_fuse_readdir: path does not contain a query");
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    
    /* add query indicator to directory listing if show_indicator is set */
    if(iqf->conf->show_indicator > 0) {
      if(strcmp(coll_path, IQF_PATH_SEP) == 0) {
	rodsLog(LOG_DEBUG, "iquest_fuse_readdir: skipping showing of query indicator in root (don't support root queries yet)");
      } else {
	bzero(&stbuf, sizeof (struct stat));
	fill_dir_stat(&stbuf, 1349696964, 1349696964, 1349696964);
	filler(buf, iqf->conf->indicator, &stbuf, 0);
      }
    }

    //    strcpy(_path, path+1);
    //    status = iquest_parse_rods_path_str(iqf, _path, coll_path);  
    free(coll);
    free(query_cond);
    free(query_part_attr);
    free(pqpath);
    free(_path);
    if (status < 0) {
      rodsLogError (LOG_ERROR, status, 
		    "iquest_fuse_readdir: iquest_parse_rods_path_str of %s error", _path);
      /* use ENOTDIR for this type of error */
      return -ENOTDIR;
    }
    
    connstat = get_iquest_fuse_irods_conn(&irods_conn, iqf);
    if (connstat != 0) return connstat;
    
    rodsLog (LOG_DEBUG, "iquest_fuse_readdir: calling rclOpenCollection for %s", coll_path);
    status = rclOpenCollection (irods_conn->conn, coll_path, DATA_QUERY_FIRST_FG, &collHandle);

    if (status < 0) {
      if (isReadMsgError (status)) {
	ifuseReconnect (irods_conn);
	status = rclOpenCollection (irods_conn->conn, coll_path, DATA_QUERY_FIRST_FG, 
				    &collHandle);
      }
      if (status < 0) {
	rodsLog (LOG_ERROR,
		 "iquest_fuse_readdir: rclOpenCollection of %s error. status = %d",
		 coll_path, status);
	relIFuseConn (irods_conn);
	return map_irods_auth_errors(status, -ENOENT);
      }
    }

    while ((status = rclReadCollection(irods_conn->conn, &collHandle, &collEnt)) >= 0) {
      char myDir[MAX_NAME_LEN], mySubDir[MAX_NAME_LEN];
#ifdef CACHE_FUSE_PATH
      char childPath[MAX_NAME_LEN];

      bzero (&stbuf, sizeof (struct stat));
#endif
      if (collEnt.objType == DATA_OBJ_T) {
	status = filler(buf, collEnt.dataName, NULL, 0);
	if(status != 0) {
	  rodsLog( LOG_ERROR, "iquest_fuse_readdir: filler error" );
	  status = -ENOMEM;
	  break;
	}
#ifdef CACHE_FUSE_PATH
	if (strcmp (path, IQF_PATH_SEP) == 0) {
	  snprintf (childPath, MAX_NAME_LEN, "/%s", collEnt.dataName);
	} else {
	  snprintf (childPath, MAX_NAME_LEN, "%s/%s", 
		    path, collEnt.dataName);
	}
	if (matchPathInPathCache ((char *) childPath, PathArray, 
				  &tmpPathCache) != 1) {
	  fill_file_stat(&stbuf, collEnt.dataMode, collEnt.dataSize,
			atoi (collEnt.createTime), atoi (collEnt.modifyTime), 
			atoi (collEnt.modifyTime));
	  addPathToCache (childPath, PathArray, &stbuf, &tmpPathCache);
	}
#endif
      } else { // if (collEnt.objType == COLL_OBJ_T) {
	splitPathByKey (collEnt.collName, myDir, mySubDir, '/');
	if(strcmp(mySubDir,"") != 0) {
	  status = filler(buf, mySubDir, NULL, 0);
	  if(status != 0) {
	    rodsLog( LOG_ERROR, "iquest_fuse_readdir: filler error" );
	    status = -ENOMEM;
	    break;
	  }
	  rodsLog(LOG_DEBUG, "iquest_fuse_readdir: filler succeeded for [%s]", mySubDir);
	} else {
	  rodsLog(LOG_DEBUG, "iquest_fuse_readdir: skipping empty mySubDir [%s] myDir [%s]", mySubDir, myDir);
	}
#ifdef CACHE_FUSE_PATH
	if (strcmp (path, IQF_PATH_SEP) == 0) {
	  snprintf (childPath, MAX_NAME_LEN, "/%s", mySubDir);
	} else {
	  snprintf (childPath, MAX_NAME_LEN, "%s/%s", path, mySubDir);
	}
	if (matchPathInPathCache ((char *) childPath, PathArray, 
				  &tmpPathCache) != 1) {
	  fill_dir_stat (&stbuf, 
		       atoi (collEnt.createTime), atoi (collEnt.modifyTime), 
		       atoi (collEnt.modifyTime));
	  rodsLog(LOG_DEBUG, "iquest_fuse_readdir: calling addPathToCache childPath [%s]", childPath);
	  addPathToCache (childPath, PathArray, &stbuf, &tmpPathCache);
	}
#endif
      }
    }
    rclCloseCollection (&collHandle);
    relIFuseConn (irods_conn);
    if(status >= 0 || status == CAT_NO_ROWS_FOUND) {
      rodsLog(LOG_DEBUG, "iquest_fuse_readdir: success");
      return 0;
    } 
    status = map_irods_auth_errors(status, -ENOENT);
    rodsLogError(LOG_DEBUG, status, "iquest_fuse_readdir");
    return status;
  }
  rodsLog(LOG_DEBUG, "iquest_fuse_readdir: returning -1");
  return -1;
}

int iquest_fuse_open(const char *path, struct fuse_file_info *fi) {
  iquest_fuse_t *iqf = fuse_get_context()->private_data;
  dataObjInp_t dataObjInp;
  int status = -1;
  int connstat = -1;
  int fd;
  int descInx;
  iquest_fuse_irods_conn_t *irods_conn = NULL;
  
  rodsLog (LOG_DEBUG, "iquest_fuse_open: %s, flags = %d", path, fi->flags);
  
#ifdef CACHE_FUSE_PATH
  if ((descInx = getDescInxInNewlyCreatedCache ((char *) path, fi->flags)) 
      > 0) {
    rodsLog (LOG_DEBUG, "iquest_fuse_open: a match for %s", path);
    fi->fh = descInx;
    return (0);
  }
#endif
  connstat = get_iquest_fuse_irods_conn_by_path(&irods_conn, iqf, (char *) path);
  if (connstat != 0) return connstat;

#ifdef CACHE_FILE_FOR_READ
  if ((descInx = iquest_fuse_open_with_read_cache (irods_conn, 
					 (char *) path, fi->flags)) > 0) {
    rodsLog (LOG_DEBUG, "iquest_fuse_open: a match for %s", path);
    fi->fh = descInx;
    relIFuseConn (irods_conn);
    return (0);
  }
#endif
  memset (&dataObjInp, 0, sizeof (dataObjInp));
  status = iquest_parse_rods_path_str(iqf, (char *) (path + 1), dataObjInp.objPath);
  if (status < 0) {
    rodsLogError (LOG_ERROR, status, 
		  "iquest_fuse_open: iquest_parse_rods_path_str of %s error", path);
    /* use ENOTDIR for this type of error */
    relIFuseConn (irods_conn);
    return -ENOTDIR;
  }
  
  dataObjInp.openFlags = fi->flags;
  /*
    char objPath[MAX_NAME_LEN];
    int createMode;
    int openFlags;
    rodsLong_t offset;
    rodsLong_t dataSize;
    int numThreads;
    int oprType;
    specColl_t *specColl;
    keyValPair_t condInput;
  */
  rodsLog(LOG_DEBUG, "iquest_fuse_open: calling rcDataObjOpen with objPath=%s createMode=%d openFlags=%d", dataObjInp.objPath, dataObjInp.createMode, dataObjInp.openFlags);
  fd = rcDataObjOpen(irods_conn->conn, &dataObjInp);
  rodsLog(LOG_DEBUG, "iquest_fuse_open: back from rcDataObjOpen with fd=%d", fd);
  
  if (fd < 0) {
    if (isReadMsgError (fd)) {
      ifuseReconnect (irods_conn);
      fd = rcDataObjOpen (irods_conn->conn, &dataObjInp);
    }
    relIFuseConn (irods_conn);
    if (fd < 0) {
      rodsLogError (LOG_ERROR, status,
		    "iquest_fuse_open: rcDataObjOpen of %s error, status = %d", path, fd);
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
		  "iquest_fuse_open: allocIFuseDesc of %s error", path);
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


int iquest_fuse_release(const char *path, struct fuse_file_info *fi) {
  int descInx;
  int status, myError;
  
  rodsLog(LOG_DEBUG, "iquest_fuse_release: %s", path);
  
  descInx = fi->fh;
  
  if (checkFuseDesc(descInx) < 0) {
    return(-EBADF);
  }
  
  status = ifuseClose((char *) path, descInx);
  
  freeIFuseDesc(descInx);
  
  if (status < 0) {
    if ((myError = getErrno (status)) > 0) {
      return(-myError);
    } else {
      return(-ENOENT);
    }
  } else {
    return(0);
  }
}
