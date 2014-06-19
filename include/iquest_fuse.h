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
 *** Portions of This file are substantially based on irodsFs.h which is   ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*****************************************************************************
 * Header for main program for iquestFuse.
 *****************************************************************************/
#ifndef IQUEST_FUSE_H
#define IQUEST_FUSE_H

/* 
 * PACKAGE_VERSION (TODO: move to autoconf)
 */ 
#define PACKAGE_VERSION "0.1"

/* 
 * Set FUSE API version before including fuse.h
 */
#define FUSE_USE_VERSION 26

/*
 * Includes for FUSE 
 * Documentation at: http://fuse.sourceforge.net/doxygen/fuse_8h.html
 */
#include <fuse.h>

/* 
 * offsetof macro
 */
#include <stddef.h>

/*
 * Includes for iRODS clients
 */
#include "rodsClient.h"


//TODO unsure whether these are reqd
//#include "parseCommandLine.h"
#include "rodsPath.h"



/*
 * iquestFuse Constants
 */
#define IQF_FILE_BLOCK_SIZE	512
#define IQF_DIR_SIZE		4096
#define IQF_DEFAULT_FILE_MODE	0660
#define IQF_DEFAULT_DIR_MODE	0770
#define IQF_DEFAULT_INDICATOR   "Q"
#define IQF_DEFAULT_SLASH_REMAP "\\"
#define IQF_PATH_SEP            "/"

#define IQF_CONN_TIMEOUT	120	/* 2 min connection timeout */
#define IQF_CONN_MANAGER_SLEEP_TIME 60
#define IQF_CONN_REQ_SLEEP_TIME 30

/* 
 * iquestFuse Types
 */

/* 
 * anything here that needs cleaning up after should be added to 
 * iquest_fuse_conf_t_destroy (in iquestFuseHelper.c)
 */
typedef struct iquest_fuse_conf {
  char *base_query;
  char *irods_cwd; 
  char *indicator; 
  char *slash_remap;
  int require_conn; /* >0 if an iRODS connection is required at startup */
  int show_indicator; /* >0 if we should include the query indicator in directory listings */
  int debug_level; 
} iquest_fuse_conf_t;


typedef struct iquest_fuse_irods_conn {
  rcComm_t *conn;    //TODO change to rcComm
  pthread_mutex_t lock;
  time_t actTime;
  int inuseCnt;
  int pendingCnt;
  int status;
  struct iquest_fuse *iqf;
  struct iquest_fuse_irods_conn *next;
} iquest_fuse_irods_conn_t;

/* 
 * anything here that needs cleaning up after should be added to 
 * iquest_fuse_t_destroy (in iquestFuseHelper.c)
 */
typedef struct iquest_fuse {
  iquest_fuse_conf_t *conf;
  //iquest_fuse_irods_conn_t *irods_conn;
  iquest_fuse_irods_conn_t *irods_conn_head;
  rodsEnv *rodsEnv;
} iquest_fuse_t;


typedef struct iquest_fuse_query_cond {
  inxValPair_t *where_cond;
  keyValPair_t *cond;
} iquest_fuse_query_cond_t;

/*
 * Command line options/args
 */

#define IQUEST_FUSE_OPT(t, p, v) { t, offsetof(iquest_fuse_conf_t, p), v }

/* iquest_fuse_opt_proc keys */
enum {
  IQUEST_FUSE_CONF_KEY_HELP,
  IQUEST_FUSE_CONF_KEY_VERSION,
  IQUEST_FUSE_CONF_KEY_DEBUG_ALL,
  IQUEST_FUSE_CONF_KEY_FOREGROUND,
  IQUEST_FUSE_CONF_KEY_SINGLETHREAD,
  IQUEST_FUSE_CONF_KEY_DEBUG_ME,
  IQUEST_FUSE_CONF_KEY_TRACE_ME,
};


/*
 * Function Prototypes
 */
int main (int argc, char **argv);
void usage(char *progname);
int iquest_fuse_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs);


#endif	/* IQUEST_FUSE_H */

