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
 *** Portions of This file are substantially based on irodsFs.c which is   ***
 *** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***
 *****************************************************************************/

/*
 ******************************************************************************
 * Contains the main executable program for iquestFuse, a FUSE-based
 * filesystem that presents a read-only query-based view of iRODS
 ******************************************************************************
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>

#include "iquest_fuse.h"
#include "iquest_fuse_operations.h"
#include "iquest_fuse_lib.h"

static struct fuse_operations iquest_fuse_operations = {
  .init = iquest_fuse_init,
  .destroy = iquest_fuse_destroy,
  .getattr = iquest_fuse_getattr,
  .readdir = iquest_fuse_readdir,
  .open = iquest_fuse_open,
  .read = iquest_fuse_read,
  .release = iquest_fuse_release,
#if 0
  .readlink = iquest_fuse_readlink,
  .mknod = iquest_fuse_mknod,
  .mkdir = iquest_fuse_mkdir,
  .symlink = iquest_fuse_symlink,
  .unlink = iquest_fuse_unlink,
  .rmdir = iquest_fuse_rmdir,
  .rename = iquest_fuse_rename,
  .link = iquest_fuse_link,
  .chmod = iquest_fuse_chmod,
  .chown = iquest_fuse_chown,
  .truncate = iquest_fuse_truncate,
  .utimens = iquest_fuse_utimens,
  .write = iquest_fuse_write,
  .statfs = iquest_fuse_statfs,
  .fsync = iquest_fuse_fsync,
  .flush = iquest_fuse_flush,
#endif
};


/*
 * Options given by IQUEST_FUSE_OPT can take arguments, which are set in the specified field of iquest_fuse_conf
 * Options given by FUSE_OPT_KEY are passed to iquest_fuse_opt_proc for handling (using the given key)
 */
static struct fuse_opt iquest_fuse_opts[] = {
  IQUEST_FUSE_OPT("-q %s",			base_query,	0),
  IQUEST_FUSE_OPT("--query=%s",			base_query,	0),
  IQUEST_FUSE_OPT("query=%s",			base_query,	0),

  IQUEST_FUSE_OPT("-c %s",			irods_cwd,	0),
  IQUEST_FUSE_OPT("--cwd=%s",			irods_cwd,	0),
  IQUEST_FUSE_OPT("cwd=%s",			irods_cwd,	0),

  IQUEST_FUSE_OPT("-i %s",			indicator,	0),
  IQUEST_FUSE_OPT("--indicator=%s",		indicator,	0),
  IQUEST_FUSE_OPT("indicator=%s",		indicator,	0),

  IQUEST_FUSE_OPT("-r %s",			slash_remap,	0),
  IQUEST_FUSE_OPT("--remap-slash-char=%s",	slash_remap,	0),
  IQUEST_FUSE_OPT("remap-slash-char=%s",	slash_remap,	0),

  IQUEST_FUSE_OPT("--require-conn",		require_conn,	1),
  IQUEST_FUSE_OPT("require-conn",		require_conn,	1),
  IQUEST_FUSE_OPT("--no-require-conn",		require_conn,	0),
  IQUEST_FUSE_OPT("no-require-conn",		require_conn,	0),

  IQUEST_FUSE_OPT("--show-indicator",		show_indicator,	1),
  IQUEST_FUSE_OPT("show-indicator",		show_indicator,	1),

  FUSE_OPT_KEY("--debug",        IQUEST_FUSE_CONF_KEY_DEBUG_ME), /* the --debug option is only recongnised by iquestFuse, not FUSE itself */
  FUSE_OPT_KEY("--debug-trace",  IQUEST_FUSE_CONF_KEY_TRACE_ME), 

  FUSE_OPT_KEY("-V",             IQUEST_FUSE_CONF_KEY_VERSION),
  FUSE_OPT_KEY("--version",      IQUEST_FUSE_CONF_KEY_VERSION),

  FUSE_OPT_KEY("-h",             IQUEST_FUSE_CONF_KEY_HELP),
  FUSE_OPT_KEY("--help",         IQUEST_FUSE_CONF_KEY_HELP),

  FUSE_OPT_KEY("-d",             IQUEST_FUSE_CONF_KEY_DEBUG_ALL),
  FUSE_OPT_KEY("debug",          IQUEST_FUSE_CONF_KEY_DEBUG_ALL),

  FUSE_OPT_KEY("-f",             IQUEST_FUSE_CONF_KEY_FOREGROUND),

  FUSE_OPT_KEY("-s",             IQUEST_FUSE_CONF_KEY_SINGLETHREAD),

  FUSE_OPT_END
};

/*
 * print usage to stderr
 */
void usage(char *progname) {
  fprintf(stderr,
	  "usage: %s mountpoint [options]\n"
	  "\n"
	  "general options:\n"
	  "    -o opt,[opt...]  mount options\n"
	  "    -h   --help      print help\n"
	  "    -V   --version   print version\n"
	  "\n"
	  "iquestFuse options:\n"
	  "  POSIX options:       GNU long options:             Mount options (within -o):\n"
	  "    -q base-query        --query=base-query            query=base-query\n"
	  "    -i query-indicator   --indicator=query-indicator   indicator=query-indicator\n"
	  "    -c irods-cwd         --cwd=irods-cwd               cwd=irods-cwd\n"
	  "    -r c                 --remap-slash-char=c          remap-slash-char=c\n"
	  "                         --require-conn                require-conn\n"
	  "                         --show-indicator              show-indicator\n"
	  "\n"
	  , progname);
}

int iquest_fuse_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs) {
  fprintf(stderr, "iquest_fuse_opt_proc: arg=%s key=%d\n", arg, key);
  iquest_fuse_conf_t *conf = data;
  switch(key) {
  case IQUEST_FUSE_CONF_KEY_HELP:
    usage(outargs->argv[0]);
        fuse_opt_add_arg(outargs, "-ho"); /* ask fuse to print help without header */
    fuse_main(outargs->argc, outargs->argv, &iquest_fuse_operations, NULL);
    exit(1);
  case IQUEST_FUSE_CONF_KEY_VERSION:
    fprintf(stderr, "iquestFuse version %s\n", PACKAGE_VERSION);
    fuse_opt_add_arg(outargs, "--version"); /* pass this along to FUSE for their version */
    fuse_main(outargs->argc, outargs->argv, &iquest_fuse_operations, NULL);
    exit(0);
  case IQUEST_FUSE_CONF_KEY_DEBUG_ME:
    conf->debug_level = LOG_DEBUG;
    fuse_opt_add_arg(outargs, "-f"); /* ask fuse to stay in foreground */
    break;
  case IQUEST_FUSE_CONF_KEY_TRACE_ME:
    conf->debug_level = LOG_DEBUG1;
    fuse_opt_add_arg(outargs, "-f"); /* ask fuse to stay in foreground */
    break;
  case IQUEST_FUSE_CONF_KEY_DEBUG_ALL:
    conf->debug_level = LOG_DEBUG;
    /* fall through as DEBUG implies FOREGROUND */
  case IQUEST_FUSE_CONF_KEY_FOREGROUND:
    /* could set foreground operation flag if we care */
    return 1; /* -d and -f need to be processed by FUSE */
  case IQUEST_FUSE_CONF_KEY_SINGLETHREAD:
    //TODO disable pthreads here???
    return 1; /* -s needs to be processed by FUSE */
 default:
   return 1; /* anything not recognised should be processed by FUSE */
  }
  /* anything that breaks out of the switch will NOT be processed by FUSE */
  return 0;
}


int main(int argc, char **argv) {
  int status;

  /* 
   * seed random number generator
   * TODO: what uses this?
   */ 
  srandom((unsigned int) time(0) % getpid());

  /*
   * iquest_fuse data that will be passed in to FUSE operations
   */
  static iquest_fuse_t iquest_fuse;
  memset(&iquest_fuse, 0, sizeof(iquest_fuse));

  static iquest_fuse_conf_t iquest_fuse_conf;
  memset(&iquest_fuse_conf, 0, sizeof(iquest_fuse_conf));
  iquest_fuse.conf = &iquest_fuse_conf;

  static rodsEnv myRodsEnv;
  iquest_fuse.rods_env = &myRodsEnv;

  iquest_fuse_t *iqf = &iquest_fuse;
  
  /*
   * Use FUSE option processing to process argc/argv
   */
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

  /*
   * Set defaults for dynamically allocated conf fields that have not been set by options
   */
  if(iqf->conf->indicator == NULL) {
    int bufsize = strlen(IQF_DEFAULT_INDICATOR)+1;
    iqf->conf->indicator = malloc(bufsize);
    if(iqf->conf->indicator == NULL) {
      fprintf(stderr, "main: malloc(%d) setting default for iqf->conf->indicator\n", bufsize);
      exit(2);
    }
    strncpy(iqf->conf->indicator, IQF_DEFAULT_INDICATOR, bufsize);
  }
  
  if(iqf->conf->slash_remap == NULL) {
    int bufsize = strlen(IQF_DEFAULT_SLASH_REMAP)+1;
    iqf->conf->slash_remap = malloc(bufsize);
    if(iqf->conf->slash_remap == NULL) {
      fprintf(stderr, "main: malloc(%d) setting default for iqf->conf->slash_remap\n", bufsize);
      exit(2);
    }
    strncpy(iqf->conf->slash_remap, IQF_DEFAULT_SLASH_REMAP, bufsize);
  }
  
  /*
   * Set configuration in iquest_fuse_conf from command-line options and 
   * call iquest_fuse_opt_proc for other options
   */
  fuse_opt_parse(&args, &iquest_fuse_conf, iquest_fuse_opts, iquest_fuse_opt_proc);

  
  if(iqf->conf->irods_cwd != NULL) {
    rodsLog(LOG_NOTICE, "using iRODS cwd %s", iqf->conf->irods_cwd);
    status = setenv("irodsCwd", iqf->conf->irods_cwd, 1);
    if( status < 0) {
      rodsLog(LOG_ERROR, "main: could not set irodsCwd environment variable");
    }
  }
  
  if(iqf->conf->base_query != NULL) {
    rodsLog(LOG_NOTICE, "using base query %s", iqf->conf->base_query);
  }
  
  if(iqf->conf->indicator != NULL) {
    rodsLog(LOG_NOTICE, "using indicator %s", iqf->conf->indicator);
  }
  
  if(iqf->conf->slash_remap != NULL) {
    rodsLog(LOG_NOTICE, "remapping slashes [/] in metadata to [%s]", iqf->conf->slash_remap);
  }

  if(iqf->conf->show_indicator > 0) {
    rodsLog(LOG_SYS_WARNING, "showing query indicator (%s) in directory listings -- you must not attempt to recursively list directories (e.g. don't use find, du, ls -R, etc)", iqf->conf->indicator);
  }
  

#ifdef CACHE_FILE_FOR_READ
  if (setAndMkFileCacheDir () < 0) exit(3);
#endif
  
  /*
   * get iRODS configuration from environment and conf file (~/.irods/.irodsEnv)
   */
  status = getRodsEnv(iqf->rods_env);
  if( status < 0 ) {
    rodsLogError(LOG_ERROR, status, "main: getRodsEnv error. ");
    exit(1);
  }


  /* 
   * override irodsLogLevel if debug_level is greater
   */
  if(iqf->conf->debug_level > iqf->rods_env->rodsLogLevel) {
    fprintf(stderr, "overriding rodsLogLevel to debug_level=%d\n", iqf->conf->debug_level);
    rodsLogLevel(iqf->conf->debug_level);
    iqf->rods_env->rodsLogLevel = iqf->conf->debug_level;
  }
  
  initPathCache ();
  initIFuseDesc ();
  

  /*
   * Pass control to FUSE (which will call iquest_fuse_init and then start event loop 
   */
  rodsLog(LOG_DEBUG, "calling fuse_main");
  status = fuse_main (args.argc, args.argv, &iquest_fuse_operations, &iquest_fuse);
  rodsLog(LOG_DEBUG, "back from fuse_main");
  
  disconnect_all(iqf);
  
  if (status < 0) {
    rodsLogError(LOG_ERROR, status, "fuse_main");
    exit(5);
  } else {
    exit(0);
  }
}

