/***************************************************************************
 * miniseed2dmc.c
 *
 * A program to send specified files of Mini-SEED records to the IRIS
 * DMC.  A record of the data sent is maintained internally and by
 * using the state file option allows for incomplete data transfers to
 * be resumed between program restarts.
 *
 * A summary of the data sent is printed when the program quits.
 *
 * The directory separator is assumed to be '/'.
 *
 * Chad Trabant, IRIS Data Management Center
 ***************************************************************************/

/* _GNU_SOURCE needed to get pread() under Linux */
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <signal.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <regex.h>
#include <ctype.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <libmseed.h>
#include <libdali.h>

#include "rbtree.h"
#include "stack.h"
#include "seedutil.h"
#include "edir.h"

#define PACKAGE "miniseed2dmc"
#define VERSION "2008.251"

/* Maximum filename length including path */
#define MAX_FILENAME_LENGTH 512

/* The FileKey and FileNode structures form the key and data elements
 * of a balanced tree that is used to keep track of all files being
 * processed.
 */

/* Structure used as the key for B-tree of file entries (FileNode) */
typedef struct filekey {
  ino_t inode;           /* Inode number, assumed to be unsigned */
  char filename[1];      /* File name, sized appropriately */
} FileKey;

/* Structure used as the data for B-tree of file entries */
typedef struct filenode {
  off_t offset;          /* Last file read offset, must be signed */
} FileNode;

/* Linkable structure to hold input file list */
typedef struct FileLink_s {
  struct FileLink_s *next;
  int accesserr;
  char filename[1];
} FileLink;


static FileLink *inputlist = 0;    /* Linked list of input files */
static char   *listfile    = 0;    /* Input list file */
static RBTree *filetree    = 0;    /* Working list of scanned files in a tree */

static char  stopsig       = 0;    /* Stop/termination signal */
static int   verbose       = 0;    /* Verbosity level */
static int   writeack      = 0;    /* Flag to control the request for write acks */

static char  maxrecur      = -1;   /* Maximum level of directory recursion */
static int   iostats       = 0;    /* Output IO stats */
static int   quiet         = 0;    /* Quiet mode */
static int   stateint      = 300;  /* State saving interval in seconds */
static char *statefile     = 0;    /* State file for saving/restoring time stamps */

static int scanfiles (char *targetdir, char *basedir, int level, time_t scantime);
static FileNode *findfile (FileKey *fkey);
static FileNode *addfile (ino_t inode, char *filename, time_t modtime);
static off_t  processfile (char *filename, FileNode *fnode, off_t newsize, time_t newmodtime);
static void   prunefiles (time_t scantime);
static void   printfilelist (FILE *fd);
static int    savestate (char *statefile);
static int    recoverstate (char *statefile);
static int    sendrecord (char *record, int reclen);
static int    processparam (int argcount, char **argvec);
static char  *getoptval (int argcount, char **argvec, int argopt);
static void   adddir (char *dirname);
static void   processdirfile (char *filename);
static int    keycompare (const void *a, const void *b);
static time_t calcdaytime (int year, int day);
static time_t budfiledaytime (char *filename);
static void   term_handler();
static void   print_handler();
static void   lprintf0 (char *message);
static int    lprintf (int level, const char *fmt, ...);
static void   usage ();

static DLCP *dlconn;

int
main (int argc, char** argv)
{
  FileLink *flp;
  time_t scantime, statetime;
  struct timespec tstart;
  double iostatsinterval;
  
  /* Signal handling using POSIX routines */
  struct sigaction sa;
  
  sa.sa_flags = SA_RESTART;
  sigemptyset(&sa.sa_mask);
  
  sa.sa_handler = print_handler;
  sigaction(SIGUSR1, &sa, NULL);
  
  sa.sa_handler = term_handler;
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
  
  sa.sa_handler = SIG_IGN;
  sigaction(SIGHUP, &sa, NULL);
  sigaction(SIGPIPE, &sa, NULL);
  
  filetree = RBTreeCreate (keycompare, free, free);
    
  /* Process command line parameters */
  if (processparam (argc, argv) < 0)
    return 1;
  
  /* Connect to server */
  if ( dl_connect (dlconn) < 0 )
    {
      fprintf (stderr, "Error connecting to server\n");
      return -1;
    }
  
  if ( iostats )
    iostatscount = iostats;
  
  /* Start scan sequence */
  while ( stopsig == 0 )
    {
      dlp = dirlist;
      
      scantime = time(NULL);
      
      scanrecordsread = 0;
      
      if ( iostats && iostats == iostatscount )
	{
	  gettimeofday (&scanstarttime, NULL);
	  scanfileschecked = 0;
	  scanfilesread = 0;
	  scanrecordswritten = 0;
	}
      
      while ( dlp != 0 && stopsig == 0 )
	{
	  /* Check for base directory existence */
	  if ( access (dlp->dirname, R_OK) )
	    {
	      /* Only log error if this is a change */
	      if ( dlp->accesserr == 0 )
		lprintf (0, "Cannot read base directory %s: %s", dlp->dirname, strerror(errno));
	      
	      dlp->accesserr = 1;
	    }
	  else
	    {
	      if ( dlp->accesserr == 1 )
		dlp->accesserr = 0;
	      
	      if ( scanfiles (dlp->dirname, dlp->dirname, maxrecur, scantime) == -2 )
		stopsig = 1;
	    }
	  
	  dlp = dlp->next;
	}
      
      if ( stopsig == 0 )
	{
	  /* Prune files that were not found from the filelist */
	  prunefiles (scantime);
	  
	  /* Save intermediate state file */
	  if ( statefile && stateint && (scantime - statetime) > stateint )
	    {
	      savestate (statefile);
	      statetime = scantime;
	    }
	  
	  /* Re-read directory list file */
	  if ( dirfile && dirfilecount == 0 )
	    {
	      processdirfile (dirfile->filename);
	      dirfilecount = DIRFILEINT;
	    }
	  else
	    {
	      dirfilecount--;
	    }
	  
	  /* Reset the next new flag, the first scan is now complete */
	  if ( nextnew ) nextnew = 0;
	  
	  /* Sleep for specified interval */
	  if ( scansleep )
	    nanosleep (&treq, &trem);

	  /* Sleep for specified interval if no records were read */
	  if ( scansleep0 && scanrecordsread == 0 )
	    nanosleep (&treq0, &trem);
	}
      
      if ( iostats && iostatscount <= 1 )
	{
          /* Determine run time since scanstarttime was set */
	  gettimeofday (&scanendtime, NULL);
	  
	  iostatsinterval = ( ((double)scanendtime.tv_sec + (double)scanendtime.tv_usec/1000000) -
			      ((double)scanstarttime.tv_sec + (double)scanstarttime.tv_usec/1000000) );
	  
	  lprintf (0, "STAT: Time: %g seconds for %d scan(s) (%g seconds/scan)",
                   iostatsinterval, iostats, iostatsinterval/iostats);
	  lprintf (0, "STAT: Files checked: %d, read: %d (%g read/sec)",
		   scanfileschecked, scanfilesread, scanfilesread/iostatsinterval);
	  lprintf (0, "STAT: Records read: %d, written: %d (%g written/sec)",
		   scanrecordsread, scanrecordswritten, scanrecordswritten/iostatsinterval);

          /* Reset counter */
          iostatscount = iostats;
	}
      else if ( iostats )
        {
          iostatscount--;
        }

      /* Quit if only doing one scan */
      if ( onescan )
	break;
    } /* End of main scan sequence */
  
  /* Shut down the connection to the server */
  if ( dlconn->link != -1 )
    dl_disconnect (dlconn);
  
  /* Save the state file */
  if ( statefile )
    savestate (statefile);
  
  return 0;
}  /* End of main() */


/***************************************************************************
 * scanfiles:
 *
 * Scan a directory and recursively drop into sub-directories up to the 
 * maximum recursion level.
 *
 * When initially called the targetdir and basedir should be the same,
 * either an absolute or relative path.  During directory recursion
 * these values are maintained such that they are either absolute
 * paths or targetdir is a relatively path from the current working
 * directory (which changes during recursion) and basedir is a
 * relative path from the initial directory.
 *
 * If the FileNode->offset of an existing entry is -1 skip the file,
 * this happens when there was trouble reading the file, the contents
 * were not Mini-SEED on a previous attempt, etc.
 *
 * Return 0 on success and -1 on error and -2 on fatal error.
 ***************************************************************************/
static int
scanfiles (char *targetdir, char *basedir, int level, time_t scantime)
{
  static int dlevel = 0;
  FileNode *fnode;
  FileKey *fkey;
  char filekeybuf[sizeof(FileKey)+MAX_FILENAME_LENGTH]; /* Room for fkey */
  struct stat st;
  struct dirent *de;
  int rrootdir;
  EDIR *dir;
  
  fkey = (FileKey *) &filekeybuf;
  
  lprintf (3, "Processing directory '%s'", basedir);
  
  if ( (rrootdir = open (".", O_RDONLY, 0)) == -1 )
    {
      if ( ! (stopsig && errno == EINTR) )
	lprintf (0, "Error opening current working directory");
      return -1;
    }
  
  if ( chdir (targetdir) )
    {
      if ( ! (stopsig && errno == EINTR) )
	lprintf (0, "Cannot change to directory %s: %s", targetdir, strerror(errno));
      close (rrootdir);
      return -1;
    }
  
  if ( (dir = eopendir (".")) == NULL )
    {
      lprintf (0, "Cannot open directory %s: %s", targetdir, strerror(errno));
      
      if ( fchdir (rrootdir) )
	{
	  lprintf (0, "Cannot change to relative root directory: %s", strerror(errno));
	}
      
      close (rrootdir);
      return -1;
    }
  
  while ( stopsig == 0 && (de = ereaddir(dir)) != NULL )
    {
      int filenamelen;
      
      /* Skip "." and ".." entries */
      if ( !strcmp(de->d_name, ".") || !strcmp(de->d_name, "..") )
	continue;
      
      /* BUD file name latency check */
      if ( budlatency )
	{
	  time_t budfiletime = budfiledaytime (de->d_name);
	  
	  /* Skip this file if the BUD file name is more than budlatency days old */
	  if ( budfiletime && ((currentday-budfiletime) > (budlatency * 86400)) )
	    {
	      if ( verbose > 1 )
		lprintf (0, "Skipping due to BUD file name latency: %s", de->d_name);
	      continue;
	    }
	}
      
      /* Build a FileKey for this file */
      fkey->inode = de->d_ino;
      filenamelen = snprintf (fkey->filename, sizeof(filekeybuf) - sizeof(FileKey),
			      "%s/%s", basedir, de->d_name);
      
      /* Make sure the filename was not truncated */
      if ( filenamelen >= (sizeof(filekeybuf) - sizeof(FileKey) - 1) )
	{
	  lprintf (0, "Directory entry name beyond maximum of %d characters, skipping:",
		   (sizeof(filekeybuf) - sizeof(FileKey) - 1));
	  lprintf (0, "  %s", de->d_name);
	  continue;
	}
      
      /* Search for a matching entry in the filetree */
      if ( (fnode = findfile (fkey)) )
	{
	  /* Check if the file is permanently skipped */
	  if ( fnode->offset == -1 )
	    {
	      fnode->scantime = scantime;
	      continue;
	    }
	  
	  /* Check if file has triggered a delayed check */
	  if ( fnode->idledelay > 0 )
	    {
	      fnode->scantime = scantime;
	      fnode->idledelay--;
	      continue;
	    }
	}
      
      /* Stat the file */
      if ( lstat (de->d_name, &st) < 0 )
	{
	  if ( ! (stopsig && errno == EINTR) )
	    lprintf (0, "Cannot stat %s: %s", fkey->filename, strerror(errno));
	  continue;
	}
      
      /* If symbolic link stat the real file, if it's a broken link continue */
      if ( S_ISLNK(st.st_mode) )
	{
	  if ( stat (de->d_name, &st) < 0 )
	    {
	      /* Interruption signals when the stop signal is set should break out */
	      if ( stopsig && errno == EINTR )
		break;
	      
	      /* Log an error if the error is anything but a disconnected link */
	      if ( errno != ENOENT )
		lprintf (0, "Cannot stat %s: %s", fkey->filename, strerror(errno));
	      else
		continue;
	    }
	}
      
      /* If directory recurse up to the limit */
      if ( S_ISDIR(st.st_mode) )
	{
	  if ( dlevel < level )
	    {
	      lprintf (4, "Recursing into %s", fkey->filename);
	      
	      dlevel++;
	      if ( scanfiles (de->d_name, fkey->filename, level, scantime) == -2 )
		return -2;
	      dlevel--;
	    }
	  continue;
	}
      
      /* Increment files found counter */
      if ( iostats )
	scanfileschecked++;
      
      /* Do regex matching if an expression was specified */
      if ( fnmatch != 0 )
	if ( regexec (fnmatch, de->d_name, (size_t) 0, NULL, 0) != 0 )
	  continue;
      
      /* Do regex rejecting if an expression was specified */
      if ( fnreject != 0 )
	if ( regexec (fnreject, de->d_name, (size_t) 0, NULL, 0) == 0 )
	  continue;
      
      /* Sanity check for a regular file */
      if ( !S_ISREG(st.st_mode) )
	{
	  lprintf (0, "%s is not a regular file", fkey->filename);
	  continue;
	}
      
      /* Sanity check that the dirent inode and stat inode are the same */
      if ( st.st_ino != de->d_ino )
	{
	  lprintf (0, "Inode numbers from dirent and stat do not match for %s\n", fkey->filename);
	  lprintf (0, "  dirent: %llu  VS  stat: %llu\n",
		   (unsigned long long int) de->d_ino, (unsigned long long int) st.st_ino);
	  continue;
	}
      
      lprintf (3, "Checking file %s", fkey->filename);
      
      /* If the file has never been seen add it to the list */
      if ( ! fnode )
	{
	  /* Add new file to tree setting modtime to one second in the
	   * past so we are triggered to look at this file the first time. */
	  if ( ! (fnode = addfile (fkey->inode, fkey->filename, st.st_mtime - 1)) )
	    {
	      lprintf (0, "Error adding %s to file list", fkey->filename);
	      continue;
	    }
        }
	  
      /* Only update the offset if skipping the first scan, otherwise process */
      if ( nextnew )
        fnode->offset = st.st_size;
      
      /* Check if the file is quiet and mark to always skip if true */
      if ( quietsec && st.st_mtime < (scantime - quietsec) )
	{
	  lprintf (2, "Marking file as quiet, no processing: %s", fkey->filename);
	  fnode->offset = -1;
	}
      
      /* Otherwise check if the file is idle and set idledelay appropriately */
      else if ( idledelay && fnode->idledelay == 0 && st.st_mtime < (scantime - idlesec) )
	{
	  lprintf (2, "Marking file as idle, will not check for %d scans: %s",
		   idledelay, fkey->filename);
	  fnode->idledelay = (idledelay > 0) ? (idledelay-1) : 0;
	}
      
      /* Process (read records from) the file if it's modification time has increased,
	 it's size has increased and is not marked for permanent skipping */
      if ( fnode->modtime < st.st_mtime &&
	   fnode->offset < st.st_size &&
	   fnode->offset != -1 )
	{
	  /* Increment files read counter */
	  if ( iostats )
	    scanfilesread++;
	  
	  fnode->offset = processfile (fkey->filename, fnode, st.st_size, st.st_mtime);
	  
	  /* If a proper file read but fatal error occured set the offset correctly
	     and return a fatal error. */
	  if ( fnode->offset < -1 )
	    {
	      fnode->offset = -fnode->offset;
	      return -2;
	    }
	}
      
      /* Update scantime */
      fnode->scantime = scantime;
    }
  
  eclosedir (dir);
  
  if ( fchdir (rrootdir) )
    {
      if ( ! (stopsig && errno == EINTR) )
	lprintf (0, "Cannot change to relative root directory: %s",
		 strerror(errno));
    }
  
  close (rrootdir);
  
  return 0;
}  /* End of scanfiles() */


/***************************************************************************
 * findfile:
 *
 * Search the filetree for a given FileKey.
 *
 * Return a pointer to a FileNode if found or 0 if no match found.
 ***************************************************************************/
static FileNode*
findfile (FileKey *fkey)
{
  FileNode *fnode = 0;
  RBNode *tnode;
  
  /* Search for a matching inode + file name entry */
  if ( (tnode = RBFind (filetree, fkey)) )
    {
      fnode = (FileNode *)tnode->data;
    }
  
  return fnode;
}  /* End of findfile() */


/***************************************************************************
 * addfile:
 *
 * Add a file to the file tree, no checking is done to determine if
 * this entry already exists.
 *
 * Return a pointer to the added FileNode on success and 0 on error.
 ***************************************************************************/
static FileNode*
addfile (ino_t inode, char *filename, time_t modtime)
{
  FileKey *newfkey;
  FileNode *newfnode;
  size_t filelen;
  
  lprintf (1, "Adding %s", filename);
  
  /* Create new tree key */
  filelen = strlen (filename);
  newfkey = (FileKey *) malloc (sizeof(FileKey)+filelen);
  
  /* Create new tree node */
  newfnode = (FileNode *) malloc (sizeof(FileNode));
  
  if ( ! newfkey || ! newfnode )
    return 0;
  
  /* Populate the new key and node */
  newfkey->inode = inode;
  memcpy (newfkey->filename, filename, filelen+1);
  
  newfnode->offset = 0;
  newfnode->modtime = modtime;
  newfnode->idledelay = 0;
  
  RBTreeInsert (filetree, newfkey, newfnode);
  
  return newfnode;
}  /* End of addfile() */


/***************************************************************************
 * processfile:
 *
 * Process a file by reading any data after the last offset.
 *
 * Return the new file offset on success and -1 on error reading file
 * and the negated file offset on successful read but fatal sending
 * error.
 ***************************************************************************/
static off_t
processfile (char *filename, FileNode *fnode, off_t newsize, time_t newmodtime)
{
  int fd;
  int nread;
  int reccnt = 0;
  int reachedmax = 0;
  int detlen;
  int flags;
  off_t newoffset = fnode->offset;
  char mseedbuf[RECSIZE];
  int mseedsize = RECSIZE;
  char *tptr;
  struct timespec treq, trem;
  
  lprintf (3, "Processing file %s", filename);
  
  /* Set the throttling sleep time */
  treq.tv_sec = (time_t) 0;
  treq.tv_nsec = (long) throttlensec;
  
  /* We are already in the local directory so do a cheap basename(1) */
  if ( ! (tptr = strrchr (filename, '/')) )
    tptr = filename;
  else
    tptr++;
  
  /* Set open flags */
#if defined(__sun__) || defined(__sun)
  flags = O_RDONLY | O_RSYNC;
#else
  flags = O_RDONLY;
#endif
  
  /* Open target file */
  if ( (fd = open (tptr, flags, 0)) == -1 )
    {
      if ( ! (stopsig && errno == EINTR) )
	{
	  lprintf (0, "Error opening %s: %s", filename, strerror(errno));
	  return -1;
	}
      else
	return newoffset;
    }
  
  /* Read RECSIZE byte chunks off the end of the file */
  while ( (newsize - newoffset) >= RECSIZE )
    {
      /* Jump out if we've read the maximum allowed number of records
	 from this file for this scan */
      if ( filemaxrecs && reccnt >= filemaxrecs )
	{
	  reachedmax = 1;
	  break;
	}
      
      /* Read the next record */
      if ( (nread = pread (fd, mseedbuf, RECSIZE, newoffset)) != RECSIZE )
	{
	  if ( ! (stopsig && errno == EINTR) )
	    {
	      lprintf (0, "Error: only read %d bytes from %s", nread, filename);
	      close (fd);
	      return -1;
	    }
	  else
	    return newoffset;
	}
      
      newoffset += nread;
      
      /* Increment records read counter */
      scanrecordsread++;
      
      /* Check record for 1000 blockette and verify SEED structure */
      if ( (detlen = find_reclen (mseedbuf, RECSIZE)) <= 0 )
	{
	  /* If no data has ever been read from this file, ignore file */
	  if ( fnode->offset == 0 )
	    {
	      lprintf (0, "%s: Not a valid Mini-SEED record at offset %lld, ignoring file",
		       filename, (long long) (newoffset - nread));
	      close (fd);
	      return -1;
	    }
	  /* Otherwise, if records have been read, skip until next scan */
	  else
	    {
	      lprintf (0, "%s: Not a valid Mini-SEED record at offset %lld (new bytes %lld), skipping for this scan",
		       filename, (long long) (newoffset - nread),
		       (long long ) (newsize - newoffset + nread));
	      close (fd);
	      return (newoffset - nread);
	    }
	}
      else if ( detlen != RECSIZE )
	{
	  lprintf (0, "%s: Unexpected record length: %d at offset %lld, ignoring file",
		   filename, detlen, (long long) (newoffset - nread));
	  close (fd);
	  return -1;
	}
      /* Prepare for sending the record off to the DataLink ringserver */
      else
	{
	  lprintf (4, "Sending %20.20s", mseedbuf);
	  
	  /* Send data record to the ringserver */
	  if ( sendrecord (mseedbuf, mseedsize) )
	    {
	      return -newoffset;
	    }
	  
	  /* Increment records written counter */
	  if ( iostats )
	    scanrecordswritten++;
	  
	  /* Sleep for specified throttle interval */
	  if ( throttlensec )
	    nanosleep (&treq, &trem);
	}
      
      reccnt++;
    }
  
  close (fd);
  
  /* Update modtime:
   * If the maximum number of records have been reached we are not necessarily 
   * at the end of the file so set the modtime one second in the past so we are
   * triggered to look at this file again. */
  if ( reachedmax )
    fnode->modtime = newmodtime - 1;
  else
    fnode->modtime = newmodtime;
  
  if ( reccnt )
    lprintf (2, "Read %d %d-byte record(s) from %s",
	     reccnt, RECSIZE, filename);
  
  return newoffset;
}  /* End of processfile() */


/***************************************************************************
 * printfilelist:
 *
 * Print file tree to the specified descriptor.
 ***************************************************************************/
static void
printfilelist (FILE *fp)
{
  FileKey  *fkey;
  FileNode *fnode;
  RBNode   *tnode;
  Stack    *stack;
  
  stack = StackCreate();
  RBBuildStack (filetree, stack);
  
  while ( (tnode = (RBNode *) StackPop (stack)) )
    {
      fkey = (FileKey *) tnode->key;
      fnode = (FileNode *) tnode->data;
      
      fprintf (fp, "%s\t%llu\t%lld\t%lld\n",
	       fkey->filename,
	       (unsigned long long int) fkey->inode,
	       (signed long long int) fnode->offset,
	       (signed long long int) fnode->modtime);
    }
  
  StackDestroy (stack, free);
}  /* End of printfilelist() */


/***************************************************************************
 * savestate:
 *
 * Save state information to a specified file.  First the new state
 * file is written to a temporary file (the same statefile name with a
 * ".tmp" extension) then rename the temporary file.  This avoids
 * partial writes of the state file if the program is killed while
 * writing the state file.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
savestate (char *statefile)
{
  char tmpstatefile[255];
  int fnsize;
  FILE *fp;
  
  lprintf (2, "Saving state file");
  
  /* Create temporary state file */
  fnsize = snprintf (tmpstatefile, sizeof(tmpstatefile), "%s.tmp", statefile);
  
  /* Check for truncation */
  if ( fnsize >= sizeof(tmpstatefile) )
    {
      lprintf (0, "Error, temporary statefile name too long (%d bytes)",
	       fnsize);
      return -1;
    }
  
  /* Open temporary state file */
  if ( (fp = fopen (tmpstatefile, "w")) == NULL )
    {
      lprintf (0, "Error opening temporary statefile %s: %s",
	       tmpstatefile, strerror(errno));
      return -1;
    }
  
  /* Write file list to temporary state file */
  printfilelist (fp);
  
  fclose (fp);
  
  /* Rename temporary state file overwriting the current state file */
  if ( rename (tmpstatefile, statefile) )
    {
      lprintf (0, "Error renaming temporary statefile %s->%s: %s",
	       tmpstatefile, statefile, strerror(errno));
      return -1;
    }
  
  return 0;
}  /* End of savestate() */


/***************************************************************************
 * recoverstate:
 *
 * Recover the state information from the state file.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
recoverstate (char *statefile)
{
  FileNode *fnode;
  char line[600];
  int fields, count;
  FILE *fp;
  
  char filename[MAX_FILENAME_LENGTH];
  unsigned long long int inode;
  signed long long int offset, modtime;
  
  if ( (fp=fopen(statefile, "r")) == NULL )
    {
      lprintf (0, "Error opening statefile %s: %s", statefile, strerror(errno));
      return -1;
    }
  
  lprintf (1, "Recovering state");
  
  count = 1;
  
  while ( (fgets (line, sizeof(line), fp)) !=  NULL)
    {
      fields = sscanf (line, "%s %llu %lld %lld\n",
		       filename, &inode, &offset, &modtime);
      
      if ( fields < 0 )
        continue;
      
      if ( fields < 4 )
        {
          lprintf (0, "Could not parse line %d of state file", count);
	  continue;
        }
      
      fnode = addfile ((ino_t) inode, filename, (time_t) modtime);
      
      if ( fnode )
	{
	  fnode->offset = (off_t) offset;
	  fnode->scantime = 0;
	}
      
      count++;
    }
  
  fclose (fp);
  
  return 0;
}  /* End of recoverstate() */


/***************************************************************************
 * sendrecord:
 *
 * Send the specified record to the DataLink server.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
sendrecord (char *record, int reclen)
{
  struct fsdh_s *fsdh;
  struct btime_s stime;
  hptime_t starttime;
  char streamid[100];
  
  /* Generate stream ID for this record: NET_STA_LOC_CHAN/MSEED */
  ms_recsrcname (record, streamid, 0);
  strcat (streamid, "/MSEED");
  
  fsdh = (struct fsdh_s *) record;
  memcpy (&stime, &fsdh->start_time, sizeof(struct btime_s));
  
  /* Swap start time values if improbable year value */
  if ( stime.year < 1960 || stime.year > 3000 )
    {
      MS_SWAPBTIME(&stime);
    }
  
  /* Determine high precision start time */
  starttime = ms_btime2hptime (&stime);
  
  /* Send record to server */
  if ( dl_write (dlconn, record, reclen, streamid, starttime, writeack) < 0 )
    {
      return -1;
    }
  
  return 0;
}  /* End of sendrecord() */


/***************************************************************************
 * processparam:
 *
 * Process the command line parameters.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
processparam (int argcount, char **argvec)
{
  char *address = 0;
  char *dirfilename = 0;
  char *matchstr = 0;
  char *rejectstr = 0;
  char *tptr;
  int keepalive = -1;
  int optind;
  
  /* Process all command line arguments */
  for (optind = 1; optind < argcount; optind++)
    {
      if (strcmp (argvec[optind], "-V") == 0)
        {
          fprintf (stderr, "%s version: %s\n", PACKAGE, VERSION);
          exit (0);
        }
      else if (strcmp (argvec[optind], "-h") == 0)
        {
          usage();
        }
      else if (strncmp (argvec[optind], "-v", 2) == 0)
        {
          verbose += strspn (&argvec[optind][1], "v");
        }
      else if (strcmp (argvec[optind], "-1") == 0)
        {
	  onescan = 1;
        }
      else if (strcmp (argvec[optind], "-I") == 0)
        {
	  iostats = 1;
        }
      else if (strcmp (argvec[optind], "-BL") == 0)
        {
          budlatency = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-n") == 0)
        {
          nextnew = 1;
        }
      else if (strcmp (argvec[optind], "-z") == 0)
        {
          nextnew = 2;
        }
      else if (strcmp (argvec[optind], "-s") == 0)
        {
          scansleep = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-sz") == 0)
        {
          scansleep0 = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-i") == 0)
        {
          idledelay = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-I") == 0)
        {
          idlesec = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-Q") == 0)
        {
          quietsec = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-T") == 0)
        {
          throttlensec = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-f") == 0)
        {
          filemaxrecs = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-r") == 0)
        {
          maxrecur = strtol (getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-M") == 0)
        {
          matchstr = getoptval(argcount, argvec, optind++);
        }
      else if (strcmp (argvec[optind], "-R") == 0)
        {
          rejectstr = getoptval(argcount, argvec, optind++);
        }
      else if (strcmp (argvec[optind], "-k") == 0)
        {
          keepalive = strtoul(getoptval(argcount, argvec, optind++), NULL, 10);
        }
      else if (strcmp (argvec[optind], "-A") == 0)
        {
          writeack = 1;
        }
      else if (strcmp (argvec[optind], "-S") == 0)
        {
          statefile = getoptval(argcount, argvec, optind++);
	  
	  /* Create an absolute path to statefile if not already specified as such */
	  if ( statefile && *statefile != '/' )
	    {
	      char absstatefile[1024];
	      
	      if ( ! getcwd(absstatefile, sizeof(absstatefile)) )
		{
		  lprintf (0, "Error determining the current working directory: %s", strerror(errno));
		  exit (1);
		}
	      
	      strncat (absstatefile, "/", sizeof(absstatefile) - strlen(absstatefile) - 1);
	      strncat (absstatefile, statefile, sizeof(absstatefile) - strlen(absstatefile) - 1);
	      statefile = strdup(absstatefile);
	    }
        }
      else if (strcmp (argvec[optind], "-d") == 0)
        {
          adddir (getoptval(argcount, argvec, optind++));
        }
      else if (strcmp (argvec[optind], "-D") == 0)
        {
	  if ( dirfilename )
	    {
	      lprintf (0, "Only one -D option is allowed");
	      exit (1);
	    }
	  
	  dirfilename = getoptval(argcount, argvec, optind++);
        }
      else if (strncmp (argvec[optind], "-", 1) == 0)
        {
          lprintf (0, "Unknown option: %s", argvec[optind]);
          exit (1);
        }
      else
	{
	  if ( ! address )
	    address = argvec[optind];
	  else
	    lprintf (0, "Unknown option: %s", argvec[optind]);
	}
    }
  
  /* Make sure a server was specified */
  if ( ! address )
    {
      fprintf(stderr, "No DataLink server specified\n\n");
      fprintf(stderr, "%s version: %s\n\n", PACKAGE, VERSION);
      fprintf(stderr, "Usage: %s [options] [host][:port]\n", PACKAGE);
      fprintf(stderr, "Try '-h' for detailed help\n");
      exit (1);
    }
  
  /* Allocate and initialize a new connection description */
  dlconn = dl_newdlcp (address, argvec[0]);
  
  /* Set keepalive parameter, allow for valid value of 0 */
  if ( keepalive >= 0 )
    dlconn->keepalive = keepalive;
  
  /* Initialize the verbosity for the dl_log function */
  dl_loginit (verbose, &lprintf0, "", &lprintf0, "");
  
  /* Report the program version */
  lprintf (0, "%s version: %s", PACKAGE, VERSION);
  
  /* Load dirlist from dirfile */
  if ( dirfilename )
    {
      /* Make sure no directories were already added to the list */
      if ( dirlist )
	{
	  lprintf (0, "Cannot specify both -d and -D options");
	  exit (1);
	}
      
      /* Allocate new DirFile */
      dirfile = (DirFile *) malloc (sizeof(DirFile));
      
      if ( dirfile == NULL )
	{
	  lprintf (0, "Error allocating memory for dir file list");
	  exit (1);
	}
      
      /* Copy directory list file name */
      strncpy (dirfile->filename, dirfilename, sizeof(dirfile->filename));
      
      /* Set dirfile modification time to 0 */
      dirfile->modtime = 0;
      
      processdirfile (dirfile->filename);
    }
  
  /* Make sure base dir(s) specified */
  if ( dirlist == 0 )
    {
      lprintf (0, "No base directories were specified");
      exit (1);
    }
  
  /* Compile the match regex if specified */
  if ( matchstr )
    {
      fnmatch = (regex_t *) malloc (sizeof (regex_t));
      
      if ( regcomp(fnmatch, matchstr, REG_EXTENDED|REG_NOSUB ) != 0 )
	{
	  lprintf (0, "Cannot compile regular expression '%s'", matchstr);
	  exit (1);
	}
    }
  
  /* Compile the reject regex if specified */
  if ( rejectstr )
    {
      fnreject = (regex_t *) malloc (sizeof (regex_t));
      
      if ( regcomp(fnreject, rejectstr, REG_EXTENDED|REG_NOSUB ) != 0 )
	{
	  lprintf (0, "Cannot compile regular expression '%s'", rejectstr);
	  exit (1);
	}
    }
  
  /* Attempt to recover sequence numbers from state file */
  if ( statefile )
    {
      /* Check if interval was specified for state saving */
      if ( (tptr = strchr (statefile, ':')) != NULL )
        {
          char *tail;
          
          *tptr++ = '\0';
	  
          stateint = (int) strtol (tptr, &tail, 10);
	  
          if ( *tail || (stateint < 0 || stateint > 1e9) )
            {
              lprintf (0, "State saving interval specified incorrectly");
              exit (1);
            }
        }
      
      if ( recoverstate (statefile) < 0 )
        {
          lprintf (0, "state recovery failed");
        }
      /* If nextnew (-z) was specified but state file read, turn it off */
      else if ( nextnew == 2 )
	{
	  nextnew = 0;
	}
    }
  
  return 0;
}  /* End of processparam() */


/***************************************************************************
 * getoptval:
 * Return the value to a command line option; checking that the value is 
 * itself not an option (starting with '-') and is not past the end of
 * the argument list.
 *
 * argcount: total arguments in argvec
 * argvec: argument list
 * argopt: index of option to process, value is expected to be at argopt+1
 *
 * Returns value on success and exits with error message on failure
 ***************************************************************************/
static char *
getoptval (int argcount, char **argvec, int argopt)
{
  if ( argvec == NULL || argvec[argopt] == NULL )
    {
      lprintf (0, "getoptval(): NULL option requested");
      exit (1);
    }
  
  if ( (argopt+1) < argcount && *argvec[argopt+1] != '-' )
    return argvec[argopt+1];
  
  lprintf (0, "Option %s requires a value", argvec[argopt]);
  exit (1);
}  /* End of getoptval() */


/***************************************************************************
 * adddir:
 *
 * Add directory to end of the global directory list (dirlist).
 ***************************************************************************/
static void
adddir (char *dirname)
{
  struct dirlink *lastlp, *newlp;
  struct stat st;
  int dirlen;
  
  if ( ! dirname )
    {
      lprintf (0, "adddir(): No directory name specified");
      return;
    }
  
  dirlen = strlen (dirname);
  
  /* Remove trailing slash if included */
  if ( dirname[dirlen-1] == '/' )
    dirname[dirlen-1] = '\0';
  
  /* Verify that a directory exists, if not log a warning */
  if ( stat (dirname, &st) )
    {
      lprintf (0, "WARNING: Could not find '%s'", dirname);
    }
  else if ( ! S_ISDIR(st.st_mode) )
    {
      lprintf (0, "WARNING '%s' is not a directory, skipping", dirname);
      return;
    }
  
  /* Create the new DirLink */
  newlp = (DirLink *) malloc (sizeof(DirLink)+dirlen+1);
  
  if ( !newlp )
    return;
  
  memcpy (newlp->dirname, dirname, dirlen+1);
  newlp->accesserr = 0;
  newlp->next = 0;
  
  /* Find the last DirLink in the directory list */
  lastlp = dirlist;
  while ( lastlp != 0 )
    {
      if ( lastlp->next == 0 )
        break;
      
      lastlp = lastlp->next;
    }
  
  /* Insert the new DirLink at the end of the directory list */
  if ( lastlp == 0 )
    dirlist = newlp;
  else
    lastlp->next = newlp;
  
}  /* End of adddir() */


/***************************************************************************
 * processdirfile:
 *
 * Clear out global directory list (dirlist) and add all directories
 * listed in the specified file.
 ***************************************************************************/
static void
processdirfile (char *filename) 
{
  DirLink *dlp;
  struct stat st;
  FILE *fp;
  char dirlistent[MAX_FILENAME_LENGTH];

  lprintf (3, "Checking directory list file '%s'", filename);

  if ( stat (filename, &st) )
    {
      lprintf (0, "Cannot stat dir list file %s: %s", filename, strerror(errno));
      return;
    }

  if ( !S_ISREG(st.st_mode) )
    {
      lprintf (0, "%s is not a regular file", filename);
      return;
    }
  
  /* Check that the file has been modified since the last check */
  if ( st.st_mtime <= dirfile->modtime )
    {
      return;  /* No modification = no (re)loading of dir list */
    }
  
  /* Store new modification time */
  dirfile->modtime = st.st_mtime;
  
  /* Clear out directory list (dirlist) */
  if ( dirlist )
    {
      dlp = dirlist->next;      
      free (dirlist);
      dirlist = dlp;
    }
  dirlist = 0;
  
  if ( ! (fp = fopen(filename, "r")) )
    {
      lprintf (0, "Cannot open dir list file %s: %s", filename, strerror(errno));
      return;
    }
  
  lprintf (2, "Reading list of directories from %s", filename);
  
  while ( fgets (dirlistent, sizeof(dirlistent), fp) )
    {
      char *cp;
      
      /* End string at first newline character */
      if ( (cp = strchr(dirlistent, '\n')) )
	*cp = '\0';
      
      /* Skip empty lines */
      if ( ! strlen (dirlistent) )
	continue;
      
      /* Skip comment lines */
      if ( *dirlistent == '#' )
	continue;
      
      lprintf (2, "Adding directory '%s' from list file", dirlistent);
      
      adddir (dirlistent);
    }
  
  fclose (fp);
  
}  /* End of processdirfile() */


/***************************************************************************
 * keycompare:
 *
 * Compare two FileKeys passed as void pointers.
 *
 * Return 1 if a > b, -1 if a < b and 0 otherwise (e.g. equality).
 ***************************************************************************/
static int
keycompare (const void *a, const void *b)
{
  int cmpval;
  
  /* Compare Inode values */
  if ( ((FileKey*)a)->inode > ((FileKey*)b)->inode )
    return 1;
  
  else if ( ((FileKey*)a)->inode < ((FileKey*)b)->inode )
    return -1;
  
  /* Compare filename values */
  cmpval = strcmp ( ((FileKey*)a)->filename, ((FileKey*)b)->filename );
  
  if ( cmpval > 0 )
    return 1;
  else if ( cmpval < 0 )
    return -1;
  
  return 0;
}  /* End of keycompare() */


/***************************************************************************
 * term_handler and print_handler:
 * Signal handler routines.
 ***************************************************************************/
static void
term_handler (int sig)
{
  stopsig = 1;
}

static void
print_handler (int sig)
{
  fprintf (stderr, "Filename\tInode\tOffset\tModtime\n");
  printfilelist (stderr);
}


/***************************************************************************
 * lprintf0:
 *
 * Interface to lprintf for level 0, trimming any newline characters.
 ***************************************************************************/
static void
lprintf0 (char *message)
{
  char *newline;
  
  /* Truncate message at last newline */
  if ( (newline = strrchr (message, '\n')) )
    {
      *newline = '\0';
    }
  
  lprintf (0, message);
}


/***************************************************************************
 * lprintf:
 *
 * A generic log message handler, pre-pends a current date/time string
 * to each message.  This routine add a newline to the final output
 * message so it should not be included with the message.
 *
 * Returns the number of characters in the formatted message.
 ***************************************************************************/
static int
lprintf (int level, const char *fmt, ...)
{
  int rv = 0;
  char message[1024];
  va_list argptr;
  struct tm *tp;
  time_t curtime;
  
  char *day[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  char *month[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
		   "Aug", "Sep", "Oct", "Nov", "Dec"};

  if ( level <= verbose ) {
    
    /* Build local time string and generate final output */
    curtime = time(NULL);
    tp = localtime (&curtime);
    
    va_start (argptr, fmt);
    rv = vsnprintf (message, sizeof(message), fmt, argptr);
    va_end (argptr);
    
    printf ("%3.3s %3.3s %2.2d %2.2d:%2.2d:%2.2d %4.4d - %s: %s\n",
	    day[tp->tm_wday], month[tp->tm_mon], tp->tm_mday,
	    tp->tm_hour, tp->tm_min, tp->tm_sec, tp->tm_year + 1900,
	    PACKAGE, message);
    
    fflush (stdout);
  }
  
  return rv;
}  /* End of lprintf() */


/***************************************************************************
 * usage:
 *
 * Print usage message and exit.
 ***************************************************************************/
static void
usage()
{
  fprintf(stderr,"%s version %s\n\n", PACKAGE, VERSION);
  fprintf(stderr,"Send files of Mini-SEED to the IRIS DMC\n\n");
  fprintf(stderr,"Usage: %s [options] [host][:port] files\n\n", PACKAGE);
  fprintf(stderr," ## Options ##\n"
	  " -V             Report program version\n"
	  " -h             Show this usage message\n"
	  " -v             Be more verbose, multiple flags can be used\n"

          " -L scans       Print IO stats after this many full scans\n"

	  " -r level       Maximum directory levels to recurse (%d levels)\n"
	  " -M match       Only process filenames that match this regular expression\n"
	  " -R reject      Only process filenames that do not match this regular expression\n"

          " -I             Print IO stats during transmission\n"
	  
	  " -q             Be quiet, do not print diagnostics or transmission summary NOTDONE\n"
	  " -NA            Do not require the server to acknowledge each packet received NOTDONE\n"

	  " -A             Require the server to acknowledge each packet received\n"
	  " -S file[:int]  State file to save/restore file time stamps, optionally\n"
	  "                  an interval, in seconds, can be specified to save the\n"
	  "                  state file (default interval: %d seconds)\n\n",
	  idledelay, idlesec, throttlensec, filemaxrecs, maxrecur, stateint);
  exit (1);
}  /* End of usage() */
