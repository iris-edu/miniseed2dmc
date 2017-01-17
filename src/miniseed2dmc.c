/***************************************************************************
 * miniseed2dmc.c
 *
 * A program to send specified files of Mini-SEED records to the a
 * data management center.  A record of the data sent is maintained
 * internally and by using the state file option allows for incomplete
 * data transfers to be resumed between program restarts.
 *
 * A summary of the data sent is printed when the program quits.
 *
 * The directory separator is assumed to be '/'.
 *
 * Chad Trabant, IRIS Data Management Center
 ***************************************************************************/

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <regex.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <libdali.h>
#include <libmseed.h>

#include "edir.h"

#define PACKAGE "miniseed2dmc"
#define VERSION "2017.017"

/* Maximum filename length including path */
#define MAX_FILENAME_LENGTH 512

/* Linkable structure to hold input file list */
typedef struct FileLink_s
{
  struct FileLink_s *next;
  off_t offset;         /* Last file read offset, must be signed */
  off_t size;           /* Total size of file */
  uint64_t bytecount;   /* Count of bytes sent */
  uint64_t recordcount; /* Count of records sent */
  char name[1];         /* File name, complete path to access */
} FileLink;

static FileLink *filelist = 0;     /* Linked list of input files */
static FileLink *lastfile = 0;     /* Last entry of input files list */
static Selections *selections = 0; /* List of data selections */

static char stopsig = 0;    /* Stop/termination signal */
static int verbose = 0;     /* Verbosity level */
static int writeack = 0;    /* Flag to control the request for write acks */
static int64_t maxrate = 0; /* Max rate in bits/sec, 0 to disable  */

static char maxrecur = -1;  /* Maximum level of directory recursion */
static int filenames = 0;   /* Include file names in streamIDs */
static char pretend = 0;    /* Flag to control pretending mode */
static int iostats = 0;     /* Output IO stats */
static int iostatsint = 30; /* Output IO stats interval */
static int quiet = 0;       /* Quiet mode */
static int quitonerror = 0; /* Quit program on connection errors */
static int reconnect = 60;  /* Reconnect delay if not quitting on errors */
static int syncfile = 1;    /* SYNC file for writing data coverage */
static char *workdir = "."; /* Directory to write SYNC and state files */
static char *statefile = 0; /* State file for saving/restoring time stamps */

static uint64_t inputbytes = 0;   /* Total size for all input files */
static uint64_t totalbytes = 0;   /* Track count of total bytes sent */
static uint64_t totalrecords = 0; /* Track count of total records sent */
static uint64_t totalfiles = 0;   /* Track count of total files sent */
static MSTraceList *traces = 0;   /* Track all trace segments sent */

static void printfilelist (FILE *fd);
static int writesync (MSTraceList *mstl, time_t start, time_t end);
static int savestate (char *statefile);
static int recoverstate (char *statefile);
static int processparam (int argcount, char **argvec);
static char *getoptval (int argcount, char **argvec, int argopt);
static int64_t calcbitsize (char *sizestr);
static int makeratestr (char *ratestr, int ratestrlen, uint64_t bps);
static int addfile (FileLink **list, char *filename, struct stat *stp);
static int adddir (char *targetdir, char *basedir, int level);
static int addlistfile (char *filename);
static int freelist (FileLink **list);
static void term_handler ();
static void print_handler ();
static void lprintf0 (char *message);
static int lprintf (int level, const char *fmt, ...);
static void usage ();

static DLCP *dlconn;

int
main (int argc, char **argv)
{
  FileLink *file;
  Selections *matchsp = 0;
  SelectTime *matchstp = 0;
  struct timeval procstart;
  struct timeval procend;
  struct timeval filestart;
  struct timeval lastpkt;
  struct timeval iostatsprint;
  struct timeval now;
  struct timespec rcsleep;
  double interval;
  int restart = 0;
  int allsent = 0;
  int exitval = 0;
  int streamlen;
  char ratestr[50];

  MSRecord *msr = 0;
  char srcname[50];
  char qsrcname[50];
  char streamid[100];
  off_t filepos = 0;
  hptime_t endtime;
  int retcode = MS_ENDOFFILE;

  /* Signal handling using POSIX routines */
  struct sigaction sa;

  sa.sa_flags = SA_RESTART;
  sigemptyset (&sa.sa_mask);

  sa.sa_handler = print_handler;
  sigaction (SIGUSR1, &sa, NULL);

  sa.sa_handler = term_handler;
  sigaction (SIGINT, &sa, NULL);
  sigaction (SIGTERM, &sa, NULL);

  sa.sa_handler = SIG_IGN;
  sigaction (SIGHUP, &sa, NULL);
  sigaction (SIGPIPE, &sa, NULL);

  /* Process command line parameters */
  if (processparam (argc, argv) < 0)
    return 1;

  /* Shortcut: check if all input data has already been sent */
  file = filelist;
  allsent = 1;
  while (file)
  {
    if (file->bytecount != file->size)
      allsent = 0;

    file = file->next;
  }
  if (allsent)
  {
    lprintf (0, "All data transmitted (based on saved state).");

    /* Free the global file list */
    freelist (&filelist);

    return 0;
  }

  /* Configure reconnection sleep time */
  rcsleep.tv_sec = reconnect;
  rcsleep.tv_nsec = 0;

  /* Initialize trace segment tracking */
  traces = mstl_init (traces);

  /* Initilize transmission rate timer */
  if (maxrate)
    gettimeofday (&lastpkt, NULL);

  /* Set processing start time */
  gettimeofday (&procstart, NULL);

  iostatsprint.tv_sec = iostatsprint.tv_usec = 0;

  /* Start scan sequence */
  while (!stopsig)
  {
    file = filelist;

    /* Connect to server */
    if (!pretend && dl_connect (dlconn) < 0)
    {
      lprintf (0, "Error connecting to server");
    }
    else
    {
      restart = 0;

      if (!quiet && !pretend)
        lprintf (0, "Connected to %s", dlconn->addr);

      if (!dlconn->writeperm && !pretend)
      {
        lprintf (0, "ERROR Write permission not granted for %s", dlconn->addr);

        stopsig = 1;
        exitval = 1;
        break;
      }

      while (file && !restart && !stopsig)
      {
        /* Skip file if already sent */
        if (file->offset == file->size)
        {
          if (!file->next) /* If that was the last file set the stop signal */
            stopsig = 1;
          else /* Otherwise increment to the next file in the list */
            file = file->next;

          continue;
        }

        if (iostats)
        {
          gettimeofday (&filestart, NULL);
          iostatsprint.tv_sec = filestart.tv_sec + iostatsint;
          iostatsprint.tv_usec = 0;
        }

        lprintf (3, "Sending Mini-SEED from file %s", file->name);

        /* Reset byte and record counters */
        file->bytecount = 0;
        file->recordcount = 0;

        /* Set initial file position if this file has been read from */
        if (file->offset > 0)
          filepos = file->offset * -1;

        /* Read all data records from file and send to the server */
        while (!stopsig &&
               (retcode = ms_readmsr (&msr, file->name, -1, &filepos, NULL, 1, 0, verbose - 2)) == MS_NOERROR)
        {
          msr_srcname (msr, srcname, 0);
          endtime = msr_endtime (msr);

          /* Check if record is matched by selection */
          if (selections)
          {
            msr_srcname (msr, qsrcname, 1);

            if (!(matchsp = ms_matchselect (selections, qsrcname, msr->starttime, endtime, &matchstp)))
            {
              if (verbose >= 3)
              {
                char stime[30];
                ms_hptime2seedtimestr (msr->starttime, stime, 1);
                ms_log (1, "Skipping (selection) %s, %s\n", qsrcname, stime);
              }
              continue;
            }
          }

          /* Generate stream ID for this record: [filename::]NET_STA_LOC_CHAN/MSEED */
          if (filenames)
            streamlen = snprintf (streamid, sizeof (streamid), "%s::%s/MSEED", file->name, srcname);
          else
            streamlen = snprintf (streamid, sizeof (streamid), "%s/MSEED", srcname);

          /* Check for stream ID truncation */
          if (streamlen >= sizeof (streamid))
          {
            lprintf (0, "ERROR Resulting stream ID is too long: '%s::%s/MSEED'", file->name, srcname);

            stopsig = 1;
            exitval = 1;
            break;
          }

          /* Enforce maximum transmission rate */
          if (maxrate)
          {
            uint64_t totalbits = (totalbytes + msr->reclen) * 8;

            gettimeofday (&now, NULL);

            /* Calculate interval since program start */
            interval = (((double)now.tv_sec + (double)now.tv_usec / 1000000) -
                        ((double)procstart.tv_sec + (double)procstart.tv_usec / 1000000));

            /* Sleep if rate would be larger than maximum */
            if (interval == 0.0 || ((double)totalbits / interval) > maxrate)
            {
              /* Minimum interval needed for all data at maxrate */
              double rateinterval = ((double)totalbits / maxrate);

              /* Subtract interval since last packet */
              rateinterval -= interval;

              /* Sleep until within maximum rate */
              if (rateinterval > 0)
              {
                struct timespec naptime;
                naptime.tv_sec = 0;
                naptime.tv_nsec = (long)(rateinterval * 1.0e9);
                nanosleep (&naptime, NULL);
              }
            }
          }

          lprintf (4, "Sending %s", streamid);

          /* Send record to server */
          if (!pretend &&
              dl_write (dlconn, msr->record, msr->reclen, streamid, msr->starttime, endtime, writeack) < 0)
          {
            lprintf (0, "Error sending record to %s", dlconn->addr);
            restart = 1;
            break;
          }
          else
          {
            /* Track read position in input file */
            file->offset = filepos + msr->reclen;

            /* Update counts */
            file->bytecount += msr->reclen;
            file->recordcount++;

            totalbytes += msr->reclen;
            totalrecords++;

            /* Add record to trace coverage */
            if (traces && !mstl_addmsr (traces, msr, 0, 1, -1.0, -1.0))
            {
              lprintf (0, "Error adding %s coverage to trace tracking", streamid);
            }
          }

          if (maxrate)
          {
            gettimeofday (&lastpkt, NULL);
          }

          if (iostats)
          {
            /* Only print stats if interval has passed */
            gettimeofday (&now, NULL);

            if (iostatsprint.tv_sec < now.tv_sec)
            {
              interval = (((double)now.tv_sec + (double)now.tv_usec / 1000000) -
                          ((double)filestart.tv_sec + (double)filestart.tv_usec / 1000000));

              makeratestr (ratestr, sizeof (ratestr), 8 * ((interval) ? (file->bytecount / interval) : 0));

              lprintf (0, "%s: sent %d%% (%s, %.1f records/s)",
                       file->name, (int)(100.0 * file->bytecount / file->size),
                       ratestr, (interval) ? (file->recordcount / interval) : 0);

              /* Increment iostats print interval time stamp */
              iostatsprint.tv_sec += iostatsint;
            }
          }
        } /* End of reading records from file */

        /* Make sure everything is cleaned up */
        ms_readmsr (&msr, NULL, 0, NULL, NULL, 0, 0, 0);

        /* Print error if not EOF and not set shutdown or restart signal */
        if (retcode == MS_NOTSEED && file->bytecount == 0)
        {
          lprintf (0, "%s: no SEED data found, skipping", file->name);
          file->bytecount = file->size;
        }
        else if (retcode != MS_ENDOFFILE && !stopsig && !restart)
        {
          lprintf (0, "Error reading %s: %s", file->name, ms_errorstr (retcode));
          exitval = 1;
          stopsig = 1;
        }
        else
        {
          if (!quiet)
            lprintf (0, "%s: sent %llu bytes in %llu records",
                     file->name, file->bytecount, file->recordcount);

          /* Print IO stats */
          if (iostats)
          {
            /* Determine run time since filestart was set */
            gettimeofday (&now, NULL);

            interval = (((double)now.tv_sec + (double)now.tv_usec / 1000000) -
                        ((double)filestart.tv_sec + (double)filestart.tv_usec / 1000000));

            makeratestr (ratestr, sizeof (ratestr), 8 * ((interval) ? (file->bytecount / interval) : 0));

            lprintf (0, "%s: sent in %.1f seconds (%s, %.1f records/s)",
                     file->name, interval, ratestr,
                     (interval) ? (file->recordcount / interval) : 0);
          }
        }

        if (restart)
          break;

        totalfiles++;

        /* Save the state file if not the last file */
        if (file->next && statefile)
          savestate (statefile);

        if (!file->next) /* If that was the last file set the stop signal */
          stopsig = 1;
        else /* Otherwise increment to the next file in the list */
          file = file->next;
      } /* End of traversing file list */
    }

    /* Quit on connection errors if requested */
    if (!stopsig && quitonerror)
      break;

    /* Sleep before reconnecting */
    if (!stopsig)
    {
      lprintf (0, "Reconnecting in %d seconds", reconnect);
      nanosleep (&rcsleep, NULL);
    }
  } /* End of main scan sequence */

  /* Set processing end time */
  gettimeofday (&procend, NULL);

  if (!quiet)
  {
    interval = (((double)procend.tv_sec + (double)procend.tv_usec / 1000000) -
                ((double)procstart.tv_sec + (double)procstart.tv_usec / 1000000));

    makeratestr (ratestr, sizeof (ratestr), 8 * ((interval) ? (totalbytes / interval) : 0));

    lprintf (0, "Time elapsed: %.1f seconds (%s, %.1f records/s)",
             interval, ratestr,
             totalrecords / interval);

    lprintf (0, "Sent %llu bytes in %llu records from %llu file(s)",
             (unsigned long long)totalbytes,
             (unsigned long long)totalrecords,
             (unsigned long long)totalfiles);
  }

  /* Shut down the connection to the server */
  if (!pretend && dlconn->link != -1)
    dl_disconnect (dlconn);

  /* Save the state file */
  if (statefile)
    savestate (statefile);

  /* Write SYNC file listing for coverage sent */
  if (syncfile && traces->numtraces > 0)
    writesync (traces, (time_t)procstart.tv_sec, (time_t)procend.tv_sec);

  /* Check that all input data was sent */
  file = filelist;
  allsent = 1;
  while (file)
  {
    if (file->bytecount != file->size)
      allsent = 0;

    file = file->next;
  }

  if (allsent)
    lprintf (0, "All data transmitted.");

  /* Print trace coverage sent */
  if (verbose >= 3)
    mstl_printtracelist (traces, 0, 1, 0);

  /* Free the global file list */
  freelist (&filelist);

  return exitval;
} /* End of main() */

/***************************************************************************
 * printfilelist:
 *
 * Print file tree to the specified descriptor.
 ***************************************************************************/
static void
printfilelist (FILE *fp)
{
  FileLink *file = filelist;

  while (file)
  {
    fprintf (fp, "%s\t%lld\t%lld\t%llu\t%llu\n",
             file->name,
             (signed long long int)file->offset,
             (signed long long int)file->size,
             (unsigned long long int)file->bytecount,
             (unsigned long long int)file->recordcount);

    file = file->next;
  }

  return;
} /* End of printfilelist() */

/***************************************************************************
 * writesync:
 *
 * Write trace coverage for the given MSTraceList to a SYNC file.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
writesync (MSTraceList *mstl, time_t start, time_t end)
{
  MSTraceID *id;
  MSTraceSeg *seg;
  FILE *sf = 0;
  time_t now;
  struct tm *nt;
  struct tm *st;
  struct tm *et;
  char yearday[10];
  char starttime[50];
  char endtime[50];
  char filename[MAX_FILENAME_LENGTH];

  if (!mstl)
    return -1;

  /* Generate current time stamp */
  now = time (NULL);
  nt = localtime (&now);
  nt->tm_year += 1900;
  nt->tm_yday += 1;
  snprintf (yearday, sizeof (yearday), "%04d,%03d", nt->tm_year, nt->tm_yday);

  /* Generate sync file name */
  st = localtime (&start);
  st->tm_year += 1900;
  st->tm_mon += 1;
  et = localtime (&end);
  et->tm_year += 1900;
  et->tm_mon += 1;
  snprintf (filename, sizeof (filename),
            "%s/%04d-%02d-%02dT%02d:%02d:%02d--%04d-%02d-%02dT%02d:%02d:%02d.sync", workdir,
            st->tm_year, st->tm_mon, st->tm_mday, st->tm_hour, st->tm_min, st->tm_sec,
            et->tm_year, et->tm_mon, et->tm_mday, et->tm_hour, et->tm_min, et->tm_sec);

  /* Open sync file */
  if (!(sf = fopen (filename, "w")))
  {
    lprintf (0, "Error opening SYNC file %s: %s", filename, strerror (errno));
    return -1;
  }

  /* Print header line */
  fprintf (sf, "DCC|%s\n", yearday);

  /* Trace MSTrace list and print SYNC lines */
  id = mstl->traces;
  while (id)
  {
    seg = id->first;
    while (seg)
    {
      ms_hptime2seedtimestr (seg->starttime, starttime, 1);
      ms_hptime2seedtimestr (seg->endtime, endtime, 1);

      fprintf (sf, "%s|%s|%s|%s|%s|%s||%.2g|%" PRId64 "|||||||%s\n",
               id->network, id->station, id->location, id->channel,
               starttime, endtime, seg->samprate, seg->samplecnt,
               yearday);

      seg = seg->next;
    }

    id = id->next;
  }

  if (sf)
    fclose (sf);

  lprintf (1, "Wrote SYNC file %s", filename);

  return 0;
} /* End of writesync() */

/***************************************************************************
 * savestate:
 *
 * Save state information to a specified file.  First the new state
 * file is written to a temporary file (the same statefile name with a
 * ".tmp" extension) then the temporary file is renamed to overwrite
 * the state file.  This avoids partial writes of the state file if
 * the program is killed while writing the state file.
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
  fnsize = snprintf (tmpstatefile, sizeof (tmpstatefile), "%s.tmp", statefile);

  /* Check for truncation */
  if (fnsize >= sizeof (tmpstatefile))
  {
    lprintf (0, "Error, temporary statefile name too long (%d bytes)",
             fnsize);
    return -1;
  }

  /* Open temporary state file */
  if ((fp = fopen (tmpstatefile, "w")) == NULL)
  {
    lprintf (0, "Error opening temporary statefile %s: %s",
             tmpstatefile, strerror (errno));
    return -1;
  }

  /* Write file list to temporary state file */
  printfilelist (fp);

  fclose (fp);

  /* Rename temporary state file overwriting the current state file */
  if (rename (tmpstatefile, statefile))
  {
    lprintf (0, "Error renaming temporary statefile %s->%s: %s",
             tmpstatefile, statefile, strerror (errno));
    return -1;
  }

  return 0;
} /* End of savestate() */

/***************************************************************************
 * recoverstate:
 *
 * Recover the state information from the state file.
 *
 * Returns 1 when state recovered, 0 when state file does not exist
 * and -1 on error.
 ***************************************************************************/
static int
recoverstate (char *statefile)
{
  FileLink *file;
  char line[MAX_FILENAME_LENGTH + 30];
  int fields, count;
  FILE *fp;
  flag found;

  char filename[MAX_FILENAME_LENGTH];
  signed long long int offset, size;
  unsigned long long int bytecount, recordcount;

  if ((fp = fopen (statefile, "r")) == NULL)
  {
    /* Only log errors other than file not found */
    if (errno != ENOENT)
    {
      lprintf (0, "Error opening statefile %s: %s", statefile, strerror (errno));
      return -1;
    }
    else
      return 0;
  }

  lprintf (1, "Recovering state");

  count = 1;

  while ((fgets (line, sizeof (line), fp)) != NULL)
  {
    fields = sscanf (line, "%s %lld %lld %llu %llu\n",
                     filename, &offset, &size, &bytecount, &recordcount);

    if (fields < 0)
      continue;

    if (fields < 5)
    {
      lprintf (0, "Could not parse line %d of state file", count);
      continue;
    }

    /* Find matching entry in input file list */
    found = 0;
    file = filelist;
    while (file)
    {
      /* Shortcut if this entry has already been updated */
      if (file->offset > 0)
      {
        file = file->next;
        continue;
      }

      /* Compare file names and update offset if match found */
      if (!strcmp (filename, file->name))
      {
        file->offset = offset;
        file->bytecount = bytecount;
        file->recordcount = recordcount;

        if (file->size != size)
          lprintf (2, "%s: size has changed since last execution (%lld => %lld)",
                   filename, (signed long long int)size, (signed long long int)file->size);

        found = 1;
        break;
      }

      file = file->next;
    }

    if (!found)
    {
      lprintf (0, "%s: found in state file but not an input file", filename);
      lprintf (0, "Wrong state file?");
      fclose (fp);
      return -1;
    }

    count++;
  }

  fclose (fp);

  return 1;
} /* End of recoverstate() */

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
  FileLink *listfiles = 0;
  char *selectfile = 0;
  char *address = 0;
  char *tptr;
  int recovery;
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
      usage ();
    }
    else if (strncmp (argvec[optind], "-v", 2) == 0)
    {
      verbose += strspn (&argvec[optind][1], "v");
    }
    else if (strcmp (argvec[optind], "-p") == 0)
    {
      pretend = 1;
    }
    else if (strcmp (argvec[optind], "-r") == 0)
    {
      maxrecur = strtol (getoptval (argcount, argvec, optind++), NULL, 10);
    }
    else if (strcmp (argvec[optind], "-fn") == 0)
    {
      filenames = 1;
    }
    else if (strcmp (argvec[optind], "-E") == 0)
    {
      quitonerror = 1;
    }
    else if (strcmp (argvec[optind], "-q") == 0)
    {
      quiet = 1;
    }
    else if (strcmp (argvec[optind], "-NS") == 0)
    {
      syncfile = 0;
    }
    else if (strcmp (argvec[optind], "-ACK") == 0)
    {
      writeack = 1;
    }
    else if (strcmp (argvec[optind], "-mr") == 0)
    {
      maxrate = calcbitsize (getoptval (argcount, argvec, optind++));

      if (!maxrate)
      {
        lprintf (0, "Error parsing maximum rate string");
        exit (1);
      }
    }
    else if (strcmp (argvec[optind], "-I") == 0)
    {
      iostats = 1;
    }
    else if (strcmp (argvec[optind], "-It") == 0)
    {
      iostatsint = strtol (getoptval (argcount, argvec, optind++), NULL, 10);
      iostats = 1;
    }
    else if (strcmp (argvec[optind], "-w") == 0)
    {
      workdir = getoptval (argcount, argvec, optind++);
    }
    else if (strcmp (argvec[optind], "-S") == 0)
    {
      statefile = getoptval (argcount, argvec, optind++);
    }
    else if (strcmp (argvec[optind], "-l") == 0)
    {
      addfile (&listfiles, getoptval (argcount, argvec, optind++), NULL);
    }
    else if (strcmp (argvec[optind], "-s") == 0)
    {
      selectfile = getoptval (argcount, argvec, optind++);
    }
    else if (strncmp (argvec[optind], "-", 1) == 0)
    {
      lprintf (0, "Unknown option: %s", argvec[optind]);
      exit (1);
    }
    else
    {
      tptr = argvec[optind];

      /* Assume this is the server if not already specified */
      if (!address)
      {
        address = tptr;
      }
      /* Otherwise check for an input file list */
      else if (tptr[0] == '@')
      {
        if (addfile (&listfiles, tptr + 1, NULL) < 0)
        {
          lprintf (0, "Error adding list file %s", tptr + 1);
          exit (1);
        }
      }
      /* Otherwise this is an input file */
      else
      {
        if (addfile (NULL, tptr, NULL) < 0)
        {
          lprintf (0, "Error adding input file %s", tptr + 1);
          exit (1);
        }
      }
    }
  }

  /* Require a server to be specified */
  if (!address)
  {
    fprintf (stderr, "%s version: %s\n\n", PACKAGE, VERSION);
    fprintf (stderr, "No data submission server specified\n\n");
    fprintf (stderr, "Usage: %s [options] [host][:port] file(s)\n", PACKAGE);
    fprintf (stderr, "Try '-h' for detailed help\n");
    exit (1);
  }

  /* Check for write access on working directory */
  if (access (workdir, R_OK | X_OK | W_OK))
  {
    fprintf (stderr, "%s version: %s\n\n", PACKAGE, VERSION);
    fprintf (stderr, "Error with working directory '%s': %s\n\n",
             workdir, strerror (errno));
    exit (1);
  }

  /* Allocate and initialize a new connection description */
  dlconn = dl_newdlcp (address, argvec[0]);

  /* Initialize the verbosity for the ms_log and dl_log functions */
  ms_loginit (&lprintf0, "", &lprintf0, "");
  dl_loginit (verbose - 2, &lprintf0, "", &lprintf0, "");

  /* Report the program version */
  if (!quiet)
    lprintf (0, "%s version: %s", PACKAGE, VERSION);

  if (pretend)
    lprintf (0, "Pretend mode");

  /* Process any list files and free the list of lists */
  if (listfiles)
  {
    FileLink *listfile = listfiles;
    while (listfile)
    {
      lprintf (1, "Reading list file: %s", listfile->name);

      if (addlistfile (listfile->name) < 0)
      {
        lprintf (0, "Error processing list file %s", listfile);
        exit (1);
      }

      listfile = listfile->next;
    }

    freelist (&listfile);
  }

  /* Read data selection file */
  if (selectfile)
  {
    lprintf (1, "Reading selections file: %s", selectfile);

    if (ms_readselectionsfile (&selections, selectfile) < 0)
    {
      lprintf (0, "Cannot read data selection file\n");
      exit (1);
    }
  }

  /* Make sure input files/dirs specified */
  if (filelist == 0)
  {
    lprintf (0, "No input files or directories were specified");
    exit (1);
  }

  /* Setup default state file as "workdir/statefile" */
  if (!statefile)
  {
    char sfile[256];

    snprintf (sfile, sizeof (sfile), "%s/statefile", workdir);

    statefile = strdup (sfile);
  }

  /* Attempt to recover state */
  recovery = recoverstate (statefile);

  if (recovery == 1)
  {
    lprintf (0, "Connection state recovered");
  }
  else if (recovery == -1)
  {
    lprintf (0, "Error recovering state file");
    exit (1);
  }

  return 0;
} /* End of processparam() */

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
  if (argvec == NULL || argvec[argopt] == NULL)
  {
    lprintf (0, "getoptval(): NULL option requested");
    exit (1);
  }

  if ((argopt + 1) < argcount && *argvec[argopt + 1] != '-')
    return argvec[argopt + 1];

  lprintf (0, "Option %s requires a value", argvec[argopt]);
  exit (1);
} /* End of getoptval() */

/***************************************************************************
 * calcbitsize:
 *
 * Calculate a size in bits for the specified size string.  If the
 * string is terminated with the following suffixes the specified
 * scaling will be applied:
 *
 * 'K' or 'k' : kilobits - value * 1000
 * 'M' or 'm' : megabits - value * 1000*1000
 * 'G' or 'g' : gigabits - value * 1000*1000*1000
 *
 * Returns a size in bits on success and 0 on error.
 ***************************************************************************/
static int64_t
calcbitsize (char *sizestr)
{
  long long int size = 0;
  char *parsestr;
  int termchar;

  if (!sizestr)
    return 0;

  if (!(parsestr = strdup (sizestr)))
    return 0;

  termchar = strlen (parsestr) - 1;

  if (termchar <= 0)
    return 0;

  /* For kilobits */
  if (parsestr[termchar] == 'K' || parsestr[termchar] == 'k')
  {
    parsestr[termchar] = '\0';
    size = strtoll (parsestr, NULL, 10);
    if (!size)
    {
      ms_log (1, "calcbitsize(): Error converting %s to integer", parsestr);
      return 0;
    }
    size *= 1000;
  }
  /* For megabits */
  else if (parsestr[termchar] == 'M' || parsestr[termchar] == 'm')
  {
    parsestr[termchar] = '\0';
    size = strtoll (parsestr, NULL, 10);
    if (!size)
    {
      ms_log (1, "calcbitsize(): Error converting %s to integer", parsestr);
      return 0;
    }
    size *= 1000 * 1000;
  }
  /* For gigabits */
  else if (parsestr[termchar] == 'G' || parsestr[termchar] == 'g')
  {
    parsestr[termchar] = '\0';
    size = strtoll (parsestr, NULL, 10);
    if (!size)
    {
      ms_log (1, "calcbitsize(): Error converting %s to integer", parsestr);
      return 0;
    }
    size *= 1000 * 1000 * 1000;
  }
  else
  {
    size = strtoll (parsestr, NULL, 10);
    if (!size)
    {
      ms_log (1, "calcbitsize(): Error converting %s to integer", parsestr);
      return 0;
    }
  }

  if (parsestr)
    free (parsestr);

  return (int64_t)size;
} /* End of calcbitsize() */

/***************************************************************************
 * makeratestr:
 *
 * Create a rate string in bits per second a specified value.  The
 * following suffixes will be used to create readable strings:
 *
 * 'bit/s'  : for values 0 - 999
 * 'kbit/s' : for values 1000 - 999999
 * 'Mbit/s' : for values 1000000 - 999999999
 * 'Gbit/s' : for values 1000000000 - 999999999999
 *
 * Returns a size in bits on success and 0 on error.
 ***************************************************************************/
static int
makeratestr (char *ratestr, int ratestrlen, uint64_t bps)
{
  if (bps >= 1000000000)
  {
    snprintf (ratestr, ratestrlen, "%.1f Gbit/s", (double)bps / 1000000000.0);
    ratestr[ratestrlen - 1] = '\0';
  }
  else if (bps >= 1000000)
  {
    snprintf (ratestr, ratestrlen, "%.1f Mbit/s", (double)bps / 1000000.0);
    ratestr[ratestrlen - 1] = '\0';
  }
  else if (bps >= 1000)
  {
    snprintf (ratestr, ratestrlen, "%.1f kbit/s", (double)bps / 1000.0);
    ratestr[ratestrlen - 1] = '\0';
  }
  else
  {
    snprintf (ratestr, ratestrlen, "%lld bit/s", (long long int)bps);
    ratestr[ratestrlen - 1] = '\0';
  }

  return 0;
} /* End of makeratestr() */

/***************************************************************************
 * addfile:
 *
 * Add file to end of a list, as a special case when list == NULL the
 * file will be added to the global file list (filelist).
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
addfile (FileLink **list, char *filename, struct stat *stp)
{
  FileLink *newfile;
  FileLink *last;
  struct stat st;
  int filelen;

  if (!filename)
  {
    lprintf (0, "addfile(): No file or directory name specified");
    return -1;
  }

  filelen = strlen (filename);

  /* Check file name length */
  if (filelen > MAX_FILENAME_LENGTH)
  {
    lprintf (0, "File name longer than maximum allowd (%d): '%s'",
             MAX_FILENAME_LENGTH, filename);
    return -1;
  }

  /* Remove trailing slash if included */
  if (filename[filelen - 1] == '/')
    filename[filelen - 1] = '\0';

  /* Stat file unless a struct stat pointer is provieded */
  if (stp == NULL)
  {
    if (stat (filename, &st))
    {
      lprintf (0, "Error: could not find '%s': %s", filename, strerror (errno));
      return -1;
    }

    stp = &st;
  }

  /* If the file is actually a directory add files it contains recursively */
  if (S_ISDIR (stp->st_mode))
  {
    if (adddir (filename, filename, maxrecur))
    {
      return -1;
    }
  }
  /* If the file is a regular file add it to the input list */
  else if (S_ISREG (stp->st_mode))
  {
    /* Create the new FileLink */
    if (!(newfile = (FileLink *)malloc (sizeof (FileLink) + filelen + 1)))
    {
      lprintf (0, "Error allocating memory");
      return -1;
    }

    newfile->next = 0;
    newfile->offset = 0;
    newfile->size = stp->st_size;
    newfile->bytecount = 0;
    newfile->recordcount = 0;
    memcpy (newfile->name, filename, filelen + 1);

    inputbytes += stp->st_size;

    /* Insert the new FileLink at the end of the global input list */
    if (!list || list == &filelist)
    {
      if (lastfile == 0)
        filelist = newfile;
      else
        lastfile->next = newfile;

      lastfile = newfile;
    }
    /* Otherwise insert the new FileLink at the end of the specified list */
    else
    {
      last = *list;
      if (!last)
      {
        /* Add first entry for first link */
        *list = newfile;
      }
      else
      {
        /* Find last entry and insert the new link */
        while (last->next)
        {
          last = last->next;
        }
        last->next = newfile;
      }
    }
  }
  else
  {
    lprintf (0, "Error: '%s' is not a regular file or directory", filename);
    return -1;
  }

  return 0;
} /* End of addfile() */

/***************************************************************************
 * adddir:
 *
 * Scan a directory and recursively drop into sub-directories up to the
 * maximum recursion level adding all files found to the input file list.
 *
 * When initially called the targetdir and basedir should be the same,
 * either an absolute or relative path.  During directory recursion
 * these values are maintained such that they are either absolute
 * paths or targetdir is a relatively path from the current working
 * directory (which changes during recursion) and basedir is a
 * relative path from the initial directory.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
adddir (char *targetdir, char *basedir, int level)
{
  static int dlevel = 0;
  struct stat st;
  struct dirent *de;
  int rrootdir;
  EDIR *dir;

  lprintf (3, "Processing directory '%s'", basedir);

  if ((rrootdir = open (".", O_RDONLY, 0)) == -1)
  {
    if (!(stopsig && errno == EINTR))
      lprintf (0, "Error opening current working directory");
    return -1;
  }

  if (chdir (targetdir))
  {
    if (!(stopsig && errno == EINTR))
      lprintf (0, "Cannot change to directory %s: %s", targetdir, strerror (errno));
    close (rrootdir);
    return -1;
  }

  if ((dir = eopendir (".")) == NULL)
  {
    lprintf (0, "Cannot open directory %s: %s", targetdir, strerror (errno));

    if (fchdir (rrootdir))
    {
      lprintf (0, "Cannot change to relative root directory: %s", strerror (errno));
    }

    close (rrootdir);
    return -1;
  }

  while ((de = ereaddir (dir)) != NULL)
  {
    char filename[MAX_FILENAME_LENGTH];
    int filenamelen;

    /* Skip "." and ".." entries */
    if (!strcmp (de->d_name, ".") || !strcmp (de->d_name, ".."))
      continue;

    filenamelen = snprintf (filename, sizeof (filename),
                            "%s/%s", basedir, de->d_name);

    /* Make sure the filename was not truncated */
    if (filenamelen >= sizeof (filename))
    {
      lprintf (0, "File name beyond maximum of %d characters:", sizeof (filename));
      lprintf (0, "  %s", filename);
      return -1;
    }

    /* Stat the file */
    if (stat (de->d_name, &st) < 0)
    {
      /* Interruption signals when the stop signal is set should break out */
      if (stopsig && errno == EINTR)
        break;

      lprintf (0, "Cannot stat %s: %s", filename, strerror (errno));
      return -1;
    }

    /* If directory recurse up to the limit */
    if (S_ISDIR (st.st_mode))
    {
      if (level < 0 || dlevel < level)
      {
        lprintf (4, "Recursing into %s", filename);

        dlevel++;
        if (adddir (de->d_name, filename, level) == -2)
          return -1;
        dlevel--;
      }
      continue;
    }

    /* Sanity check for a regular file */
    if (!S_ISREG (st.st_mode))
    {
      lprintf (0, "Error %s is not a regular file, skipping", filename);
      continue;
    }

    /* Add file to input list */
    if (addfile (NULL, filename, &st) < 0)
    {
      lprintf (0, "Error adding input file %s", filename);
      return -1;
    }
  }

  eclosedir (dir);

  if (fchdir (rrootdir))
  {
    if (!(stopsig && errno == EINTR))
      lprintf (0, "Error: cannot change to relative root directory: %s",
               strerror (errno));
  }

  close (rrootdir);

  return 0;
} /* End of adddir() */

/***************************************************************************
 * addlistfile:
 *
 * Add files listed in the specified file to the global input file list.
 *
 * Returns count of files added on success and -1 on error.
 ***************************************************************************/
static int
addlistfile (char *filename)
{
  FILE *fp;
  char filelistent[MAX_FILENAME_LENGTH];
  int filecount = 0;
  int rv;

  lprintf (1, "Reading list file '%s'", filename);

  if (!(fp = fopen (filename, "r")))
  {
    lprintf (0, "Error: Cannot open list file %s: %s", filename, strerror (errno));
    return -1;
  }

  while (fgets (filelistent, sizeof (filelistent), fp))
  {
    char *cp;

    /* End string at first newline character */
    if ((cp = strchr (filelistent, '\n')))
      *cp = '\0';

    /* Skip empty lines */
    if (!strlen (filelistent))
      continue;

    /* Skip comment lines */
    if (*filelistent == '#')
      continue;

    lprintf (2, "Adding '%s' from list file", filelistent);

    rv = addfile (NULL, filelistent, NULL);

    if (rv < 0)
    {
      filecount = rv;
      break;
    }

    filecount += rv;
  }

  fclose (fp);

  return filecount;
} /* End of addlistfile() */

/***************************************************************************
 * freelist:
 *
 * Free a FileLink list.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
freelist (FileLink **list)
{
  FileLink *file;
  FileLink *next;

  if (!list)
    return -1;

  file = *list;
  while (file)
  {
    next = file->next;
    free (file);
    file = next;
  }

  *list = 0;

  return 0;
} /* End of freelist() */

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
  if ((newline = strrchr (message, '\n')))
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

  if (level <= verbose)
  {
    /* Build local time string and generate final output */
    curtime = time (NULL);
    tp = localtime (&curtime);

    va_start (argptr, fmt);
    rv = vsnprintf (message, sizeof (message), fmt, argptr);
    va_end (argptr);

    printf ("%3.3s %3.3s %2.2d %2.2d:%2.2d:%2.2d %4.4d - %s: %s\n",
            day[tp->tm_wday], month[tp->tm_mon], tp->tm_mday,
            tp->tm_hour, tp->tm_min, tp->tm_sec, tp->tm_year + 1900,
            PACKAGE, message);

    fflush (stdout);
  }

  return rv;
} /* End of lprintf() */

/***************************************************************************
 * usage:
 *
 * Print usage message and exit.
 ***************************************************************************/
static void
usage ()
{
  fprintf (stderr, "%s version %s\n\n", PACKAGE, VERSION);
  fprintf (stderr, "Send Mini-SEED to the IRIS DMC\n\n");
  fprintf (stderr, "Usage: %s [options] <host:port> files|directories\n\n", PACKAGE);
  fprintf (stderr, " ## Options ##\n"
                   " -V             Report program version\n"
                   " -h             Show this usage message\n"
                   " -v             Be more verbose, multiple flags can be used\n"
                   " -p             Pretend, process input files as usual but do not transfer to DMC\n"
                   " -r level       Maximum directory levels to recurse, default is no limit\n"
                   " -fn            Embed relative path and filename in data stream IDs\n"
                   " -E             Quit on connection errors, by default the client will reconnect\n"
                   " -q             Be quiet, do not print diagnostics or transmission summary\n"
                   " -NS            Do not write a SYNC file after sending data\n"
                   " -ACK           Require acknowledgements from the server for each record (slow)\n"
                   " -mr rate       Maximum transmission rate in bits/second, no limit by default\n"
                   " -I             Print transfer rate during transmission\n"
                   " -It interval   Interval in seconds to print transfer statistics (default: %d)\n"
                   " -w workdir     Location to write SYNC and state files, default is current dir\n"
                   " -S statefile   File to track transfer status, default is workdir/statefile\n"
                   " -l listfile    File containing a list of input files and/or directories\n"
                   " -s file        Specify a file containing data selection criteria\n"
                   "\n",
           iostatsint);
  exit (1);
} /* End of usage() */
