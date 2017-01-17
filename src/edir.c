/***************************************************************************
 * edir.h
 *
 * Enhanced directory handling routines.  The primary function of
 * these routines is to return directory entries in a sorted order.
 *
 * modified: 2008.087
 ***************************************************************************/

#include "edir.h"
#include <stdlib.h>
#include <string.h>

static int sortedirentries (EDIR *edirp);

/***************************************************************************
 * eopendir:
 *
 * Open a directory, read all entries, sort them and return an
 * enhanced directory stream pointer for use with edirread() and
 * edirclose().
 *
 * Return a pointer to an enhanced directory stream on success and
 * NULL on error.
 ***************************************************************************/
EDIR *
eopendir (const char *dirname)
{
  DIR *dirp;
  EDIR *edirp;
  struct dirent *de, *decopy;
  struct edirent *ede;
  struct edirent *prevede = 0;

  if (!dirname)
    return NULL;

  dirp = opendir (dirname);

  if (!dirp)
    return NULL;

  /* Allocate new EDIR */
  if (!(edirp = (EDIR *)malloc (sizeof (EDIR))))
  {
    closedir (dirp);
    return NULL;
  }

  /* Read all directory entries */
  while ((de = readdir (dirp)))
  {
    /* Allocate space for directory entry copy */
    if (!(decopy = (struct dirent *)malloc (de->d_reclen)))
    {
      closedir (dirp);
      eclosedir (edirp);
      return NULL;
    }

    /* Copy directory entry */
    memcpy (decopy, de, de->d_reclen);

    /* Allocate space for enhanced directory entry */
    if (!(ede = (struct edirent *)calloc (1, sizeof (struct edirent))))
    {
      closedir (dirp);
      eclosedir (edirp);
      return NULL;
    }

    ede->de = decopy;
    ede->prev = prevede;

    /* Add new enhanced directory entry to the list */
    if (prevede == 0)
    {
      edirp->ents = ede;
    }
    else
    {
      prevede->next = ede;
    }

    prevede = ede;
  }

  closedir (dirp);

  /* Sort directory entries */
  if (sortedirentries (edirp) < 0)
  {
    eclosedir (edirp);
    return NULL;
  }

  /* Set the current entry to the top of the list */
  edirp->current = edirp->ents;

  return edirp;
} /* End of ediropen() */

/***************************************************************************
 * ereaddir:
 *
 * Return the next directory entry from the list associated with the
 * EDIR and NULL if no more entries.
 ***************************************************************************/
struct dirent *
ereaddir (EDIR *edirp)
{
  struct edirent *ede;

  if (!edirp)
    return NULL;

  ede = edirp->current;

  if (edirp->current)
    edirp->current = ede->next;

  return (ede) ? ede->de : NULL;
} /* End of ereaddir() */

/***************************************************************************
 * eclosedir:
 *
 * Close an enhanced directory stream freeing all associated memory.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
int
eclosedir (EDIR *edirp)
{
  struct edirent *ede, *nede;

  if (!edirp)
    return -1;

  ede = edirp->ents;

  /* Loop through associated entries and free them */
  while (ede)
  {
    nede = ede->next;

    if (ede->de)
      free (ede->de);

    free (ede);

    ede = nede;
  }

  free (edirp);

  return 0;
} /* End of eclosedir() */

/***************************************************************************
 * sortedirentries:
 *
 * Sort enhanced directory entries using the mergesort alorthim.
 * directory entries are compared using the strcmp() function.  The
 * mergesort implementation was inspired by the listsort function
 * published and copyright 2001 by Simon Tatham.
 *
 * Return the number of merges completed on success and -1 on error.
 ***************************************************************************/
static int
sortedirentries (EDIR *edirp)
{
  struct edirent *p, *q, *e, *top, *tail;
  int nmerges, totalmerges;
  int insize, psize, qsize, i;

  if (!edirp)
    return -1;

  top = edirp->ents;
  totalmerges = 0;
  insize = 1;

  for (;;)
  {
    p = top;
    top = NULL;
    tail = NULL;

    nmerges = 0; /* count number of merges we do in this pass */

    while (p)
    {
      nmerges++; /* there exists a merge to be done */
      totalmerges++;

      /* step `insize' places along from p */
      q = p;
      psize = 0;
      for (i = 0; i < insize; i++)
      {
        psize++;
        q = q->next;
        if (!q)
          break;
      }

      /* if q hasn't fallen off end, we have two lists to merge */
      qsize = insize;

      /* now we have two lists; merge them */
      while (psize > 0 || (qsize > 0 && q))
      {
        /* decide whether next element of merge comes from p or q */
        if (psize == 0)
        { /* p is empty; e must come from q. */
          e = q;
          q = q->next;
          qsize--;
        }
        else if (qsize == 0 || !q)
        { /* q is empty; e must come from p. */
          e = p;
          p = p->next;
          psize--;
        }
        else if (strcmp (p->de->d_name, q->de->d_name) <= 0)
        { /* First element of p is lower (or same), e must come from p. */
          e = p;
          p = p->next;
          psize--;
        }
        else
        { /* First element of q is lower; e must come from q. */
          e = q;
          q = q->next;
          qsize--;
        }

        /* add the next element to the merged list */
        if (tail)
          tail->next = e;
        else
          top = e;

        e->prev = tail;
        tail = e;
      }

      /* now p has stepped `insize' places along, and q has too */
      p = q;
    }

    tail->next = NULL;

    /* If we have done only one merge, we're finished. */
    if (nmerges <= 1) /* allow for nmerges==0, the empty list case */
    {
      edirp->ents = top;

      return totalmerges;
    }

    /* Otherwise repeat, merging lists twice the size */
    insize *= 2;
  }
} /* End of sortedirentries() */
