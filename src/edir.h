/***************************************************************************
 * edir.h
 *
 * Enhanced directory handling defines.
 *
 * modified: 2008.086
 ***************************************************************************/

#ifndef EDIR_H
#define EDIR_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <dirent.h>

typedef struct EDIR_s {
  struct edirent *ents;
  struct edirent *current;
} EDIR;

struct edirent {
  struct edirent *prev;
  struct edirent *next;
  struct dirent *de;
};

extern EDIR  *eopendir (const char *dirname);
extern struct dirent *ereaddir (EDIR *edirp);
extern int    eclosedir (EDIR *edirp);

#ifdef __cplusplus
}
#endif

#endif /* EDIR_H */
