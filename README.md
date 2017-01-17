# miniseed2dmc - send Mini-SEED to a Data Management Center

This program transfers selected Mini-SEED data records from the local
computer to a remote Data Management Center.  Prior to using this program
coordination with the DMC is required.

To track the Mini-SEED data transferred the program writes SYNC files
representing the data coverage.  Additionally, a state file is maintained
to track the progress of data transfer.  This tracking means that the client
can be shut down and then resume the transfer when the client is restarted.
More importantly it allows the client to determine when all records from
a given data set have been transferred preventing them from being transferred
again erroneously.

For usage information see the [miniseed2dmc manual](doc/miniseed2dmc.md) in the
'doc' directory.

## Building

In most Unix/Linux environments a simple 'make' will build the program.

The CC and CFLAGS environment variables can be used to configure
the build parameters.

## Licensing 

GNU LGPL version 3.  See included LICENSE file for details.

Copyright (c) 2017 Chad Trabant
