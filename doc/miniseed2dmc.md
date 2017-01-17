# <p >miniseed2dmc 
###  Transfer Mini-SEED data to a DMC</p>

1. [Name](#)
1. [Synopsis](#synopsis)
1. [Description](#description)
1. [Options](#options)
1. [Selection File](#selection-file)
1. [Examples](#examples)
1. [Notes](#notes)
1. [Author](#author)

## <a id='synopsis'>Synopsis</a>

<pre >
miniseed2dmc [options] host:port files|directories ...
</pre>

## <a id='description'>Description</a>

<p ><b>miniseed2dmc</b> transfers selected Mini-SEED data records from the local computer to a remote Data Management Center.  Prior to using this program coordination with DMC is required and the user must have been assigned a host and port indicating where the data should be sent.</p>

<p >Mini-SEED to be transfered can be identified 3 different ways:</p>
<pre >
1 - files on the command line
2 - directories on the command line which will be searched
3 - files containing lists of files and/or directories
</pre>

<p >If directories are specified on the command line or in list files all files they contain are assumed to be input files and all sub-directories will be searched (the level of recursion can be limited with the <b>-r</b> option).</p>

<p >List files are identified by prefixing the file name with '@' on the command line or by using the <b>-l</b> option.</p>

<p >A state file is maintained by <b>miniseed2dmc</b> to track the progress of data transfer.  This tracking means that the client can be shut down and then resume the transfer when the client is restarted.  More importantly it allows the client to determine when all records from a given data set have been transferred preventing them from being transferred again erroneously.  By default the state file is written to a file named, creatively, 'statefile' in the working directory (see the <b>-w</b> option).  The default state file location may be overridden using the <b>-S</b> option.</p>

<p >To track the Mini-SEED data transferred <b>miniseed2dmc</b> writes SYNC files representing the data coverage.  The SYNC files are written to the working directory (see the <b>-w</b> option).  The SYNC file names contain the connection start and end times as a record of when the data was sent.  A separate SYNC file listing is written for each time the program is executed.</p>

## <a id='options'>Options</a>

<b>-V</b>

<p style="padding-left: 30px;">Print the program version and exit.</p>

<b>-h</b>

<p style="padding-left: 30px;">Print the program help/usage and exit.</p>

<b>-v</b>

<p style="padding-left: 30px;">Be more verbose.  This flag can be used multiple times ("-v -v" or "-vv") for more verbosity.</p>

<b>-p</b>

<p style="padding-left: 30px;">Pretend, process input files as usual and write the state file and SYNC file but do not connect or send data to the submission server. Useful for client side testing, the transfer statistics will not be accurate.  After running the program in this mode the state file (and probably the SYNC file) created must be removed prior to actually transferring the data.</p>

<b>-r </b><i>level</i>

<p style="padding-left: 30px;">Specify the maximum number of directories to recurse into, default is no limits.</p>

<b>-fn</b>

<p style="padding-left: 30px;">Embed a relative path and filename in the data stream packets sent to the server, this information can be used by the remote server to re-create the local file names and organization.</p>

<b>-E</b>

<p style="padding-left: 30px;">Exit the program on connection errors.  By default miniseed2dmc will continuously try to connect to the specified server until all data has been sent.</p>

<b>-q</b>

<p style="padding-left: 30px;">Be quiet, do not print the default diagnostic messages or transmission summary.</p>

<b>-NS</b>

<p style="padding-left: 30px;">Do not write a SYNC file after sending data, by default a SYNC file representing the data set sent is written for each session.  All SYNC files are written to the "work directory" (the <b>-w</b> option).</p>

<b>-ACK</b>

<p style="padding-left: 30px;">Request and require acknowledgements from the server for each Mini-SEED record sent, this guarantees that each record sent was written to the filesystem by the remote server.  This should not be necessary since TCP performs this function for the network layer, leaving only a very small potential that a server crash will lose data sent by the client.  It will also significantly slow down the transfer rate.</p>

<b>-mr </b><i>maxrate</i>

<p style="padding-left: 30px;">Specify a maximum transmission rate in bits/second.  The suffixes <b>K</b>, <b>M</b> and <b>G</b> are recognized for the <b>maxrate</b> value, e.g. '100M' is understood to be 100 megabits/second.  The transmission rate is not limited by default.</p>

<b>-I</b>

<p style="padding-left: 30px;">Print the transfer rate at a specified interval (the <b>-It</b> option) during transmission.</p>

<b>-It </b><i>interval</i>

<p style="padding-left: 30px;">Specify <i>interval</i> in seconds at which to print transfer rate statistics during transmission, default is 30 seconds.  This option implies the <b>-I</b> option.</p>

<b>-w </b><i>workdir</i>

<p style="padding-left: 30px;">The working directory is where the (default) state file and SYNC listing will be written, default is the current working directory.</p>

<b>-S </b><i>statefile</i>

<p style="padding-left: 30px;">A state file is written to track the status of the transmission.  It is recommended to use a unique state file for each separate data set. By default a file named "statefile" will be written to the <b>workdir</b>.  If the specified value is not an absolute path it is relative to the current working directory (not <b>workdir</b>).</p>

<b>-l </b><i>listfile</i>

<p style="padding-left: 30px;">The <i>listfile</i> is a file containing a list of files and/or directories containing Mini-SEED to be sent.  This is an alternative to prefixing an input file with the '@' which identifies it as a list file.</p>

<b>-s </b><i>selectfile</i>

<p style="padding-left: 30px;">Limit processing to Mini-SEED records that match a selection in the specified file.  The selection file contains parameters to match the network, station, location, channel, quality and time range for input records.  As a special case, specifying "-" will result in selection lines being read from stdin.  For more details see the <b>SELECTION FILE</b> section below.</p>

<b></b><i>host:port</i>

<p style="padding-left: 30px;">The required host and port arguments specify the server where the Mini-SEED records should be sent.</p>

## <a id='selection-file'>Selection File</a>

<p >A selection file is used to match input data records based on network, station, location and channel information.  Optionally a quality and time range may also be specified for more refined selection.  The non-time fields may use the '*' wildcard to match multiple characters and the '?' wildcard to match single characters.  Character sets may also be used, for example '[ENZ]' will match either E, N or Z.  After a '#' character is read the remaining portion of the line will be ignored.</p>

<p >Example selection file entires (the first four fields are required)</p>
<pre >
#net sta  loc  chan  qual  start             end
IU   ANMO *    BH?
II   *    *    *     Q
IU   COLA 00   LH[ENZ] R
IU   COLA 00   LHZ   *     2008,100,10,00,00 2008,100,10,30,00
</pre>

## <a id='examples'>Examples</a>

<p >For the below examples the host and port are specified as <b>host:port</b>, in real usage these must be a real host name and port.</p>

<b>Example 1</b>

<p style="padding-left: 30px;">The most simple example is sending a couple of files containing Mini-SEED:</p>

<pre style="padding-left: 30px;">
> miniseed2dmc host:port 080101.mseed 080102.mseed
</pre>

<p >In this case the state file and SYNC files will be written to the current directory.</p>

<b>Example 2</b>

<p style="padding-left: 30px;">If there is a directory called 'data' that contains only files of Mini-SEED they can all be sent using:</p>

<pre style="padding-left: 30px;">
> miniseed2dmc host:port data
</pre>

<p >The program automatically distinguishes between files and directories.</p>

<b>Example 3</b>

<p style="padding-left: 30px;">A list file named 'mseeds.txt' contains:</p>
<pre style="padding-left: 30px;">
---- mseeds.txt
/data/080101.mseed
/data/080102.mseed
----
</pre>

<p >This file can be used with miniseed2dmc using the follow sytax (both are equivalent):</p>

<pre >
> miniseed2dmc host:port @mseeds.txt
> miniseed2dmc host:port -l mseeds.txt
</pre>

<b>Example 4</b>

<p style="padding-left: 30px;">A recommended organization of data is to have a directory for each data set and keep all the Mini-SEED files in a sub-directory.  For example, a monthly data set can be kept in /archive/Jan2008/ with all the January 2008 Mini-SEED files kept in /archive/Jan2008/data/. <b>miniseed2dmc</b> could then we executed in the following manner:</p>

<pre style="padding-left: 30px;">
> miniseed2dmc host:port -w /archive/Jan2008/ /archive/Jan2008/data/
</pre>

<p >This will result in all the Mini-SEED data in /archive/Jan2008/data/ being transferred and the related state and SYNC file(s) being written to /archive/Jan2008/.</p>

## <a id='notes'>Notes</a>

<p >This program is intended to transfer static data sets, it is not designed for transfer of real-time streaming data.</p>

## <a id='author'>Author</a>

<pre >
Chad Trabant
IRIS Data Management Center
</pre>


(man page 2017/01/17)
