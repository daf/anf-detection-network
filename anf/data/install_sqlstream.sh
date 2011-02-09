#!/bin/bash -eu
# -e : Exit immediatly if a command exits with non-zero status
# -u : Treat unset variables as an error while substituting

# Install SQLstream script
# Dave Foster <dfoster@asascience.com>
# Initial 7 Dec 2010
#
# Usage:
# install_sqlstream.sh <SDP Port> <HSQLDB Port> <Path to seismic jar> <optional install dir>
#

SS_INSTALLER_BIN="$HOME/ooici.supplemental.packages/SQLstream-2.5.0RC1-opto-x86_64.bin"
RABBITMQ_JAVA_CLIENT_ZIP="$HOME/ooici.supplemental.packages/rabbitmq-java-client-bin-1.5.3.zip"

# ports are specified on the command line
if [ $# -lt 2 ]; then
    echo "Usage: $0 <SDP port ex 5570> <HSQLDB port ex 9001> [full install path, optional]"
    exit 1
fi

SDPPORT=$1; shift
HSQLDBPORT=$1; shift
SS_SEISMIC_JAR=$1; shift
if [ $# -eq 1 ]; then
    DIRNAME=$1
else
    DIRNAME=${TMPDIR-/tmp}/sqlstream.$SDPPORT.$HSQLDBPORT
fi

# 1. Make sure dirname specified is legal
if [ -d $DIRNAME ]; then
    echo "Specified directory $DIRNAME already exists!"
    exit 1
fi

# 2. Install SQLstream daemon into new temp dir
$SS_INSTALLER_BIN --mode unattended --prefix $DIRNAME

# 3. Fix daemon file
DAEMONPATH=$DIRNAME/bin/SQLstreamd
chmod +w $DAEMONPATH
patch --no-backup-if-mismatch -d $DIRNAME/bin >/dev/null <<'EOF'
--- /tmp/sqlstream.EZUzM6/bin/SQLstreamd	2010-11-17 17:49:05.000000000 -0500
+++ sqlstream/SQLstreamd	2010-12-03 09:21:52.365831757 -0500
@@ -202,13 +202,6 @@
     exit 1
 fi
 
-# restore from checkpoint, if any
-if ! $ASPEN_BIN/checkpoint --restore
-then
-    echo "----- Restore failed - Exiting -----"
-    exit 1
-fi
-
 # Start external HSQLDB process, if needed
 if [ "$START_HSQLDB" -eq 1 ]; then
     start_external_hsqldb
@@ -260,11 +253,4 @@
     exit $SERVER_EXIT_CODE
 fi
 
-# save a copy of the repository only on clean exit
-echo "----- Checkpoint repository -----";
-if ! $ASPEN_BIN/checkpoint --create; then
-    echo "----- WARNING - Checkpoint failed -----";
-    exit 1
-fi
-
 exit 0
EOF
chmod -w $DAEMONPATH

# 4. Change SDP port
echo -e "aspen.sdp.port=$SDPPORT\naspen.controlnode.url=sdp://localhost:$SDPPORT" >$DIRNAME/aspen.custom.properties

# 5. Change HSQLDB port in props file
HSQLDBPROPSTEMP=`mktemp -t`
PROPSFILE=$DIRNAME/catalog/ReposStorage.hsqldbserver.properties
HSQLDBPROPSPERMS=`stat --format=%a $PROPSFILE`
sed "s/:9001/:$HSQLDBPORT/" <$PROPSFILE >$HSQLDBPROPSTEMP
chmod +w $PROPSFILE
mv $HSQLDBPROPSTEMP $PROPSFILE
chmod $HSQLDBPROPSPERMS $PROPSFILE

# 6. Change HSQLDB port in binary file
HSQLDBBINTEMP=`mktemp -t`
HSQLDBBIN=$DIRNAME/bin/hsqldb
HSQLDBPERMS=`stat --format=%a $HSQLDBBIN`
sed "s/DB_PORT=9001/DB_PORT=$HSQLDBPORT/" <$HSQLDBBIN >$HSQLDBBINTEMP
chmod +w $HSQLDBBIN
mv $HSQLDBBINTEMP $HSQLDBBIN
chmod $HSQLDBPERMS $HSQLDBBIN

# 7. Copy UCSD seismic application jar to plugin dir
cp $SS_SEISMIC_JAR $DIRNAME/plugin

# 8. Unzip rabbitmq client zipfile to a known location
# possible security risk : using known file
RABBITBASE=`basename $RABBITMQ_JAVA_CLIENT_ZIP`
RABBITDIR=`dirname $DIRNAME`/${RABBITBASE%.zip}
if [ ! -d "${RABBITDIR}" ]; then
    unzip $RABBITMQ_JAVA_CLIENT_ZIP -d /tmp >/dev/null
fi

# 9. Symlink things in plugin/autocp dir
AUTOCPDIR=$DIRNAME/plugin/autocp
ln -s $RABBITDIR/rabbitmq-client.jar $AUTOCPDIR/rabbitmq-client.jar
ln -s $RABBITDIR/commons-cli-1.1.jar $AUTOCPDIR/commons-cli-1.1.jar
ln -s $RABBITDIR/commons-io-1.2.jar $AUTOCPDIR/commons-io-1.2.jar
ln -s $DIRNAME/plugin/AmqpAdapter.jar $AUTOCPDIR/AmqpAdapter.jar

# DONE!
exit 0

