#!/bin/sh

# NOTE:
# This is a script I use to synchronize code from the Foresight repository to this directory.
# It is useless to anyone not working at Foresight. Just ignore it.

SRC="c:/Users/daniel/Desktop/foresight-ng"

# inboke as: copy frombase tobase pattern
function copy() {
  cp -f $SRC/$1/$3 $2/$3
}

# macros project

copy fs_LibMacros macros src/main/scala/com/fsist/util/FastAsync.scala

# streams project, utils

copy fs_Lib streams src/main/scala/com/fsist/util/BugException.scala

# Excluded: FutureOps.scala in foresight-ng depends on akka scheduler
for x in AsyncQueue AsyncSemaphore CancelToken; do
    copy fs_Lib streams src/main/scala/com/fsist/util/concurrent/$x.scala
done

# FutureTester.scala in foresight-ng depends on akka testkit timeout exception type
#copy fs_Lib streams src/test/scala/com/fsist/FutureTester.scala

for x in AsyncQueueTest CancelTokenTest; do
    copy fs_Lib streams src/test/scala/com/fsist/util/concurrent/$x.scala
done

# streams project, main

for x in NamedLogger package Pipe Sink SinkImpl Source SourceImpl package; do
    copy fs_Lib streams src/main/scala/com/fsist/stream/$x.scala
done

for x in SourceTest PipeTest SinkTest; do
    copy fs_Lib streams src/test/scala/com/fsist/stream/$x.scala
done

# bytestreams project, util

copy fs_Lib bytestreams src/main/scala/com/fsist/util/Nio.scala

copy fs_Lib bytestreams src/test/scala/com/fsist/util/NioTest.scala

# bytestreams project, main

for x in ByteSource BytePipe ByteSink; do
    copy fs_Lib bytestreams src/main/scala/com/fsist/stream/$x.scala
done

for x in BytePipeTest; do
    copy fs_Lib bytestreams src/test/scala/com/fsist/stream/$x.scala
done
