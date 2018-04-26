#!/bin/sh
#./gradlew -PhadoopVersion=2.7.3 -PhiveVersion=1.0.1 -PgobblinFlavor=prezi assemble -Pversion=0.11.0-20170822-prezi -PexcludeHadoopDeps -PexcludeHiveDeps
./gradlew -PhadoopVersion=2.7.3 -PhiveVersion=1.0.1 -PgobblinFlavor=prezi assemble -Pversion=0.11.0-20170822-prezi
