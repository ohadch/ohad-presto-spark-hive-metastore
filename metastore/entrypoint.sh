#!/bin/bash

bin/schematool -initSchema -dbType postgres --verbose
/opt/metastore/bin/start-metastore