#!/bin/bash
scala -cp target/scala-2.11/FinNOR-assembly-1.0.jar wzk.akkalogger.server.AkkaLoggerServer | tee akka.logserver.log
