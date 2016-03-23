package com.griddynamics.deepdetector.opentsdb.agent

import java.io.{BufferedOutputStream, PrintStream}
import java.net.ServerSocket
import java.nio.file.{Files, Paths}

import com.griddynamics.deepdetector.opentsdb.client.{TSDBClient, TSDBQuery}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.io.BufferedSource

/**
  * TODO
  */
object TsdbAgentServer extends LazyLogging {


  def main(args: Array[String]) {
    //TODO arguments checks
    val server = new ServerSocket(args(0).toInt) //9999
    val pathToOutputFile = Paths.get(args(1)) //"/home/ipertushin/Documents/ts_proc.loadavg.1min_1.txt"

    val os = new BufferedOutputStream(Files.newOutputStream(pathToOutputFile))

    //TODO refactor to dynamic params
    // TODO load enviromnet and query params from property file at startup or accept agent's config wia telnet after???
    val interval = 30
    val query = new TSDBQuery()
      .forMetric("proc.loadavg.1min") //proc.meminfo.active //proc.stat.cpu
      .forInterval(interval)

    val worker = new TSDBClient("172.26.5.43", "4242")
    //TODO worker.setUncaughtExceptionHandler()
    worker.streamTS(query, os)

    var isRunning = true
    while (isRunning) {
      val session = server.accept()
      val in = new BufferedSource(session.getInputStream()).getLines()
      val out = new PrintStream(session.getOutputStream())

      if (in.contains("stop")) {
        worker.interrupt()
        isRunning = false
        out.println("server shutdown...")
      } else {
        out.println("no such command...")
      }

      out.flush()
      session.close()
    }
  }
}
