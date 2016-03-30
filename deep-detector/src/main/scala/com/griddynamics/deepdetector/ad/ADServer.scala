package com.griddynamics.deepdetector.ad

import java.io.{BufferedOutputStream, PrintStream}
import java.net.ServerSocket
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.io.BufferedSource

/**
  * TODO apply asynchronous server socket channel
  */
object ADServer extends LazyLogging {

  val workersPool = Executors.newCachedThreadPool()

  def main(args: Array[String]) {
    //TODO arguments checks
    val server = new ServerSocket(args(0).toInt) //9999
    val pathToOutputFile = Paths.get(args(1)) //"/home/ipertushin/Documents/ts_proc.loadavg.1min_1.txt"

    val os = new BufferedOutputStream(Files.newOutputStream(pathToOutputFile))

    //TODO refactor to dynamic params
    // TODO load environment and query params from property file at startup or accept agent's config wia telnet after???
    val interval = 30 * 1000L
    val timeout = 15 * 1000L
    val query = new TSDBQuery("172.26.5.43", 4242)
      .forMetric("proc.loadavg.1min") //proc.meminfo.active //proc.stat.cpu //proc.meminfo.dirty
      .forInterval(interval)
      .withTimeout(timeout)

    val worker = new ADWorker(query,os)
    //TODO worker.setUncaughtExceptionHandler()

    workersPool.submit(worker)

    var isRunning = true
    while (isRunning) {
      val session = server.accept()
      val in = new BufferedSource(session.getInputStream()).getLines()
      val out = new PrintStream(session.getOutputStream())

      if (in.contains("stop")) {

        isRunning = false
        out.println("server shutdown...")
        logger.info("server shutdown...")

        workersPool.shutdownNow()
      } else {
        out.println("no such command...")
      } //TODO implement handling of a command for starting a AD-worker for an environment

      out.flush()
      session.close()
    }
  }
}
