package com.griddynamics.deepdetector.ad

import scala.collection.mutable.ListBuffer

/**
  * TODO
  */
class TSDBQuery(server: String, port: Int) {

  private[this] val REST_ENDPOINT = s"api/query?"
  private[this] var tags: ListBuffer[(String, String)] = _
  private[this] var timeLineInterval = 30 * 1000L
  private[this] var waitingTimout = 15 * 1000L
  private[this] var start = (System.currentTimeMillis() - 60 * 60 * 1000L).toString
  private[this] var end = System.currentTimeMillis().toString
  private[this] var metric: String = _
  private[this] var aggregationFunc: String = "sum"

  def getServer() = server

  def getPort() = port

  def from(s: String) = {
    start = s
    this
  }

  def to(e: String) = {
    end = e
    this
  }

  def forMetric(m: String) = {
    metric = m
    this
  }

  def getMetric(): String = {
    metric
  }


  def getTimeout(): Long = {
    waitingTimout
  }

  def withAggregation(a: String) = {
    aggregationFunc = a
    this
  }

  def withTimeout(timeout: Long) = {
    waitingTimout = timeout
    this
  }

  def forInterval(interval: Long) = {
    timeLineInterval = interval
    this
  }

  @Deprecated
  def create(server: String, port: String): String = {
    s"http://${server}:${port}/${create()}"
  }

  def create(): String = {
    s"http://${server}:${port}/${REST_ENDPOINT}start=${start}&end=${end}&m=${aggregationFunc}:${metric}" //%7Bhost=ipetrushin%7D
  }

  @Deprecated
  def refresh(): Unit = {
    start = (System.currentTimeMillis() - getInterval()).toString
    end = System.currentTimeMillis().toString
  }

  def getInterval(): Long = {
    timeLineInterval
  }

  override def toString = create

}
