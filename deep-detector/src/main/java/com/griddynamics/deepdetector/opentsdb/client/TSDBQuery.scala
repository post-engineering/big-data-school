package com.griddynamics.deepdetector.opentsdb.client

/**
  * TODO
  */
class TSDBQuery {

  private[this] val REST_ENDPOINT = s"api/query?"
  private[this] var interval = 0
  private[this] var start = System.currentTimeMillis() - 60 * 60 * 1000L
  private[this] var end = System.currentTimeMillis()
  private[this] var metric: String = _
  private[this] var aggregationFunc: String = "sum"

  def from(s: Long) = {
    start = s
    this
  }

  def to(e: Long) = {
    end = e
    this
  }

  def forMetric(m: String) = {
    metric = m
    this
  }

  def getIntervalInMS(): Long = {
    interval * 1000L
  }

  def withAggregation(a: String) = {
    aggregationFunc = a
    this
  }

  def forInterval(intervalInSec: Int) = {
    interval = intervalInSec
    this
  }


  def create(server: String, port: String): String = {
    s"http://${server}:${port}/${create()}"
  }

  def create(): String = {
    refresh
    s"${REST_ENDPOINT}start=${start}&end=${end}&m=${aggregationFunc}:${metric}"
  }

  def refresh(): Unit =  {
    start = System.currentTimeMillis() - getIntervalInMS()
    end = System.currentTimeMillis()
  }

  override def toString = create

}
