package com.griddynamics.analytics.darknet.utils

import java.util.regex.Pattern

import com.typesafe.scalalogging.slf4j.LazyLogging
import edu.uci.ics.crawler4j.crawler.{Page, WebCrawler}
import edu.uci.ics.crawler4j.url.WebURL

/**
  * TODO the idea is to crawl the web to find html-pages which content may fit to specific classification group
  */
object ClassificationCrawler extends WebCrawler with LazyLogging {

  val FILTERS = Pattern.compile(".*(\\.(css|js|gif|jpg|png|mp3|mp3|zip|gz))$")

  override def visit(page: Page): Unit = {
    super.visit(page)
    //TODO
  }

  override def shouldVisit(referringPage: Page, url: WebURL): Boolean = {
    super.shouldVisit(referringPage, url)
    //TODO filtering
  }

  /**
    * TODO The function is going to build TF-IDF vector from page's content
    * TODO and check whether the page fits to a predefined model for the ClassificationGroup
    *
    * @param page
    * @param classificationGroup
    * @return
    */
  def doesPageFitToClass(page: String, classificationGroup: String): Boolean = {
    //TODO
    false
  }
}
