package com.yg.ml.kor

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import scala.jdk.CollectionConverters.asScalaBufferConverter

object AnalyzerTester {
  def main(args: Array[String]): Unit = {
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.setUserDic("./myDic.txt")

    komoran.analyze("아이브,日 정식 데뷔전부터 후지TV 테마송부터 공연·프로모션 성공적").getNouns.asScala.foreach(println)
  }
}
