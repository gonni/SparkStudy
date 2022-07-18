package com.yg

import com.yg.KorAnalyzer.komoran
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object KorAnalyzerConsole {
  def main(args: Array[String]): Unit = {
    println("Active System ..")

    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.setUserDic("/Users/a1000074/dev/works/SparkStudy/myDic.txt")

//    val sentence = "컬링은 선수 요청이 없으면 심판이나 코치가 개입할 수 없는 종목이다. 잘 운영이 될 수 있을 것이다.";
    val sentence = "아이폰 기다리다 지쳐 애플공홈에서 언락폰질러버렸다 6+ 128기가실버ㅋ"
    val tokens = komoran.analyze(sentence).getTokenList.asScala.map(x => x.getMorph)

    tokens.map(println)

    println("============= ONLY Nouns ==============")
    val nouns = komoran.analyze(sentence).getNouns.asScala
    nouns.map(println)

    println(nouns.mkString("|"))
//    tokens.flatMap(token => token.mkString).map(println)
  }
}
