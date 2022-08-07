package sample

//sealed trait Option[+A]
//case class Some[+A](get: A) extends Option[A]
//case object None extends Option[Nothing]
//
//




object Optionium extends App {
  println("Active System ..")

  def testOption(o: Option[Int]) = {
    val result = o match {
      case Some(n) => n
      case None => "nothing"
    }

    result
  }

  println("res ->" + testOption(Option(1)))

  val a = None //Option(20)
  val c = a.fold(-1)(x=>x)
  println(c)

  val o1: Option[Int] = Some(10)
  println(o1.map(_.toString))

  val o2: Option[Int] = None
  assert(o2.map(_.toString).isEmpty)

  val player : Player = new Player {
    override def name: String = "YG"
    override def getFavoriteTeam: Option[String] = Option("NC")
  }

  val tournament = new Tournament {
    override def getTopScore(team: String): Option[Int] =
      team match {
        case "NC" => Option(99)
        case _ => Option(1)
      }
  }

  def getTopScore(player: Player, tournament: Tournament): Option[(Player, Int)] = {
    player.getFavoriteTeam.flatMap(tournament.getTopScore).map(score => (player, score))
  }

  val res = getTopScore(player, tournament)
  res.map((a) => {
    println(a._1.name + "=>" + a._2)
  })
//  println(s"p,t => ${res}")
}

trait Player {
  def name: String
  def getFavoriteTeam: Option[String]
}

trait Tournament {
  def getTopScore(team: String): Option[Int]
}