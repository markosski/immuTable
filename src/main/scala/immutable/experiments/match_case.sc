
class User1(name: String)

case class User2(name: String)

val user1 = new User1("Marcinek") match {
    case User1(name) => println(s"This is User1 - $name")
    case _ => Unit
}

//val user2 = User2("Marcin") match {
//    case User2(name) => println(s"This is User2 - $name")
//    case _ => Unit
//}