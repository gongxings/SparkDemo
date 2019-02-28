object Test{

  /* 这是我的第一个 Scala 程序

    * 以下程序将输出'Hello World!'

    */

  def main(args: Array[String]) {

    val one =123;//val 声明的变量不可变
    val two ="123";
    val yy = one+two;
    var v = 233; //var 声明的变量可变
    v=32233;
    println(yy)
    val s :Int = 3223;

    for(i <- 1 to 3;j<- 1 to 3 if(i!=j))  println(i*10+j +" ")


  }

}