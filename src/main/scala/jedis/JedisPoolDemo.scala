package jedis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author WangLeiKai
  *         2018/9/30  9:26
  */
object JedisPoolDemo {

  val config = new JedisPoolConfig()
  config.setMaxTotal(20)
  config.setMaxIdle(10)


  val pool = new JedisPool(config,"192.168.59.11",6379,10000,"123456")

  def getConnection(): Jedis = {
    pool.getResource
  }


  def main(args: Array[String]): Unit = {
    val conn = JedisPoolDemo.getConnection()
    conn.set("income","10000")

    val j1 = conn.get("income")
    println(j1)
    val j2 = conn.incrBy("income",50)
    println(j2)

    conn.close()


  }
}
