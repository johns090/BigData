import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive._
import modelclasses._

object MyMain extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Mini Project"))
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  //sqlContext.sql("show databases").show
  //sqlContext.sql("select * from t1").show
  //println("Hello, world")

  import sqlContext.implicits._

  // 1) Prepare data Data Frame and temporary tables for trans logs

  val trans = sc.textFile("/data/source/miniproject/trans_log.csv").map(x=>x.split(","))
  println("Parsed reading file")
  var trans_TT = trans.filter(e =>e(1) == "TT")
  println("Parsed TT")

  var trans_PP = trans.filter(e =>e(1) == "PP")
  println("Parsed PP")

  var trans_LL= trans.filter(e =>e(1) == "LL")
  println("Parsed LL")

  //case class tt(Trans_Seq:Int, Trans_code:String,	scan_seq:String, Product_code:String, amount:Double, discount:Double, add_remove_flag:Int, store_num:String, OS_emp_num:Int, lane:Int, timestamp:String)
  val ttDf = trans_TT.map(e=>TT(e(0).toInt,e(1),e(2), e(3), e(4).toDouble, e(5).toDouble, e(6).toInt, e(7), e(8).toInt, e(9).toInt, e(10))).toDF()
  ttDf.registerTempTable("ttTable")
  sqlContext.sql("select * from ttTable").show()

  case class pp(Trans_Seq:Int, Trans_code:String,	promo_code:String, store_num:String, POS_emp_num:Int, lane:Int, timestamp:String)
  val ppDf = trans_PP.map(e=>pp(e(0).toInt,e(1),e(2), e(3), e(4).toInt, e(5).toInt, e(6))).toDF
  ppDf.registerTempTable("ppTable")
  sqlContext.sql("select * from ppTable").show

  case class ll(Trans_Seq:Int, Trans_code:String,	loyalty_card_no:String, store_num:String, POS_emp_num:Int, lane:Int, timestamp:String)
  val llDf = trans_LL.map(e=>ll(e(0).toInt,e(1),e(2), e(3), e(4).toInt, e(5).toInt, e(6))).toDF
  llDf.registerTempTable("llTable")
  sqlContext.sql("select * from llTable").show


  //1.5) UDF
  def getTransId(date_p:String, storeid_p:Int, lane_p:Int, trans_seq_p:Int): Long =
  {
    val date = date_p.split(" ")(0).replaceAll("-","")
    val storeid = "%05d".format(storeid_p)
    val lane = "%02d".format(lane_p)
    val trans_seq= "%04d".format(trans_seq_p)

    (date + storeid.toString + lane.toString + trans_seq.toString).toLong

  }

  val transId = getTransId(_,_,_,_)

 // tid used in spark df, but getTransId used in SqlContext

  val tid = sqlContext.udf.register("getTransId", transId)
  //sqlContext.sql("select getTransId('2018-01-01 222', 1, 2, 3) from ttTable ").show

  //2) Prepare target_stg table
  sqlContext.sql("use staging")

  sqlContext.sql("truncate table target_stg")

  sqlContext.sql("""
      with tt as (
      select getTransId(t.timestamp, s.store_id, t.lane, t.Trans_Seq ) as tx_id, t.* , s.store_id
      from ttTable t
      left join store s on s.store_num = t.store_num ) ,
      pp as (
      select getTransId(p.timestamp, s.store_id, p.lane, p.Trans_Seq ) as tx_id, p.promo_code
      from ppTable p
      left join store s on s.store_num = p.store_num ) ,
      ll as (
      select getTransId(l.timestamp, s.store_id, l.lane, l.Trans_Seq ) as tx_id, l.loyalty_card_no
      from llTable l
      left join store s on s.store_num = l.store_num )

      insert into target_stg
      select trans.tx_id  , trans.store_id, p.product_id, l.loyalty_member_id, pr.promo_cd_id, e.emp_id,  trans.amount, trans.discount, trans.timestamp

      from (
      select tt.tx_id  , tt.store_id, ll.loyalty_card_no, pp.promo_code,  tt.amount, tt.discount, tt.timestamp, tt.add_remove_flag, tt.product_code, tt.OS_emp_num
      from tt
      left join pp  on pp.tx_id = tt.tx_id
      left join ll on ll.tx_id = tt.tx_id
      ) as trans
      left join product p on p.product_cd = trans.product_code
      left join employee e on e.emp_num = trans.OS_emp_num
      left join promotions pr on pr.promo_cd = trans.promo_code
      left join loyalty l on l.card_no = trans.loyalty_card_no
      where trans.add_remove_flag = 1
    """).show

  sqlContext.sql("select * from target_stg")

  // 3) Prepare target_stg_remove table
  sqlContext.sql("truncate table target_stg_remove")

  sqlContext.sql("""
    with tt as (
    select getTransId(t.timestamp, s.store_id, t.lane, t.Trans_Seq ) as tx_id, t.* , s.store_id
    from ttTable t
    left join store s on s.store_num = t.store_num ) ,
    pp as (
    select getTransId(p.timestamp, s.store_id, p.lane, p.Trans_Seq ) as tx_id, p.promo_code
    from ppTable p
    left join store s on s.store_num = p.store_num ) ,
    ll as (
    select getTransId(l.timestamp, s.store_id, l.lane, l.Trans_Seq ) as tx_id, l.loyalty_card_no
    from llTable l
    left join store s on s.store_num = l.store_num )

    insert into target_stg_remove

    select trans.tx_id  , trans.store_id, p.product_id, l.loyalty_member_id, pr.promo_cd_id, e.emp_id,  trans.amount, trans.discount, trans.timestamp

    from (
    select tt.tx_id  , tt.store_id, ll.loyalty_card_no, pp.promo_code,  tt.amount, tt.discount, tt.timestamp, tt.add_remove_flag, tt.product_code, tt.OS_emp_num
    from tt
    left join pp  on pp.tx_id = tt.tx_id
    left join ll on ll.tx_id = tt.tx_id
    ) as trans

    left join product p on p.product_cd = trans.product_code
    left join employee e on e.emp_num = trans.OS_emp_num
    left join promotions pr on pr.promo_cd = trans.promo_code
    left join loyalty l on l.card_no = trans.loyalty_card_no
    where trans.add_remove_flag = -1
  """).show

  sqlContext.sql("select * from target_stg_remove")

  //4) remove returned transaction
  sqlContext.sql("""
  insert overwrite target_stg
  select s.tx_id  , s.store_id , s.product_id , s.loyalty_member_num , s.promo_code_id , s.emp_id , s.amt  , s.discount_amt , s.tx_date
  from
  (select *, RANK() over (partition by tx_id, product_id order by product_id)  as rank from target_stg ) as s
  left join
    (select *,  RANK() over (partition by tx_id, product_id order by product_id)  as rank from target_stg_remove  ) as r
  on s.tx_id = r.tx_id and s.product_id = r. product_id
  where r.store_id is null """)

  sqlContext.sql("select * from target_stg")

  //5) Populate to target table
  sqlContext.sql(" alter table targetdb.target drop partition (tx_date <> '') ")
  sqlContext.sql("insert into targetdb.target partition (tx_date) select Tx_Id, tStore_Id,Product_Id,Loyalty_Member_Num,Promo_Code_Id, Emp_Id, Amt, Discount_Amt, tx_date  from target_stg ")


  sqlContext.sql("select * from targetdb.target")


}
