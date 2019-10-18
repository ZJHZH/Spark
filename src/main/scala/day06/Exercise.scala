package day06

import org.apache.spark.sql.SparkSession

/**
 * date 2019-10-15 17:38<br>
 *
 * @author ZJHZH
 */
object Exercise {
  private val spark: SparkSession = SparkSession.builder().appName("Exercise").config("spark.sql.warehouse.dir","hdfs://hadoop04:9000/spark_warehouse").master("spark://hadoop04:7077").enableHiveSupport().getOrCreate()

  def main(args: Array[String]): Unit = {
//    create_database_table()
    spark.sql("use spark_1901")
//    load_data_overwrite()

    // 测试数据是否存在
//    spark.sql("select * from spark_1901.dept").show()
//    spark.sql("select * from spark_1901.emp").show()

    // 列出至少有一个员工的所有部门。
    spark.sql("select distinct deptno from emp").show()

    // 列出薪金比"刘一"多的所有员工。
    spark.sql("select emp.ename from emp join emp as e on e.ename = '刘一' where emp.sal > e.sal").show()

    // 列出所有员工的姓名及其直接上级的姓名。
    spark.sql("select \nemp.ename as ename,\nm.ename as mname\nfrom emp join emp as m on emp.mgr = m.empno\nwhere emp.mgr is not null").show()

//    列出受雇日期早于其直接上级的所有员工。
    spark.sql("""select
                |emp.ename
                |from emp join emp as m on emp.mgr = m.empno
                |where emp.mgr is not null and emp.hiredate < m.hiredate""".stripMargin).show()

//    列出部门名称和这些部门的员工信息，同时列出那些没有员工的部门。
    spark.sql("""select
                |dept.dname,
                |dept.ioc,
                |emp.*
                |from dept join emp on dept.deptno = emp.deptno""".stripMargin).show()

//    列出所有job为“职员”的姓名及其部门名称。
    spark.sql("""select
                |e.ename,
                |d.dname
                |from emp as e join dept as d on e.deptno = d.deptno and e.job = '职员'""".stripMargin).show()

//    列出最低薪金大于1500的各种工作。
    spark.sql("""select
                |job
                |from emp
                |group by job
                |having min(sal) > 1500""".stripMargin).show()

//    列出在部门 "销售部" 工作的员工的姓名，假定不知道销售部的部门编号。
    spark.sql("""select
                |e.ename
                |from emp as e join dept as d on d.dname = '销售部' and d.deptno = e.deptno""".stripMargin).show()

//    列出薪金高于公司平均薪金的所有员工。
    spark.sql("""select ename
                |from (
                |select
                |ename,
                |sal,
                |avg(sal) over(partition by 1) as avg_sal
                |from emp
                |) tmp
                |where sal > avg_sal""".stripMargin).show()

//    列出在每个部门工作的员工数量、平均工资。
    spark.sql("""select dept.deptno,
                |count(1) as enum,
                |round(avg(sal),2) as avg_sal
                |from emp right join dept on emp.deptno = dept.deptno
                |group by dept.deptno""".stripMargin).show()

//    列出所有员工的姓名、部门名称和工资。
    spark.sql("""select e.ename,
                |d.dname,
                |e.sal + nvl(e.comm,0) as sal
                |from emp as e join dept as d on e.deptno = d.deptno""".stripMargin).show()

//    查出emp表中薪水在3000以上（包括3000）的所有员工的员工号、姓名、薪水。
    spark.sql("""select empno,
                |ename,
                |sal
                |from emp
                |where sal >= 3000""".stripMargin).show()

//    查询出所有薪水在'陈二'之上的所有人员信息。
    spark.sql("""select e.*
                |from emp as e join emp s on s.ename = '陈二'
                |where e.sal > s.sal""".stripMargin).show()

//    查出emp表中所有部门的最高薪水和最低薪水，部门编号为10的部门不显示。
    spark.sql("""select dept.deptno,
                |max(sal) as max_sal,
                |min(sal) as min_sal
                |from emp join dept on emp.deptno = dept.deptno
                |where dept.deptno != 10
                |group by dept.deptno""".stripMargin).show()

    // 删除10号部门薪水最高的员工。
    // hive 无法删除，更新某一条数据。
//    spark.sql("delete from emp where deptno = 10 order by sal desc limit 1")

  }

  /**
   * 创建库
   * 创建表
   */
  def create_database_table(): Unit ={
    // 创建库，这个if not exists 不顶事儿
    spark.sql("create database if not exists spark_1901")
    // 创建dept表
    spark.sql(
      """
        |create table if not exists spark_1901.dept(
        |deptno int,
        |dname string,
        |ioc string
        |)
        |row format delimited
        |fields terminated by ','
        |""".stripMargin)
    // 创建emp 表
    spark.sql(
      """
        |create table if not exists spark_1901.emp(
        |empno int,
        |ename string,
        |job string,
        |mgr int,
        |hiredate string,
        |sal double,
        |comm double,
        |deptno int
        |)
        |row format delimited
        |fields terminated by ','
        |""".stripMargin)
  }

  /**
   * 加载数据
   * 覆写
   */
  def load_data_overwrite(): Unit ={
    // 向spark_1901.dept中加载数据
    spark.sql("load data local inpath 'dir/dept' overwrite into table spark_1901.dept")
    // 向spark_1901.emp中加载数据
    spark.sql("load data local inpath 'dir/emp' overwrite into table spark_1901.emp")
  }
}
