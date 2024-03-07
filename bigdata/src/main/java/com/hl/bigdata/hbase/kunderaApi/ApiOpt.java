package com.hl.bigdata.hbase.kunderaApi;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.kundera.client.Client;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.junit.Before;
import org.junit.Test;

/* * 
 * kundera的api操作
 * @Author: huanglin 
 * @Date: 2022-02-03 21:56:44 
 */ 
public class ApiOpt {
    
    private EntityManagerFactory emf;

    @Before
    public void init() {
        try {
            emf = Persistence.createEntityManagerFactory("hbase_pu");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 查询
     */
    @Test
    public void find() {
        try {
            EntityManager em  = emf.createEntityManager();
            DoMain        res = em.find(DoMain.class, "row500000");

            System.out.println(res.getNo() + ", " + res.getName() + ", " + res.getAge());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     */
    @Test
    public void add() {
        try {
           EntityManager em  = emf.createEntityManager();
           em.getTransaction().begin();
           for(int i = 500000; i < 500010; i++ ) {
               DoMain domain = new DoMain();
               domain.setId("row" + i);
               domain.setNo("00" + i);
               domain.setName("jack_" + i);
               domain.setAge(i);
               em.persist(domain);
           }
           em.getTransaction().commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除数据
     */
    @Test
    public void delete() {
        try {
           EntityManager em     = emf.createEntityManager();
           DoMain        domain = new DoMain();
           domain.setId("row500001");
           em.remove(domain);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用querysql方式高级查询，面向对象的sql语
     */
    @Test
    public void findBySql() {
        try {
            EntityManager em   = emf.createEntityManager();
            String        sql  = "SELECT t FROM DoMain t where t.id = 'row500002'";
            Query         res  = em.createQuery(sql);
            List<DoMain>  list = res.getResultList();
            for(DoMain domain : list) {
                System.out.println(domain.getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用过滤器加sql
     */
    @Test
    public void findBySqlWithFilter() {
        try {
            EntityManager em = emf.createEntityManager();
            // 创建EntityManager实例并使用getDelegate方法来获取客户机的映射
            Map<String,Client> delegate = (Map<String, Client>) em.getDelegate();
            // 从hbase持久化单元的客户机映射中获取客户机
            HBaseClient client = (HBaseClient) delegate.get("hbase_pu");
            // 想要使用的任何过滤器创建一个过滤器对象，并将其设置为HbaseClient对象
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("name"), CompareOp.EQUAL, new SubstringComparator("jack_500002"));
            client.setFilter(filter);

            String        sql  = "SELECT t FROM DoMain t";
            Query         res  = em.createQuery(sql);
            List<DoMain>  list = res.getResultList();
            for(DoMain domain : list) {
                System.out.println(domain.getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
