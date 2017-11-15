package com.fei.datastax;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * <p>
 *   create and use a table in Cassandra using Java API
 *   drive use https://github.com/datastax/java-driver
 * </p>
 * @author fzh
 *
 */
public class CreateTable {
    public static void main(String[] args){
        //create
        String createTable = "CREATE TABLE emp(emp_id int PRIMARY KEY, "
                + "emp_name text, "
                + "emp_city text, "
                + "emp_sal varint, "
                + "emp_phone varint );";

        String dropTable = "DROP TABLE emp";

        //creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        //Creating Session object
        Session session = cluster.connect("tp");

        ///use tp
        //session.execute("USE tp");

        //Drop table
        session.execute(dropTable);
        System.out.println("Table dropped");

        //Executing the query
        session.execute(createTable);

        System.out.println("Table emp created");
        session.close();
    }
}
