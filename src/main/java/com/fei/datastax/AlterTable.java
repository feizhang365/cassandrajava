package com.fei.datastax;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * <p>
 *   alter and use a table in Cassandra using Java API
 *   drive use https://github.com/datastax/java-driver
 * </p>
 * @author fzh
 *
 */
public class AlterTable {
    public static void main(String[] args){
        //alter
        String alterTableAddColumn = "ALTER TABLE emp ADD emp_email text";

        String alterTableDelColumn = "ALTER TABLE emp DROP emp_email;";

        //creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        //Creating Session object
        Session session = cluster.connect("tp");

        ///use tp
        //session.execute("USE tp");

        // Executing alter table delete column
        session.execute(alterTableDelColumn);
        // Executing alter table
        session.execute(alterTableAddColumn);

        System.out.println("alter Table emp  add emp_email text");
        session.close();
    }
}
