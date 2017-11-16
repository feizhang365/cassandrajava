/**
 * Copyright (c) 2017-2020, fzh (feizhang365@gmail.com).
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.fei.datastax;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * <p>
 *   mix demo in Cassandra using Java API
 *   drive use https://github.com/datastax/java-driver
 * </p>
 *
 * @author fzh feizhang365@gmail.com
 * @date 2017/11/15
 */
public class MixDemo {
    public static void main(String[] args){
        String createCql = "CREATE TABLE IF NOT EXISTS tp.catalog (catalog_id text, journal text,publisher text, edition text,title text,author text,PRIMARY KEY (catalog_id, journal))" ;
        String insertCql = "INSERT INTO tp.catalog (catalog_id, journal, publisher, edition, title,author) VALUES ('catalog', 'Oracle Publishing','Oracle Publishing', 'November-December 2013', 'Java 8 in action ','Jeffery') IF NOT EXISTS";
        String insertCql1 = "INSERT INTO tp.catalog (catalog_id, journal, publisher, edition,title,author) VALUES ('catalog1','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', 'Engineering as a Service','David A. Kelly') IF NOT EXISTS";
        String insertCql2 = "INSERT INTO tp.catalog (catalog_id, journal, publisher, edition,title,author) VALUES ('catalog2','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', 'Quintessential and Collaborative','Tom Haunert') IF NOT EXISTS";
        String updateCql = "UPDATE tp.catalog SET edition = '11/12 2013', author = 'David A. Kelly' WHERE catalog_id = 'catalog1' AND journal='Oracle Magazine' IF edition='November-December 2013'";
        //creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        //Creating Session object
        Session session = cluster.connect("tp");

        ///use tp
        //session.execute("USE tp");

        //create table
        session.execute(createCql);
        System.out.println("Table Created");
        //insert data
        session.execute(insertCql);
        session.execute(insertCql1);
        session.execute(insertCql2);
        System.out.println("Table Inserted");
        session.execute(updateCql);
        System.out.println("Data updated");

        ResultSet results = session.execute("select * from tp.catalog");
        for (Row row: results) {
            System.out.println("Catalog Id: " + row.getString("catalog_id"));
            System.out.println("Journal: " + row.getString("journal"));
            System.out.println("Publisher: " + row.getString("publisher"));
            System.out.println("Edition: " + row.getString("edition"));
            System.out.println("Title: " + row.getString("title"));
            System.out.println("Author: " + row.getString("author"));
            System.out.println("\n");
            System.out.println("\n");
        }

        session.close();
    }
}
