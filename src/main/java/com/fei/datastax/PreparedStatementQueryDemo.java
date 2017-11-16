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

import com.datastax.driver.core.*;

/**
 * <p>
 *   Truncate a table in Cassandra using Java API
 *   drive use https://github.com/datastax/java-driver
 * </p>
 * <p>
 *     may cause error:https://www.datastax.com/dev/blog/allow-filtering-explained-2
 * </p>
 * @author fzh feizhang365@gmail.com
 * @date 2017/11/15
 */
public class PreparedStatementQueryDemo {
    public static void main(String[] args){
        String preparedQryCql = "SELECT catalog_id, journal, publisher, edition,title,author FROM tp.catalog WHERE journal=?";

        //creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        //Creating Session object
        Session session = cluster.connect("tp");
        ///use tp
        //session.execute("USE tp");

        //PreparedStatement
        PreparedStatement prepared = session.prepare(preparedQryCql);
        BoundStatement boundStmt = prepared.bind("Oracle Magazine");
        System.out.println(prepared.getQueryString());
        ResultSet results = session.execute(boundStmt);
        for (Row row : results) {
            System.out.println("Journal: " + row.getString("journal"));
        }

        session.close();
    }
}
