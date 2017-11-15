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
import com.datastax.driver.core.Session;

/**
 * <p>
 *   insert data into the columns of a row in a table using the command INSERT in Cassandra using Java API
 *   drive use https://github.com/datastax/java-driver
 * </p>
 *
 * @author fzh feizhang365@gmail.com
 * @date 2017/11/15
 */
public class InsertData {
    public static void main(String[] args){
        String insert1 = "INSERT INTO emp (emp_id, emp_name, emp_city, emp_phone, emp_sal) VALUES (1,'ram', 'Hyderabad', 9848022338, 70000);" ;
        String insert2 = "INSERT INTO emp (emp_id, emp_name, emp_city, emp_phone, emp_sal) VALUES(2,'robin', 'Hyderabad', 9848022339, 40000);" ;
        String insert3 = "INSERT INTO emp (emp_id, emp_name, emp_city, emp_phone, emp_sal) VALUES(3,'rahman', 'Chennai', 9848022330, 45000);" ;
        //creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        //Creating Session object
        Session session = cluster.connect("tp");

        ///use tp
        //session.execute("USE tp");

        //insert data into table
        session.execute(insert1);
        session.execute(insert2);
        session.execute(insert3);
        System.out.println("Insert data into Table");
        session.close();
    }
}
