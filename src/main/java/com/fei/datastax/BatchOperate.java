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
 *   Using BATCH, you can execute multiple modification statements (insert, update, delete) simultaneiously
 *   <pre>
 *   BEGIN BATCH
 *     <insert-stmt>/ <update-stmt>/ <delete-stmt>
 *   APPLY BATCH
 *   </pre>
 *   drive use https://github.com/datastax/java-driver
 * </p>
 *
 * @author fzh feizhang365@gmail.com
 * @date 2017/11/15
 */
public class BatchOperate {
    public static void main(String[] args){
        String batchOperate = " BEGIN BATCH INSERT INTO emp (emp_id, emp_city, emp_name, emp_phone, emp_sal) values( 4,'Pune','rajeev',9848022331, 30000);"
                + "UPDATE emp SET emp_sal = 50000 WHERE emp_id =3;"
                + "DELETE emp_city FROM emp WHERE emp_id = 2;"
                + "APPLY BATCH;";

        //creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        //Creating Session object
        Session session = cluster.connect("tp");

        ///use tp
        //session.execute("USE tp");

        //Drop table
        session.execute(batchOperate);
        System.out.println("batch operation finish");
        session.close();
    }
}
