/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.beremaran.flink.workshop.job;

import com.beremaran.flink.workshop.function.AccessAlertFilter;
import com.beremaran.flink.workshop.generator.LogSource;
import com.beremaran.flink.workshop.model.Alarm;
import com.beremaran.flink.workshop.model.Log;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamApiJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<Log> logStream =
                env.addSource(new LogSource(1, 5000));

        logStream.print();

        DataStream<Log> mustAlertStream =
                logStream
                        .keyBy(Log::getEmail)
                        .filter(new AccessAlertFilter());

        DataStream<Alarm> loginAlertStream =
                mustAlertStream.map(log -> new Alarm(log.getEmail(), log.getClient(), new Date().getTime()));

        loginAlertStream.print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
