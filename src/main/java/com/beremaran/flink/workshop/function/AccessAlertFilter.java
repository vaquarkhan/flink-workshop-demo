package com.beremaran.flink.workshop.function;

import com.beremaran.flink.workshop.model.Log;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;

import java.util.HashSet;
import java.util.Set;

public class AccessAlertFilter extends RichFilterFunction<Log> {

    private transient MapState<String, Set<String>> pastLogins;

    @Override
    public boolean filter(Log log) throws Exception {
        if (!pastLogins.contains(log.getEmail())) {
            Set<String> clientIps = new HashSet<>();

            clientIps.add(log.getClient());

            pastLogins.put(log.getEmail(), clientIps);
            return false;
        }

        Set<String> userLogins = pastLogins.get(log.getEmail());

        if (!userLogins.contains(log.getClient())) {
            userLogins.add(log.getClient());

            return true;
        }

        return false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        pastLogins = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "pastLogins",
                        Types.STRING,
                        TypeInformation.of(
                                new TypeHint<Set<String>>() {
                                }
                        )
                )
        );
    }
}
