package com.beremaran.flink.workshop.factory;

import com.beremaran.flink.workshop.model.Log;

import java.util.Random;

public class LogFactory {
    private static final String[] IP_ADDRESSES = {
            "230.82.53.99",
            "255.220.113.245",
            "191.9.248.223",
            "168.12.237.73",
            "107.46.189.10"
    };

    private static final String[] EMAIL_ADDRESSES = {
            "john@doe.com",
            "jane@doe.com",
            "breanna.jakubowski@yahoo.com",
            "pedro42@nitzsche.com",
            "scotty15@quitzon.com",
            "vita.kuhn@bailey.com",
            "waelchi.greyson@yahoo.com",
            "afriesen@dicki.com",
            "santina24@haag.com"
    };

    private static final String[] DOMAINS = {};

    private static final Random RANDOM = new Random();

    private LogFactory() {
    }

    public static Log create() {
        Log log = new Log();

        log.setEmail(EMAIL_ADDRESSES[RANDOM.nextInt(EMAIL_ADDRESSES.length)]);
        log.setClient(IP_ADDRESSES[RANDOM.nextInt(IP_ADDRESSES.length)]);

        return log;
    }
}
