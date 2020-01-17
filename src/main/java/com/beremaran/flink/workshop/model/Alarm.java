package com.beremaran.flink.workshop.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Alarm {
    private String email;
    private String client;
    private Long loginAt;
}
