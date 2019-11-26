package com.mina.kafka.reassignment.test.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Message {

  private Integer index;
  private String description;

}
