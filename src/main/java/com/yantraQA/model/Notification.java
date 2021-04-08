package com.yantraQA.model;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Notification {
   private Long id;
   private String topicName;
   private String content;
   private NotificationType type;
}
