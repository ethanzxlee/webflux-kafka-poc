package com.example.documentcontroller.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Document {
    private String id;
    private String title;
    private String content;
    private String lastModified;
    private String created;
    private long timeoutEpoch;
    private String status;
}
