package com.example.documentprocessor.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Document {
    public String id;
    public String title;
    public String content;
    public String lastModified;
    public String created;
    public String status;
}
