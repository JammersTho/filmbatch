package com.jamesfountain.filmbatch.model;

import lombok.Data;

@Data
public class FilmDTO {

    private String title;
    private String description;
    private int length;
    private String rating;

}