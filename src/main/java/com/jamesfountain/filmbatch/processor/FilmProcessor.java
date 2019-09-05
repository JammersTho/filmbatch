package com.jamesfountain.filmbatch.processor;

import com.jamesfountain.filmbatch.model.FilmDTO;
import org.springframework.batch.item.ItemProcessor;

public class FilmProcessor implements ItemProcessor<FilmDTO, FilmDTO> {

    @Override
    public FilmDTO process(FilmDTO filmDTO) {
        System.out.println("Processing " + filmDTO.getTitle());
        return filmDTO;
    }


}