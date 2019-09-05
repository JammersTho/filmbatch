package com.jamesfountain.filmbatch.config;

import com.jamesfountain.filmbatch.model.FilmDTO;
import com.jamesfountain.filmbatch.processor.FilmProcessor;
import com.jamesfountain.filmbatch.utility.StringHeaderWriter;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StepConfiguration {

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    /**
     * Read films from MySql DB, log each one then write to csv
     *
     * @return this step
     */
    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
                .<FilmDTO, FilmDTO>chunk(20)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    /**
     * Read mySql DB and map to FilmDTO object
     *
     * @return
     */
    @Bean
    public JdbcCursorItemReader<FilmDTO> reader() {
        JdbcCursorItemReader<FilmDTO> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);

        //TODO Extract sql to constant
        reader.setSql("SELECT title, description, length, rating FROM film;");
//        reader.setFetchSize(20);
//        reader.setMaxRows(20);
//        reader.setMaxItemCount(20);
        reader.setRowMapper(new FilmRowMapper());
        return reader;
    }

    public class FilmRowMapper implements RowMapper<FilmDTO> {
        @Override
        public FilmDTO mapRow(ResultSet rs, int rowNum) throws SQLException {
            FilmDTO film = new FilmDTO();
            film.setTitle(rs.getString("title"));
            film.setDescription(rs.getString("description"));
            film.setLength(rs.getInt("length"));
            film.setRating(rs.getString("rating"));

            return film;
        }
    }

    @Bean
    public FilmProcessor processor() {
        return new FilmProcessor();
    }

    /**
     * Writes a header + each DTO to csv
     *
     * @return the writer
     */
    @Bean
    public FlatFileItemWriter<FilmDTO> writer() {
        FlatFileItemWriter<FilmDTO> writer = new FlatFileItemWriter<FilmDTO>();
        writer.setHeaderCallback(new StringHeaderWriter("TITLE,DESCRIPTION,RATING,LENGTH"));

        //TODO this is not a good location to write to, how to write to the same dir the jar is in?
        writer.setResource(new FileSystemResource("/tmp/films.csv"));

        //Turn this on to append rather than overwrite
        //writer.setAppendAllowed(true);

        writer.setLineAggregator(new DelimitedLineAggregator<FilmDTO>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<FilmDTO>() {{
                setNames(new String[]{"title", "description", "rating", "length"});
            }});
        }});

        return writer;
    }
}
