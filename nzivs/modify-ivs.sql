

--------------Main header---------------
ALTER TABLE nzivs.survey_main_header ADD COLUMN weight_x_spend DOUBLE PRECISION;
ALTER TABLE nzivs.survey_main_header ADD COLUMN weight_x_los DOUBLE PRECISION;

UPDATE nzivs.survey_main_header SET weight_x_spend = population_weight * weighted_spend;
UPDATE nzivs.survey_main_header SET weight_x_los = population_weight * length_of_stay;


------------Satisfaction ratings-----------------
ALTER TABLE nzivs.satisfaction_ratings ADD COLUMN  weight_x_satisfaction DOUBLE PRECISION;
ALTER TABLE nzivs.satisfaction_ratings ADD COLUMN  satisfaction_rating_num DOUBLE PRECISION;

-- Extract numbers from the current text ratings
-- See https://stackoverflow.com/questions/40564475/extract-numbers-from-a-field-in-postgresql
UPDATE nzivs.satisfaction_ratings 
SET satisfaction_rating_num = NULLIF(REGEXP_REPLACE(satisfaction_rating, '\D', '', 'g'), '')::NUMERIC;

-- Multiply ratings by weight.  Updating in situ is very very slow; see
-- https://stackoverflow.com/questions/3361291/slow-simple-update-query-on-postgresql-database-with-3-million-rows
-- for an explanation.  So it's vastly quicker to create  a temporary table then rename it

CREATE TABLE nzivs.temp_satisfaction AS
    SELECT 
        a.survey_response_id, 
        a.answer_number, 
        a.satisfaction_rating,
        a.amenity_type,
        a.satisfaction_rating_num,
        b.population_weight * a.satisfaction_rating_num AS weight_x_satisfaction 
    FROM 
        nzivs.satisfaction_ratings AS a
    LEFT JOIN nzivs.survey_main_header AS b
        ON a.survey_response_id = b.survey_response_id;

DROP TABLE nzivs.satisfaction_ratings;
ALTER TABLE nzivs.temp_satisfaction RENAME TO satisfaction_ratings;
CREATE INDEX sri_satisfaction_ratings ON nzivs.satisfaction_ratings(survey_response_id);

