------------------move from the public staging schema to where we want them-------
DROP VIEW IF EXISTS nzis2011.vw_mainheader;
DROP TABLE IF EXISTS nzis2011.d_sex;
DROP TABLE IF EXISTS nzis2011.d_agegrp;
DROP TABLE IF EXISTS nzis2011.d_ethnicity;
DROP TABLE IF EXISTS nzis2011.d_occupation;
DROP TABLE IF EXISTS nzis2011.d_qualification;
DROP TABLE IF EXISTS nzis2011.d_region;
DROP TABLE IF EXISTS nzis2011.f_mainheader;


ALTER TABLE public.f_mainheader     SET SCHEMA nzis2011;
ALTER TABLE public.d_sex            SET SCHEMA nzis2011;
ALTER TABLE public.d_agegrp         SET SCHEMA nzis2011;
ALTER TABLE public.d_ethnicity      SET SCHEMA nzis2011;
ALTER TABLE public.d_occupation     SET SCHEMA nzis2011;
ALTER TABLE public.d_qualification  SET SCHEMA nzis2011;
ALTER TABLE public.d_region         SET SCHEMA nzis2011;


----------------indexing----------------------
ALTER TABLE nzis2011.f_mainheader     ADD PRIMARY KEY(survey_id);

ALTER TABLE nzis2011.d_sex            ADD PRIMARY KEY(sex_id);
ALTER TABLE nzis2011.d_agegrp         ADD PRIMARY KEY(agegrp_id);
ALTER TABLE nzis2011.d_ethnicity      ADD PRIMARY KEY(ethnicity_id);
ALTER TABLE nzis2011.d_occupation     ADD PRIMARY KEY(occupation_id);
ALTER TABLE nzis2011.d_qualification  ADD PRIMARY KEY(qualification_id);
ALTER TABLE nzis2011.d_region         ADD PRIMARY KEY(region_id);

---------------create an analysis-ready view-------------------
-- we include the xxx_id columns too in case people want to use them
-- for the original ordering



CREATE VIEW nzis2011.vw_mainheader AS 
 SELECT 
  survey_id, hours, income
  sex, agegrp, occupation, qualification, region, 
  a.sex_id, a.agegrp_id, a.occupation_id, a.qualification_id, a.region_id
 FROM
    nzis2011.f_mainheader AS a   JOIN
    nzis2011.d_sex AS b          on a.sex_id = b.sex_id JOIN
    nzis2011.d_agegrp AS c       on a.agegrp_id = c.agegrp_id JOIN
    nzis2011.d_occupation AS e   on a.occupation_id = e.occupation_id JOIN
    nzis2011.d_qualification AS f on a.qualification_id = f.qualification_id JOIN
    nzis2011.d_region AS g       on a.region_id = g.region_id;
