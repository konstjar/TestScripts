CREATE TEMP TABLE Codesets  (codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from public.CONCEPT where concept_id in (1322184)
UNION  select c.concept_id
  from public.CONCEPT c
  join public.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1322184)
  and c.invalid_reason is null

) I
) C;


CREATE TEMP TABLE qualified_events

AS
WITH primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id)  AS (
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, (C.drug_exposure_start_date + 1*INTERVAL'1 day')) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM public.DRUG_EXPOSURE de
JOIN Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Drug Exposure Criteria

  ) E
	JOIN public.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE (OP.OBSERVATION_PERIOD_START_DATE + 0*INTERVAL'1 day') <= E.START_DATE AND (E.START_DATE + 0*INTERVAL'1 day') <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
 SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id

FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;
ANALYZE qualified_events
;

--- Inclusion Rule Inserts

CREATE TEMP TABLE inclusion_events  (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

CREATE TEMP TABLE included_events

AS
WITH cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal)  AS (
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from qualified_events Q
    LEFT JOIN inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
 SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date

FROM
cteIncludedEvents Results
WHERE Results.ordinal = 1
;
ANALYZE included_events
;



-- generate cohort periods into #final_cohort
CREATE TEMP TABLE cohort_rows

AS
WITH cohort_ends (event_id, person_id, end_date)  AS (
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
 SELECT
person_id, start_date, end_date

FROM
first_ends;
ANALYZE cohort_rows
;

CREATE TEMP TABLE final_cohort

AS
WITH cteEndDates (person_id, end_date)  AS (	
	SELECT
		person_id
		, (event_date + -1 * 0*INTERVAL'1 day')  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, (end_date + 0*INTERVAL'1 day') as end_date
				, 1 AS event_type
				, NULL
			FROM cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
 SELECT
person_id, min(start_date) as start_date, end_date

FROM
cteEnds
group by person_id, end_date
;
ANALYZE final_cohort
;

DELETE FROM results.temp_cohort_suylxiet where cohort_definition_id = 3;
INSERT INTO results.temp_cohort_suylxiet (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 3 as cohort_definition_id, person_id, start_date, end_date 
FROM final_cohort CO
;





TRUNCATE TABLE cohort_rows;
DROP TABLE cohort_rows;

TRUNCATE TABLE final_cohort;
DROP TABLE final_cohort;

TRUNCATE TABLE inclusion_events;
DROP TABLE inclusion_events;

TRUNCATE TABLE qualified_events;
DROP TABLE qualified_events;

TRUNCATE TABLE included_events;
DROP TABLE included_events;

TRUNCATE TABLE Codesets;
DROP TABLE Codesets;

///////////////////////////////////////////////////////////////

CREATE TEMP TABLE Codesets  (codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from public.CONCEPT where concept_id in (1112807)
UNION  select c.concept_id
  from public.CONCEPT c
  join public.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1112807)
  and c.invalid_reason is null

) I
) C;


CREATE TEMP TABLE qualified_events

AS
WITH primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id)  AS (
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, (C.drug_exposure_start_date + 1*INTERVAL'1 day')) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM public.DRUG_EXPOSURE de
JOIN Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Drug Exposure Criteria

  ) E
	JOIN public.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE (OP.OBSERVATION_PERIOD_START_DATE + 0*INTERVAL'1 day') <= E.START_DATE AND (E.START_DATE + 0*INTERVAL'1 day') <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
 SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id

FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;
ANALYZE qualified_events
;

--- Inclusion Rule Inserts

CREATE TEMP TABLE inclusion_events  (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

CREATE TEMP TABLE included_events

AS
WITH cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal)  AS (
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from qualified_events Q
    LEFT JOIN inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
 SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date

FROM
cteIncludedEvents Results
WHERE Results.ordinal = 1
;
ANALYZE included_events
;



-- generate cohort periods into #final_cohort
CREATE TEMP TABLE cohort_rows

AS
WITH cohort_ends (event_id, person_id, end_date)  AS (
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
 SELECT
person_id, start_date, end_date

FROM
first_ends;
ANALYZE cohort_rows
;

CREATE TEMP TABLE final_cohort

AS
WITH cteEndDates (person_id, end_date)  AS (	
	SELECT
		person_id
		, (event_date + -1 * 0*INTERVAL'1 day')  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, (end_date + 0*INTERVAL'1 day') as end_date
				, 1 AS event_type
				, NULL
			FROM cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
 SELECT
person_id, min(start_date) as start_date, end_date

FROM
cteEnds
group by person_id, end_date
;
ANALYZE final_cohort
;

DELETE FROM results.temp_cohort_suylxiet where cohort_definition_id = 4;
INSERT INTO results.temp_cohort_suylxiet (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 4 as cohort_definition_id, person_id, start_date, end_date 
FROM final_cohort CO
;





TRUNCATE TABLE cohort_rows;
DROP TABLE cohort_rows;

TRUNCATE TABLE final_cohort;
DROP TABLE final_cohort;

TRUNCATE TABLE inclusion_events;
DROP TABLE inclusion_events;

TRUNCATE TABLE qualified_events;
DROP TABLE qualified_events;

TRUNCATE TABLE included_events;
DROP TABLE included_events;

TRUNCATE TABLE Codesets;
DROP TABLE Codesets;

///////////////////////////////////////////////////////////////

DROP TABLE IF EXISTS cov_ref;
DROP TABLE IF EXISTS analysis_ref;
CREATE TEMP TABLE cov_ref  (covariate_id BIGINT,
	covariate_name VARCHAR(512),
	analysis_id INT,
	analysis_name VARCHAR(512),
	concept_id INT
	);
CREATE TEMP TABLE analysis_ref  (analysis_id BIGINT,
	analysis_name VARCHAR(512),
	domain_id VARCHAR(20),
	
	start_day INT,
	end_day INT,

	is_binary VARCHAR(1),
	missing_means_zero VARCHAR(1)
	);
CREATE TEMP TABLE cov_1

AS
SELECT
CAST(gender_concept_id AS BIGINT) * 1000 + 1 AS covariate_id,
		

	COUNT(*) AS sum_value


FROM
results.temp_cohort_suylxiet cohort
INNER JOIN public.person
	ON cohort.subject_id = person.person_id
WHERE gender_concept_id != 0

	
	
		AND cohort.cohort_definition_id = 3
		
GROUP BY gender_concept_id

;
ANALYZE cov_1
;
INSERT INTO cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CAST(CONCAT('gender = ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
	1 AS analysis_id,
	 CAST((covariate_id - 1) / 1000 AS INT) AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM cov_1
	) t1
LEFT JOIN public.concept
	ON concept_id = CAST((covariate_id - 1) / 1000 AS INT);
INSERT INTO analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
SELECT 1 AS analysis_id,
	CAST('DemographicsGender' AS VARCHAR(512)) AS analysis_name,
	CAST('Demographics' AS VARCHAR(20)) AS domain_id,

	CAST(NULL AS INT) AS start_day,
	CAST(NULL AS INT) AS end_day,

	CAST('Y' AS VARCHAR(1)) AS is_binary,
	CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS charlson_concepts;
CREATE TEMP TABLE charlson_concepts  (diag_category_id INT,
	concept_id INT
	);
DROP TABLE IF EXISTS charlson_scoring;
CREATE TEMP TABLE charlson_scoring  (diag_category_id INT,
	diag_category_name VARCHAR(255),
	weight INT
	);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	1,
	'Myocardial infarction',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 1,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4329847);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	2,
	'Congestive heart failure',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 2,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (316139);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	3,
	'Peripheral vascular disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 3,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (321052);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	4,
	'Cerebrovascular disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 4,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	5,
	'Dementia',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 5,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4182210);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	6,
	'Chronic pulmonary disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 6,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4063381);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	7,
	'Rheumatologic disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 7,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	8,
	'Peptic ulcer disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 8,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4247120);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	9,
	'Mild liver disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 9,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4064161, 4212540);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	10,
	'Diabetes (mild to moderate)',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 10,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (201820);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	11,
	'Diabetes with chronic complications',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 11,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4192279, 443767, 442793);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	12,
	'Hemoplegia or paralegia',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 12,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (192606, 374022);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	13,
	'Renal disease',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 13,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4030518);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	14,
	'Any malignancy',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 14,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (443392);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	15,
	'Moderate to severe liver disease',
	3
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 15,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4245975, 4029488, 192680, 24966);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	16,
	'Metastatic solid tumor',
	6
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 16,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (432851);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	17,
	'AIDS',
	6
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 17,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (439727);
DROP TABLE IF EXISTS charlson_data;
DROP TABLE IF EXISTS charlson_stats;
DROP TABLE IF EXISTS charlson_prep;
DROP TABLE IF EXISTS charlson_prep2;
CREATE TEMP TABLE charlson_data


AS
SELECT
subject_id,
	cohort_start_date,
	SUM(weight) AS score

FROM
(
	SELECT DISTINCT charlson_scoring.diag_category_id,
		charlson_scoring.weight,

		cohort.subject_id,
		cohort.cohort_start_date
			
	FROM results.temp_cohort_suylxiet cohort
	INNER JOIN public.condition_era condition_era
		ON cohort.subject_id = condition_era.person_id
	INNER JOIN charlson_concepts charlson_concepts
		ON condition_era.condition_concept_id = charlson_concepts.concept_id
	INNER JOIN charlson_scoring charlson_scoring
		ON charlson_concepts.diag_category_id = charlson_scoring.diag_category_id

	WHERE condition_era_start_date <= (cohort.cohort_start_date + 0*INTERVAL'1 day')

		AND cohort.cohort_definition_id = 3
	) temp

GROUP BY subject_id,
			cohort_start_date
	
;
ANALYZE charlson_data

;
CREATE TEMP TABLE charlson_stats

AS
WITH t1  AS (
	SELECT COUNT(*) AS cnt 
	FROM results.temp_cohort_suylxiet 
	WHERE cohort_definition_id = 3
	),
t2 AS (
	SELECT COUNT(*) AS cnt, 
		MIN(score) AS min_score, 
		MAX(score) AS max_score, 
		SUM(score) AS sum_score,
		SUM(score * score) as squared_score
	FROM charlson_data
	)
 SELECT
CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
	t2.max_score AS max_value,
	CAST(t2.sum_score / (1.0 * t1.cnt) AS NUMERIC) AS average_value,
	CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS NUMERIC) AS standard_deviation,
	t2.cnt AS count_value,
	t1.cnt - t2.cnt AS count_no_value,
	t1.cnt AS population_size

FROM
t1, t2;
ANALYZE charlson_stats
;
CREATE TEMP TABLE charlson_prep

AS
SELECT
score,
	COUNT(*) AS total,
	ROW_NUMBER() OVER (ORDER BY score) AS rn

FROM
charlson_data
GROUP BY score;
ANALYZE charlson_prep
;
CREATE TEMP TABLE charlson_prep2	

AS
SELECT
s.score,
	SUM(p.total) AS accumulated

FROM
charlson_prep s
INNER JOIN charlson_prep p
	ON p.rn <= s.rn
GROUP BY s.score;
ANALYZE charlson_prep2	
;
CREATE TEMP TABLE cov_2

AS
SELECT
CAST(1000 + 901 AS BIGINT) AS covariate_id,

	o.count_value,
	o.min_value,
	o.max_value,
	CAST(o.average_value AS NUMERIC) average_value,
	CAST(o.standard_deviation AS NUMERIC) standard_deviation,
	CASE 
		WHEN .50 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .50 * o.population_size THEN score	END) 
		END AS median_value,
	CASE 
		WHEN .10 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .10 * o.population_size THEN score	END) 
		END AS p10_value,		
	CASE 
		WHEN .25 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .25 * o.population_size THEN score	END) 
		END AS p25_value,	
	CASE 
		WHEN .75 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .75 * o.population_size THEN score	END) 
		END AS p75_value,	
	CASE 
		WHEN .90 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .90 * o.population_size THEN score	END) 
		END AS p90_value		

FROM
charlson_prep2 p
CROSS JOIN charlson_stats o

GROUP BY o.count_value,
	o.count_no_value,
	o.min_value,
	o.max_value,
	o.average_value,
	o.standard_deviation,
	o.population_size;
ANALYZE cov_2
;
TRUNCATE TABLE charlson_data;
DROP TABLE charlson_data;
TRUNCATE TABLE charlson_stats;
DROP TABLE charlson_stats;
TRUNCATE TABLE charlson_prep;
DROP TABLE charlson_prep;
TRUNCATE TABLE charlson_prep2;
DROP TABLE charlson_prep2;
TRUNCATE TABLE charlson_concepts;
DROP TABLE charlson_concepts;
TRUNCATE TABLE charlson_scoring;
DROP TABLE charlson_scoring;
INSERT INTO cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CAST('Charlson index - Romano adaptation' AS VARCHAR(512)) AS covariate_name,
	901 AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM cov_2
	) t1;
INSERT INTO analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
SELECT 901 AS analysis_id,
	CAST('CharlsonIndex' AS VARCHAR(512)) AS analysis_name,
	CAST('Condition' AS VARCHAR(20)) AS domain_id,

	CAST(NULL AS INT) AS start_day,
	0 AS end_day,

	CAST('N' AS VARCHAR(1)) AS is_binary,
	CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMP TABLE cov_3

AS
SELECT
CAST(FLOOR((EXTRACT(YEAR FROM cohort_start_date) - year_of_birth) / 5) * 1000 + 3 AS BIGINT) AS covariate_id,
	

	COUNT(*) AS sum_value


FROM
results.temp_cohort_suylxiet cohort
INNER JOIN public.person
	ON cohort.subject_id = person.person_id


	WHERE cohort.cohort_definition_id = 3

		
GROUP BY FLOOR((EXTRACT(YEAR FROM cohort_start_date) - year_of_birth) / 5)

;
ANALYZE cov_3
;
INSERT INTO cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CAST(CONCAT (
		'age group: ',
		RIGHT(CONCAT('00', CAST(5 * (covariate_id - 3) / 1000 AS VARCHAR)), 2),
		'-',
		RIGHT(CONCAT('00', CAST((5 * (covariate_id - 3) / 1000) + 4 AS VARCHAR)), 2)
		) AS VARCHAR(512)) AS covariate_name,
	3 AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM cov_3
	) t1;
INSERT INTO analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
SELECT 3 AS analysis_id,
	CAST('DemographicsAgeGroup' AS VARCHAR(512)) AS analysis_name,
	CAST('Demographics' AS VARCHAR(20)) AS domain_id,

	CAST(NULL AS INT) AS start_day,
	CAST(NULL AS INT) AS end_day,

	CAST('Y' AS VARCHAR(1)) AS is_binary,
	CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
insert into results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id,
    count_value, min_value, max_value, avg_value, stdev_value, median_value,
    p10_value, p25_value, p75_value, p90_value, strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('DISTRIBUTION' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.count_value,
    f.min_value,
    f.max_value,
    f.average_value,
    f.standard_deviation,
    f.median_value,
    f.p10_value,
    f.p25_value,
    f.p75_value,
    f.p90_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    3 as cohort_definition_id,
    17 as cc_generation_id
  from (select 3 as cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from (SELECT covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value
FROM (
SELECT covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_2
) all_covariates) W) f
    join (select 3 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 3 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join public.concept c on c.concept_id = fr.concept_id;
insert into results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id, count_value, avg_value,
                                                 strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('PREVALENCE' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.sum_value     as count_value,
    f.average_value as stat_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    3 as cohort_definition_id,
    17 as cc_generation_id
  from (select 3 as cohort_definition_id, covariate_id, sum_value, average_value from (SELECT all_covariates.covariate_id,
  all_covariates.sum_value,
  CAST(all_covariates.sum_value / (1.0 * total.total_count) AS NUMERIC) AS average_value
FROM (SELECT covariate_id, sum_value FROM cov_1 UNION ALL
SELECT covariate_id, sum_value FROM cov_3
) all_covariates, (
SELECT COUNT(*) AS total_count
FROM results.temp_cohort_suylxiet 
WHERE cohort_definition_id = 3
) total) W) f
    join (select 3 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 3 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join public.concept c on c.concept_id = fr.concept_id;
TRUNCATE TABLE cov_1;
DROP TABLE cov_1;
TRUNCATE TABLE cov_2;
DROP TABLE cov_2;
TRUNCATE TABLE cov_3;
DROP TABLE cov_3;
TRUNCATE TABLE cov_ref;
DROP TABLE cov_ref;
TRUNCATE TABLE analysis_ref;
DROP TABLE analysis_ref;
DROP TABLE IF EXISTS cov_ref;
DROP TABLE IF EXISTS analysis_ref;
CREATE TEMP TABLE cov_ref  (covariate_id BIGINT,
	covariate_name VARCHAR(512),
	analysis_id INT,
	analysis_name VARCHAR(512),
	concept_id INT
	);
CREATE TEMP TABLE analysis_ref  (analysis_id BIGINT,
	analysis_name VARCHAR(512),
	domain_id VARCHAR(20),
	
	start_day INT,
	end_day INT,

	is_binary VARCHAR(1),
	missing_means_zero VARCHAR(1)
	);
CREATE TEMP TABLE cov_1

AS
SELECT
CAST(gender_concept_id AS BIGINT) * 1000 + 1 AS covariate_id,
		

	COUNT(*) AS sum_value


FROM
results.temp_cohort_suylxiet cohort
INNER JOIN public.person
	ON cohort.subject_id = person.person_id
WHERE gender_concept_id != 0

	
	
		AND cohort.cohort_definition_id = 4
		
GROUP BY gender_concept_id

;
ANALYZE cov_1
;
INSERT INTO cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CAST(CONCAT('gender = ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
	1 AS analysis_id,
	 CAST((covariate_id - 1) / 1000 AS INT) AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM cov_1
	) t1
LEFT JOIN public.concept
	ON concept_id = CAST((covariate_id - 1) / 1000 AS INT);
INSERT INTO analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
SELECT 1 AS analysis_id,
	CAST('DemographicsGender' AS VARCHAR(512)) AS analysis_name,
	CAST('Demographics' AS VARCHAR(20)) AS domain_id,

	CAST(NULL AS INT) AS start_day,
	CAST(NULL AS INT) AS end_day,

	CAST('Y' AS VARCHAR(1)) AS is_binary,
	CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS charlson_concepts;
CREATE TEMP TABLE charlson_concepts  (diag_category_id INT,
	concept_id INT
	);
DROP TABLE IF EXISTS charlson_scoring;
CREATE TEMP TABLE charlson_scoring  (diag_category_id INT,
	diag_category_name VARCHAR(255),
	weight INT
	);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	1,
	'Myocardial infarction',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 1,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4329847);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	2,
	'Congestive heart failure',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 2,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (316139);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	3,
	'Peripheral vascular disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 3,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (321052);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	4,
	'Cerebrovascular disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 4,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	5,
	'Dementia',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 5,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4182210);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	6,
	'Chronic pulmonary disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 6,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4063381);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	7,
	'Rheumatologic disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 7,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	8,
	'Peptic ulcer disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 8,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4247120);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	9,
	'Mild liver disease',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 9,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4064161, 4212540);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	10,
	'Diabetes (mild to moderate)',
	1
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 10,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (201820);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	11,
	'Diabetes with chronic complications',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 11,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4192279, 443767, 442793);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	12,
	'Hemoplegia or paralegia',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 12,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (192606, 374022);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	13,
	'Renal disease',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 13,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4030518);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	14,
	'Any malignancy',
	2
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 14,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (443392);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	15,
	'Moderate to severe liver disease',
	3
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 15,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (4245975, 4029488, 192680, 24966);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	16,
	'Metastatic solid tumor',
	6
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 16,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (432851);
INSERT INTO charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
VALUES (
	17,
	'AIDS',
	6
	);
INSERT INTO charlson_concepts (
	diag_category_id,
	concept_id
	)
SELECT 17,
	descendant_concept_id
FROM public.concept_ancestor
WHERE ancestor_concept_id IN (439727);
DROP TABLE IF EXISTS charlson_data;
DROP TABLE IF EXISTS charlson_stats;
DROP TABLE IF EXISTS charlson_prep;
DROP TABLE IF EXISTS charlson_prep2;
CREATE TEMP TABLE charlson_data


AS
SELECT
subject_id,
	cohort_start_date,
	SUM(weight) AS score

FROM
(
	SELECT DISTINCT charlson_scoring.diag_category_id,
		charlson_scoring.weight,

		cohort.subject_id,
		cohort.cohort_start_date
			
	FROM results.temp_cohort_suylxiet cohort
	INNER JOIN public.condition_era condition_era
		ON cohort.subject_id = condition_era.person_id
	INNER JOIN charlson_concepts charlson_concepts
		ON condition_era.condition_concept_id = charlson_concepts.concept_id
	INNER JOIN charlson_scoring charlson_scoring
		ON charlson_concepts.diag_category_id = charlson_scoring.diag_category_id

	WHERE condition_era_start_date <= (cohort.cohort_start_date + 0*INTERVAL'1 day')

		AND cohort.cohort_definition_id = 4
	) temp

GROUP BY subject_id,
			cohort_start_date
	
;
ANALYZE charlson_data

;
CREATE TEMP TABLE charlson_stats

AS
WITH t1  AS (
	SELECT COUNT(*) AS cnt 
	FROM results.temp_cohort_suylxiet 
	WHERE cohort_definition_id = 4
	),
t2 AS (
	SELECT COUNT(*) AS cnt, 
		MIN(score) AS min_score, 
		MAX(score) AS max_score, 
		SUM(score) AS sum_score,
		SUM(score * score) as squared_score
	FROM charlson_data
	)
 SELECT
CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
	t2.max_score AS max_value,
	CAST(t2.sum_score / (1.0 * t1.cnt) AS NUMERIC) AS average_value,
	CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS NUMERIC) AS standard_deviation,
	t2.cnt AS count_value,
	t1.cnt - t2.cnt AS count_no_value,
	t1.cnt AS population_size

FROM
t1, t2;
ANALYZE charlson_stats
;
CREATE TEMP TABLE charlson_prep

AS
SELECT
score,
	COUNT(*) AS total,
	ROW_NUMBER() OVER (ORDER BY score) AS rn

FROM
charlson_data
GROUP BY score;
ANALYZE charlson_prep
;
CREATE TEMP TABLE charlson_prep2	

AS
SELECT
s.score,
	SUM(p.total) AS accumulated

FROM
charlson_prep s
INNER JOIN charlson_prep p
	ON p.rn <= s.rn
GROUP BY s.score;
ANALYZE charlson_prep2	
;
CREATE TEMP TABLE cov_2

AS
SELECT
CAST(1000 + 901 AS BIGINT) AS covariate_id,

	o.count_value,
	o.min_value,
	o.max_value,
	CAST(o.average_value AS NUMERIC) average_value,
	CAST(o.standard_deviation AS NUMERIC) standard_deviation,
	CASE 
		WHEN .50 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .50 * o.population_size THEN score	END) 
		END AS median_value,
	CASE 
		WHEN .10 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .10 * o.population_size THEN score	END) 
		END AS p10_value,		
	CASE 
		WHEN .25 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .25 * o.population_size THEN score	END) 
		END AS p25_value,	
	CASE 
		WHEN .75 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .75 * o.population_size THEN score	END) 
		END AS p75_value,	
	CASE 
		WHEN .90 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .90 * o.population_size THEN score	END) 
		END AS p90_value		

FROM
charlson_prep2 p
CROSS JOIN charlson_stats o

GROUP BY o.count_value,
	o.count_no_value,
	o.min_value,
	o.max_value,
	o.average_value,
	o.standard_deviation,
	o.population_size;
ANALYZE cov_2
;
TRUNCATE TABLE charlson_data;
DROP TABLE charlson_data;
TRUNCATE TABLE charlson_stats;
DROP TABLE charlson_stats;
TRUNCATE TABLE charlson_prep;
DROP TABLE charlson_prep;
TRUNCATE TABLE charlson_prep2;
DROP TABLE charlson_prep2;
TRUNCATE TABLE charlson_concepts;
DROP TABLE charlson_concepts;
TRUNCATE TABLE charlson_scoring;
DROP TABLE charlson_scoring;
INSERT INTO cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CAST('Charlson index - Romano adaptation' AS VARCHAR(512)) AS covariate_name,
	901 AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM cov_2
	) t1;
INSERT INTO analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
SELECT 901 AS analysis_id,
	CAST('CharlsonIndex' AS VARCHAR(512)) AS analysis_name,
	CAST('Condition' AS VARCHAR(20)) AS domain_id,

	CAST(NULL AS INT) AS start_day,
	0 AS end_day,

	CAST('N' AS VARCHAR(1)) AS is_binary,
	CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMP TABLE cov_3

AS
SELECT
CAST(FLOOR((EXTRACT(YEAR FROM cohort_start_date) - year_of_birth) / 5) * 1000 + 3 AS BIGINT) AS covariate_id,
	

	COUNT(*) AS sum_value


FROM
results.temp_cohort_suylxiet cohort
INNER JOIN public.person
	ON cohort.subject_id = person.person_id


	WHERE cohort.cohort_definition_id = 4

		
GROUP BY FLOOR((EXTRACT(YEAR FROM cohort_start_date) - year_of_birth) / 5)

;
ANALYZE cov_3
;
INSERT INTO cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CAST(CONCAT (
		'age group: ',
		RIGHT(CONCAT('00', CAST(5 * (covariate_id - 3) / 1000 AS VARCHAR)), 2),
		'-',
		RIGHT(CONCAT('00', CAST((5 * (covariate_id - 3) / 1000) + 4 AS VARCHAR)), 2)
		) AS VARCHAR(512)) AS covariate_name,
	3 AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM cov_3
	) t1;
INSERT INTO analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
SELECT 3 AS analysis_id,
	CAST('DemographicsAgeGroup' AS VARCHAR(512)) AS analysis_name,
	CAST('Demographics' AS VARCHAR(20)) AS domain_id,

	CAST(NULL AS INT) AS start_day,
	CAST(NULL AS INT) AS end_day,

	CAST('Y' AS VARCHAR(1)) AS is_binary,
	CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
insert into results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id,
    count_value, min_value, max_value, avg_value, stdev_value, median_value,
    p10_value, p25_value, p75_value, p90_value, strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('DISTRIBUTION' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.count_value,
    f.min_value,
    f.max_value,
    f.average_value,
    f.standard_deviation,
    f.median_value,
    f.p10_value,
    f.p25_value,
    f.p75_value,
    f.p90_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    4 as cohort_definition_id,
    17 as cc_generation_id
  from (select 4 as cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from (SELECT covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value
FROM (
SELECT covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_2
) all_covariates) W) f
    join (select 4 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 4 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join public.concept c on c.concept_id = fr.concept_id;
insert into results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id, count_value, avg_value,
                                                 strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('PREVALENCE' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.sum_value     as count_value,
    f.average_value as stat_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    4 as cohort_definition_id,
    17 as cc_generation_id
  from (select 4 as cohort_definition_id, covariate_id, sum_value, average_value from (SELECT all_covariates.covariate_id,
  all_covariates.sum_value,
  CAST(all_covariates.sum_value / (1.0 * total.total_count) AS NUMERIC) AS average_value
FROM (SELECT covariate_id, sum_value FROM cov_1 UNION ALL
SELECT covariate_id, sum_value FROM cov_3
) all_covariates, (
SELECT COUNT(*) AS total_count
FROM results.temp_cohort_suylxiet 
WHERE cohort_definition_id = 4
) total) W) f
    join (select 4 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 4 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join public.concept c on c.concept_id = fr.concept_id;
TRUNCATE TABLE cov_1;
DROP TABLE cov_1;
TRUNCATE TABLE cov_2;
DROP TABLE cov_2;
TRUNCATE TABLE cov_3;
DROP TABLE cov_3;
TRUNCATE TABLE cov_ref;
DROP TABLE cov_ref;
TRUNCATE TABLE analysis_ref;
DROP TABLE analysis_ref;