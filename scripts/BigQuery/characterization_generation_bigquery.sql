create table synpuf_110k_results.jz75rg2xcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.jz75rg2xcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1322184)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1322184)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.jz75rg2xqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.jz75rg2xcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) c


-- End Drug Exposure Criteria

  ) e
	join synpuf_110k.observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(cast(op.observation_period_start_date as date), interval 0 DAY) <= e.start_date and DATE_ADD(cast(e.start_date as date), interval 0 DAY) <= op.observation_period_end_date
) p
where p.ordinal = 1
-- End Primary Events

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
 FROM (
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date asc) as ordinal, cast(pe.visit_occurrence_id  as int64) as visit_occurrence_id
  from primary_events pe
  
) qe

;

--- Inclusion Rule Inserts

create table synpuf_110k_results.jz75rg2xinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.jz75rg2xincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.jz75rg2xqualified_events q
    left join synpuf_110k_results.jz75rg2xinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;



-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.jz75rg2xcohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,op_end_date  as end_date from synpuf_110k_results.jz75rg2xincluded_events
), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.jz75rg2xincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.jz75rg2xfinal_cohort
  AS WITH cteenddates   as (select person_id
		 as person_id,DATE_ADD(cast(event_date as date), interval -1 * 0 DAY)   as end_date from (
		select
			person_id
			, event_date
			, event_type
			, max(start_ordinal) over (partition by person_id order by event_date, event_type rows unbounded preceding) as start_ordinal 
			, row_number() over (partition by person_id order by event_date, event_type) as overall_ord
		from
		(
			select
				person_id
				, start_date as event_date
				, -1 as event_type
				, row_number() over (partition by person_id order by start_date) as start_ordinal
			from synpuf_110k_results.jz75rg2xcohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.jz75rg2xcohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.jz75rg2xcohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_qfsybqv7 where cohort_definition_id = 3;
insert into synpuf_110k_results.temp_cohort_qfsybqv7 (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 3 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.jz75rg2xfinal_cohort co
;





DELETE FROM synpuf_110k_results.jz75rg2xcohort_rows WHERE True;
drop table synpuf_110k_results.jz75rg2xcohort_rows;

DELETE FROM synpuf_110k_results.jz75rg2xfinal_cohort WHERE True;
drop table synpuf_110k_results.jz75rg2xfinal_cohort;

DELETE FROM synpuf_110k_results.jz75rg2xinclusion_events WHERE True;
drop table synpuf_110k_results.jz75rg2xinclusion_events;

DELETE FROM synpuf_110k_results.jz75rg2xqualified_events WHERE True;
drop table synpuf_110k_results.jz75rg2xqualified_events;

DELETE FROM synpuf_110k_results.jz75rg2xincluded_events WHERE True;
drop table synpuf_110k_results.jz75rg2xincluded_events;

DELETE FROM synpuf_110k_results.jz75rg2xcodesets WHERE True;
drop table synpuf_110k_results.jz75rg2xcodesets;



///////////////////////////////////////////////////////////////



create table synpuf_110k_results.tg0hbjfscodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.tg0hbjfscodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1112807)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1112807)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.tg0hbjfsqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.tg0hbjfscodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) c


-- End Drug Exposure Criteria

  ) e
	join synpuf_110k.observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(cast(op.observation_period_start_date as date), interval 0 DAY) <= e.start_date and DATE_ADD(cast(e.start_date as date), interval 0 DAY) <= op.observation_period_end_date
) p
where p.ordinal = 1
-- End Primary Events

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
 FROM (
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date asc) as ordinal, cast(pe.visit_occurrence_id  as int64) as visit_occurrence_id
  from primary_events pe
  
) qe

;

--- Inclusion Rule Inserts

create table synpuf_110k_results.tg0hbjfsinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.tg0hbjfsincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.tg0hbjfsqualified_events q
    left join synpuf_110k_results.tg0hbjfsinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;



-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.tg0hbjfscohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,op_end_date  as end_date from synpuf_110k_results.tg0hbjfsincluded_events
), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.tg0hbjfsincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.tg0hbjfsfinal_cohort
  AS WITH cteenddates   as (select person_id
		 as person_id,DATE_ADD(cast(event_date as date), interval -1 * 0 DAY)   as end_date from (
		select
			person_id
			, event_date
			, event_type
			, max(start_ordinal) over (partition by person_id order by event_date, event_type rows unbounded preceding) as start_ordinal 
			, row_number() over (partition by person_id order by event_date, event_type) as overall_ord
		from
		(
			select
				person_id
				, start_date as event_date
				, -1 as event_type
				, row_number() over (partition by person_id order by start_date) as start_ordinal
			from synpuf_110k_results.tg0hbjfscohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.tg0hbjfscohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.tg0hbjfscohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_qfsybqv7 where cohort_definition_id = 4;
insert into synpuf_110k_results.temp_cohort_qfsybqv7 (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 4 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.tg0hbjfsfinal_cohort co
;





DELETE FROM synpuf_110k_results.tg0hbjfscohort_rows WHERE True;
drop table synpuf_110k_results.tg0hbjfscohort_rows;

DELETE FROM synpuf_110k_results.tg0hbjfsfinal_cohort WHERE True;
drop table synpuf_110k_results.tg0hbjfsfinal_cohort;

DELETE FROM synpuf_110k_results.tg0hbjfsinclusion_events WHERE True;
drop table synpuf_110k_results.tg0hbjfsinclusion_events;

DELETE FROM synpuf_110k_results.tg0hbjfsqualified_events WHERE True;
drop table synpuf_110k_results.tg0hbjfsqualified_events;

DELETE FROM synpuf_110k_results.tg0hbjfsincluded_events WHERE True;
drop table synpuf_110k_results.tg0hbjfsincluded_events;

DELETE FROM synpuf_110k_results.tg0hbjfscodesets WHERE True;
drop table synpuf_110k_results.tg0hbjfscodesets;


///////////////////////////////////////////////////////////////

DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7cov_ref;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7analysis_ref;
create table synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id INT64,
	covariate_name STRING,
	analysis_id INT64,
	analysis_name STRING,
	concept_id INT64
	);
create table synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id INT64,
	analysis_name STRING,
	domain_id STRING,
	
	start_day INT64,
	end_day INT64,

	is_binary STRING,
	missing_means_zero STRING
	);
 CREATE TABLE synpuf_110k_results.qfsybqv7cov_1
  AS
SELECT
cast(gender_concept_id  as int64) * 1000 + 1 as covariate_id,
		

	count(*) as sum_value


FROM
synpuf_110k_results.temp_cohort_qfsybqv7 cohort
inner join synpuf_110k.person
	on cohort.subject_id = person.person_id
where gender_concept_id != 0

	
	
		and cohort.cohort_definition_id = 3
		
 group by  1 ;
insert into synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
select covariate_id,
	cast(concat('gender = ', case when concept_name is null then 'Unknown concept' else concept_name end) as STRING) as covariate_name,
	1 as analysis_id,
	 cast((covariate_id - 1) / 1000  as int64) as concept_id
from (
	select distinct covariate_id
	from synpuf_110k_results.qfsybqv7cov_1
	) t1
left join synpuf_110k.concept
	on concept_id = cast((covariate_id - 1) / 1000  as int64);
insert into synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
select 1 as analysis_id,
	cast('DemographicsGender' as STRING) as analysis_name,
	cast('Demographics' as STRING) as domain_id,

	cast(null  as int64) as start_day,
	cast(null  as int64) as end_day,

	cast('Y' as STRING) as is_binary,
	cast(null as STRING) as missing_means_zero;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_concepts;
create table synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id INT64,
	concept_id INT64
	);
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_scoring;
create table synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id INT64,
	diag_category_name STRING,
	weight INT64
	);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	1,
	'Myocardial infarction',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 1,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4329847);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	2,
	'Congestive heart failure',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 2,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (316139);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	3,
	'Peripheral vascular disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 3,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (321052);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	4,
	'Cerebrovascular disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 4,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (381591, 434056);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	5,
	'Dementia',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 5,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4182210);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	6,
	'Chronic pulmonary disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 6,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4063381);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	7,
	'Rheumatologic disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 7,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (257628, 134442, 80800, 80809, 256197, 255348);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	8,
	'Peptic ulcer disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 8,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4247120);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	9,
	'Mild liver disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 9,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4064161, 4212540);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	10,
	'Diabetes (mild to moderate)',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 10,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (201820);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	11,
	'Diabetes with chronic complications',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 11,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4192279, 443767, 442793);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	12,
	'Hemoplegia or paralegia',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 12,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (192606, 374022);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	13,
	'Renal disease',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 13,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4030518);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	14,
	'Any malignancy',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 14,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (443392);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	15,
	'Moderate to severe liver disease',
	3
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 15,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4245975, 4029488, 192680, 24966);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	16,
	'Metastatic solid tumor',
	6
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 16,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (432851);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	17,
	'AIDS',
	6
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 17,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (439727);
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_data;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_stats;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_prep;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_prep2;
 CREATE TABLE synpuf_110k_results.qfsybqv7charlson_data

  AS
SELECT
subject_id,
	cohort_start_date,
	sum(weight) as score

FROM
(
	select distinct charlson_scoring.diag_category_id,
		charlson_scoring.weight,

		cohort.subject_id,
		cohort.cohort_start_date
			
	from synpuf_110k_results.temp_cohort_qfsybqv7 cohort
	inner join synpuf_110k.condition_era condition_era
		on cohort.subject_id = condition_era.person_id
	inner join synpuf_110k_results.qfsybqv7charlson_concepts charlson_concepts
		on condition_era.condition_concept_id = charlson_concepts.concept_id
	inner join synpuf_110k_results.qfsybqv7charlson_scoring charlson_scoring
		on charlson_concepts.diag_category_id = charlson_scoring.diag_category_id

	where condition_era_start_date <= DATE_ADD(cast(cohort.cohort_start_date as date), interval 0 DAY)

		and cohort.cohort_definition_id = 3
	) temp

 group by  1, 2 ;
CREATE TABLE synpuf_110k_results.qfsybqv7charlson_stats
 AS WITH t1 as (
	select count(*) as cnt 
	from synpuf_110k_results.temp_cohort_qfsybqv7 
	where cohort_definition_id = 3
	),
t2 as (
	select count(*) as cnt, 
		min(score) as min_score, 
		max(score) as max_score, 
		sum(score) as sum_score,
		sum(score * score) as squared_score
	from synpuf_110k_results.qfsybqv7charlson_data
	)
 SELECT case when t2.cnt = t1.cnt then t2.min_score else 0 end as min_value,
	t2.max_score as max_value,
	cast(t2.sum_score / (1.0 * t1.cnt)  as float64) as average_value,
	cast(case when t2.cnt = 1 then 0 else sqrt((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) end  as float64) as standard_deviation,
	t2.cnt as count_value,
	t1.cnt - t2.cnt as count_no_value,
	t1.cnt as population_size
 FROM t1, t2;
 CREATE TABLE synpuf_110k_results.qfsybqv7charlson_prep
  AS
SELECT
score,
	count(*) as total,
	row_number() over (order by score) as rn

FROM
synpuf_110k_results.qfsybqv7charlson_data
 group by  1 ;
 CREATE TABLE synpuf_110k_results.qfsybqv7charlson_prep2	
  AS
SELECT
s.score,
	sum(p.total) as accumulated

FROM
synpuf_110k_results.qfsybqv7charlson_prep s
inner join synpuf_110k_results.qfsybqv7charlson_prep p
	on p.rn <= s.rn
 group by  s.score ;
 CREATE TABLE synpuf_110k_results.qfsybqv7cov_2
  AS
SELECT
cast(1000 + 901  as int64) as covariate_id,

	o.count_value,
	o.min_value,
	o.max_value,
	cast(o.average_value  as float64) average_value,
	cast(o.standard_deviation  as float64) standard_deviation,
	case 
		when .50 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .50 * o.population_size then score	end) 
		end as median_value,
	case 
		when .10 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .10 * o.population_size then score	end) 
		end as p10_value,		
	case 
		when .25 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .25 * o.population_size then score	end) 
		end as p25_value,	
	case 
		when .75 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .75 * o.population_size then score	end) 
		end as p75_value,	
	case 
		when .90 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .90 * o.population_size then score	end) 
		end as p90_value		

FROM
synpuf_110k_results.qfsybqv7charlson_prep2 p
cross join synpuf_110k_results.qfsybqv7charlson_stats o

 group by  o.count_value, o.count_no_value, o.min_value, o.max_value, o.average_value, o.standard_deviation, o.population_size ;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_data WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_data;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_stats WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_stats;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_prep WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_prep;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_prep2 WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_prep2;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_concepts WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_concepts;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_scoring WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_scoring;
insert into synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
select covariate_id,
	cast('Charlson index - Romano adaptation' as STRING) as covariate_name,
	901 as analysis_id,
	0 as concept_id
from (
	select distinct covariate_id
	from synpuf_110k_results.qfsybqv7cov_2
	) t1;
insert into synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
select 901 as analysis_id,
	cast('CharlsonIndex' as STRING) as analysis_name,
	cast('Condition' as STRING) as domain_id,

	cast(null  as int64) as start_day,
	0 as end_day,

	cast('N' as STRING) as is_binary,
	cast('Y' as STRING) as missing_means_zero;
 CREATE TABLE synpuf_110k_results.qfsybqv7cov_3
  AS
SELECT
cast(floor((EXTRACT(YEAR from cohort_start_date) - year_of_birth) / 5) * 1000 + 3  as int64) as covariate_id,
	

	count(*) as sum_value


FROM
synpuf_110k_results.temp_cohort_qfsybqv7 cohort
inner join synpuf_110k.person
	on cohort.subject_id = person.person_id


	where cohort.cohort_definition_id = 3

		
 group by  1 ;
insert into synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
select covariate_id,
	cast(concat (
		'age group: ',
		SUBSTR(concat('00', cast(5 * (covariate_id - 3) / 1000 as STRING)),-2),
		'-',
		SUBSTR(concat('00', cast((5 * (covariate_id - 3) / 1000) + 4 as STRING)),-2)
		) as STRING) as covariate_name,
	3 as analysis_id,
	0 as concept_id
from (
	select distinct covariate_id
	from synpuf_110k_results.qfsybqv7cov_3
	) t1;
insert into synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
select 3 as analysis_id,
	cast('DemographicsAgeGroup' as STRING) as analysis_name,
	cast('Demographics' as STRING) as domain_id,

	cast(null  as int64) as start_day,
	cast(null  as int64) as end_day,

	cast('Y' as STRING) as is_binary,
	cast(null as STRING) as missing_means_zero;
insert into synpuf_110k_results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id,
    count_value, min_value, max_value, avg_value, stdev_value, median_value,
    p10_value, p25_value, p75_value, p90_value, strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select cast('DISTRIBUTION' as STRING) as type,
    cast('PRESET' as STRING) as fa_type,
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
    cast('' as STRING) as strata_name,
    3 as cohort_definition_id,
    20 as cc_generation_id
  from (select 3 as cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from (select covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value
from (
select covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from synpuf_110k_results.qfsybqv7cov_2
) all_covariates) w) f
    join (select 3 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (select covariate_id, covariate_name, analysis_id, concept_id  from synpuf_110k_results.qfsybqv7cov_ref) w) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 3 as cohort_definition_id, cast(analysis_id  as int64) analysis_id, analysis_name, domain_id, start_day, end_day, cast(is_binary as STRING) is_binary,cast(missing_means_zero as STRING) missing_means_zero from (select analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero from synpuf_110k_results.qfsybqv7analysis_ref) w) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join synpuf_110k.concept c on c.concept_id = fr.concept_id;
insert into synpuf_110k_results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id, count_value, avg_value,
                                                 strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select cast('PREVALENCE' as STRING) as type,
    cast('PRESET' as STRING) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.sum_value     as count_value,
    f.average_value as stat_value,
    0 as strata_id,
    cast('' as STRING) as strata_name,
    3 as cohort_definition_id,
    20 as cc_generation_id
  from (select 3 as cohort_definition_id, covariate_id, sum_value, average_value from (select all_covariates.covariate_id,
  all_covariates.sum_value,
  cast(all_covariates.sum_value / (1.0 * total.total_count)  as float64) as average_value
from (select covariate_id, sum_value from synpuf_110k_results.qfsybqv7cov_1 union all
select covariate_id, sum_value from synpuf_110k_results.qfsybqv7cov_3
) all_covariates, (
select count(*) as total_count
from synpuf_110k_results.temp_cohort_qfsybqv7 
where cohort_definition_id = 3
) total) w) f
    join (select 3 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (select covariate_id, covariate_name, analysis_id, concept_id  from synpuf_110k_results.qfsybqv7cov_ref) w) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 3 as cohort_definition_id, cast(analysis_id  as int64) analysis_id, analysis_name, domain_id, start_day, end_day, cast(is_binary as STRING) is_binary,cast(missing_means_zero as STRING) missing_means_zero from (select analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero from synpuf_110k_results.qfsybqv7analysis_ref) w) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join synpuf_110k.concept c on c.concept_id = fr.concept_id;
DELETE FROM synpuf_110k_results.qfsybqv7cov_1 WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_1;
DELETE FROM synpuf_110k_results.qfsybqv7cov_2 WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_2;
DELETE FROM synpuf_110k_results.qfsybqv7cov_3 WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_3;
DELETE FROM synpuf_110k_results.qfsybqv7cov_ref WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_ref;
DELETE FROM synpuf_110k_results.qfsybqv7analysis_ref WHERE True;
drop table synpuf_110k_results.qfsybqv7analysis_ref;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7cov_ref;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7analysis_ref;
create table synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id INT64,
	covariate_name STRING,
	analysis_id INT64,
	analysis_name STRING,
	concept_id INT64
	);
create table synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id INT64,
	analysis_name STRING,
	domain_id STRING,
	
	start_day INT64,
	end_day INT64,

	is_binary STRING,
	missing_means_zero STRING
	);
 CREATE TABLE synpuf_110k_results.qfsybqv7cov_1
  AS
SELECT
cast(gender_concept_id  as int64) * 1000 + 1 as covariate_id,
		

	count(*) as sum_value


FROM
synpuf_110k_results.temp_cohort_qfsybqv7 cohort
inner join synpuf_110k.person
	on cohort.subject_id = person.person_id
where gender_concept_id != 0

	
	
		and cohort.cohort_definition_id = 4
		
 group by  1 ;
insert into synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
select covariate_id,
	cast(concat('gender = ', case when concept_name is null then 'Unknown concept' else concept_name end) as STRING) as covariate_name,
	1 as analysis_id,
	 cast((covariate_id - 1) / 1000  as int64) as concept_id
from (
	select distinct covariate_id
	from synpuf_110k_results.qfsybqv7cov_1
	) t1
left join synpuf_110k.concept
	on concept_id = cast((covariate_id - 1) / 1000  as int64);
insert into synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
select 1 as analysis_id,
	cast('DemographicsGender' as STRING) as analysis_name,
	cast('Demographics' as STRING) as domain_id,

	cast(null  as int64) as start_day,
	cast(null  as int64) as end_day,

	cast('Y' as STRING) as is_binary,
	cast(null as STRING) as missing_means_zero;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_concepts;
create table synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id INT64,
	concept_id INT64
	);
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_scoring;
create table synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id INT64,
	diag_category_name STRING,
	weight INT64
	);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	1,
	'Myocardial infarction',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 1,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4329847);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	2,
	'Congestive heart failure',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 2,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (316139);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	3,
	'Peripheral vascular disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 3,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (321052);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	4,
	'Cerebrovascular disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 4,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (381591, 434056);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	5,
	'Dementia',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 5,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4182210);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	6,
	'Chronic pulmonary disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 6,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4063381);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	7,
	'Rheumatologic disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 7,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (257628, 134442, 80800, 80809, 256197, 255348);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	8,
	'Peptic ulcer disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 8,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4247120);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	9,
	'Mild liver disease',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 9,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4064161, 4212540);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	10,
	'Diabetes (mild to moderate)',
	1
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 10,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (201820);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	11,
	'Diabetes with chronic complications',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 11,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4192279, 443767, 442793);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	12,
	'Hemoplegia or paralegia',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 12,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (192606, 374022);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	13,
	'Renal disease',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 13,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4030518);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	14,
	'Any malignancy',
	2
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 14,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (443392);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	15,
	'Moderate to severe liver disease',
	3
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 15,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (4245975, 4029488, 192680, 24966);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	16,
	'Metastatic solid tumor',
	6
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 16,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (432851);
insert into synpuf_110k_results.qfsybqv7charlson_scoring (
	diag_category_id,
	diag_category_name,
	weight
	)
values (
	17,
	'AIDS',
	6
	);
insert into synpuf_110k_results.qfsybqv7charlson_concepts (
	diag_category_id,
	concept_id
	)
select 17,
	descendant_concept_id
from synpuf_110k.concept_ancestor
where ancestor_concept_id in (439727);
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_data;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_stats;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_prep;
DROP TABLE IF EXISTS synpuf_110k_results.qfsybqv7charlson_prep2;
 CREATE TABLE synpuf_110k_results.qfsybqv7charlson_data

  AS
SELECT
subject_id,
	cohort_start_date,
	sum(weight) as score

FROM
(
	select distinct charlson_scoring.diag_category_id,
		charlson_scoring.weight,

		cohort.subject_id,
		cohort.cohort_start_date
			
	from synpuf_110k_results.temp_cohort_qfsybqv7 cohort
	inner join synpuf_110k.condition_era condition_era
		on cohort.subject_id = condition_era.person_id
	inner join synpuf_110k_results.qfsybqv7charlson_concepts charlson_concepts
		on condition_era.condition_concept_id = charlson_concepts.concept_id
	inner join synpuf_110k_results.qfsybqv7charlson_scoring charlson_scoring
		on charlson_concepts.diag_category_id = charlson_scoring.diag_category_id

	where condition_era_start_date <= DATE_ADD(cast(cohort.cohort_start_date as date), interval 0 DAY)

		and cohort.cohort_definition_id = 4
	) temp

 group by  1, 2 ;
CREATE TABLE synpuf_110k_results.qfsybqv7charlson_stats
 AS WITH t1 as (
	select count(*) as cnt 
	from synpuf_110k_results.temp_cohort_qfsybqv7 
	where cohort_definition_id = 4
	),
t2 as (
	select count(*) as cnt, 
		min(score) as min_score, 
		max(score) as max_score, 
		sum(score) as sum_score,
		sum(score * score) as squared_score
	from synpuf_110k_results.qfsybqv7charlson_data
	)
 SELECT case when t2.cnt = t1.cnt then t2.min_score else 0 end as min_value,
	t2.max_score as max_value,
	cast(t2.sum_score / (1.0 * t1.cnt)  as float64) as average_value,
	cast(case when t2.cnt = 1 then 0 else sqrt((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) end  as float64) as standard_deviation,
	t2.cnt as count_value,
	t1.cnt - t2.cnt as count_no_value,
	t1.cnt as population_size
 FROM t1, t2;
 CREATE TABLE synpuf_110k_results.qfsybqv7charlson_prep
  AS
SELECT
score,
	count(*) as total,
	row_number() over (order by score) as rn

FROM
synpuf_110k_results.qfsybqv7charlson_data
 group by  1 ;
 CREATE TABLE synpuf_110k_results.qfsybqv7charlson_prep2	
  AS
SELECT
s.score,
	sum(p.total) as accumulated

FROM
synpuf_110k_results.qfsybqv7charlson_prep s
inner join synpuf_110k_results.qfsybqv7charlson_prep p
	on p.rn <= s.rn
 group by  s.score ;
 CREATE TABLE synpuf_110k_results.qfsybqv7cov_2
  AS
SELECT
cast(1000 + 901  as int64) as covariate_id,

	o.count_value,
	o.min_value,
	o.max_value,
	cast(o.average_value  as float64) average_value,
	cast(o.standard_deviation  as float64) standard_deviation,
	case 
		when .50 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .50 * o.population_size then score	end) 
		end as median_value,
	case 
		when .10 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .10 * o.population_size then score	end) 
		end as p10_value,		
	case 
		when .25 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .25 * o.population_size then score	end) 
		end as p25_value,	
	case 
		when .75 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .75 * o.population_size then score	end) 
		end as p75_value,	
	case 
		when .90 * o.population_size < count_no_value then 0
		else min(case when p.accumulated + count_no_value >= .90 * o.population_size then score	end) 
		end as p90_value		

FROM
synpuf_110k_results.qfsybqv7charlson_prep2 p
cross join synpuf_110k_results.qfsybqv7charlson_stats o

 group by  o.count_value, o.count_no_value, o.min_value, o.max_value, o.average_value, o.standard_deviation, o.population_size ;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_data WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_data;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_stats WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_stats;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_prep WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_prep;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_prep2 WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_prep2;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_concepts WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_concepts;
DELETE FROM synpuf_110k_results.qfsybqv7charlson_scoring WHERE True;
drop table synpuf_110k_results.qfsybqv7charlson_scoring;
insert into synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
select covariate_id,
	cast('Charlson index - Romano adaptation' as STRING) as covariate_name,
	901 as analysis_id,
	0 as concept_id
from (
	select distinct covariate_id
	from synpuf_110k_results.qfsybqv7cov_2
	) t1;
insert into synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
select 901 as analysis_id,
	cast('CharlsonIndex' as STRING) as analysis_name,
	cast('Condition' as STRING) as domain_id,

	cast(null  as int64) as start_day,
	0 as end_day,

	cast('N' as STRING) as is_binary,
	cast('Y' as STRING) as missing_means_zero;
 CREATE TABLE synpuf_110k_results.qfsybqv7cov_3
  AS
SELECT
cast(floor((EXTRACT(YEAR from cohort_start_date) - year_of_birth) / 5) * 1000 + 3  as int64) as covariate_id,
	

	count(*) as sum_value


FROM
synpuf_110k_results.temp_cohort_qfsybqv7 cohort
inner join synpuf_110k.person
	on cohort.subject_id = person.person_id


	where cohort.cohort_definition_id = 4

		
 group by  1 ;
insert into synpuf_110k_results.qfsybqv7cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
select covariate_id,
	cast(concat (
		'age group: ',
		SUBSTR(concat('00', cast(5 * (covariate_id - 3) / 1000 as STRING)),-2),
		'-',
		SUBSTR(concat('00', cast((5 * (covariate_id - 3) / 1000) + 4 as STRING)),-2)
		) as STRING) as covariate_name,
	3 as analysis_id,
	0 as concept_id
from (
	select distinct covariate_id
	from synpuf_110k_results.qfsybqv7cov_3
	) t1;
insert into synpuf_110k_results.qfsybqv7analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,

	start_day,
	end_day,

	is_binary,
	missing_means_zero
	)
select 3 as analysis_id,
	cast('DemographicsAgeGroup' as STRING) as analysis_name,
	cast('Demographics' as STRING) as domain_id,

	cast(null  as int64) as start_day,
	cast(null  as int64) as end_day,

	cast('Y' as STRING) as is_binary,
	cast(null as STRING) as missing_means_zero;
insert into synpuf_110k_results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id,
    count_value, min_value, max_value, avg_value, stdev_value, median_value,
    p10_value, p25_value, p75_value, p90_value, strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select cast('DISTRIBUTION' as STRING) as type,
    cast('PRESET' as STRING) as fa_type,
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
    cast('' as STRING) as strata_name,
    4 as cohort_definition_id,
    20 as cc_generation_id
  from (select 4 as cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from (select covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value
from (
select covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from synpuf_110k_results.qfsybqv7cov_2
) all_covariates) w) f
    join (select 4 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (select covariate_id, covariate_name, analysis_id, concept_id  from synpuf_110k_results.qfsybqv7cov_ref) w) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 4 as cohort_definition_id, cast(analysis_id  as int64) analysis_id, analysis_name, domain_id, start_day, end_day, cast(is_binary as STRING) is_binary,cast(missing_means_zero as STRING) missing_means_zero from (select analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero from synpuf_110k_results.qfsybqv7analysis_ref) w) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join synpuf_110k.concept c on c.concept_id = fr.concept_id;
insert into synpuf_110k_results.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id, count_value, avg_value,
                                                 strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select cast('PREVALENCE' as STRING) as type,
    cast('PRESET' as STRING) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.sum_value     as count_value,
    f.average_value as stat_value,
    0 as strata_id,
    cast('' as STRING) as strata_name,
    4 as cohort_definition_id,
    20 as cc_generation_id
  from (select 4 as cohort_definition_id, covariate_id, sum_value, average_value from (select all_covariates.covariate_id,
  all_covariates.sum_value,
  cast(all_covariates.sum_value / (1.0 * total.total_count)  as float64) as average_value
from (select covariate_id, sum_value from synpuf_110k_results.qfsybqv7cov_1 union all
select covariate_id, sum_value from synpuf_110k_results.qfsybqv7cov_3
) all_covariates, (
select count(*) as total_count
from synpuf_110k_results.temp_cohort_qfsybqv7 
where cohort_definition_id = 4
) total) w) f
    join (select 4 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (select covariate_id, covariate_name, analysis_id, concept_id  from synpuf_110k_results.qfsybqv7cov_ref) w) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 4 as cohort_definition_id, cast(analysis_id  as int64) analysis_id, analysis_name, domain_id, start_day, end_day, cast(is_binary as STRING) is_binary,cast(missing_means_zero as STRING) missing_means_zero from (select analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero from synpuf_110k_results.qfsybqv7analysis_ref) w) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join synpuf_110k.concept c on c.concept_id = fr.concept_id;
DELETE FROM synpuf_110k_results.qfsybqv7cov_1 WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_1;
DELETE FROM synpuf_110k_results.qfsybqv7cov_2 WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_2;
DELETE FROM synpuf_110k_results.qfsybqv7cov_3 WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_3;
DELETE FROM synpuf_110k_results.qfsybqv7cov_ref WHERE True;
drop table synpuf_110k_results.qfsybqv7cov_ref;
DELETE FROM synpuf_110k_results.qfsybqv7analysis_ref WHERE True;
drop table synpuf_110k_results.qfsybqv7analysis_ref;