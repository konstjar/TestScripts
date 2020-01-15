create table synpuf_110k_results.w6ihc0q4codesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.w6ihc0q4codesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1503297,19059796,1525215,1580747,1597756,1560171,1547504,1559684,1502905,1567198)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1503297,19059796,1525215,1580747,1597756,1560171,1547504,1559684,1502905,1567198)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.w6ihc0q4codesets (codeset_id, concept_id)
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (444094,35506621)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (444094,35506621)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.w6ihc0q4codesets (codeset_id, concept_id)
select 2 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (201820)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (201820)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.w6ihc0q4codesets (codeset_id, concept_id)
select 3 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where 0=1
) i
) c;
insert into synpuf_110k_results.w6ihc0q4codesets (codeset_id, concept_id)
select 4 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where 0=1
) i
) c;


CREATE TABLE synpuf_110k_results.w6ihc0q4qualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Condition Era Criteria
select c.person_id, c.condition_era_id as event_id, c.condition_era_start_date as start_date,
       c.condition_era_end_date as end_date, c.condition_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.condition_era_start_date as sort_date
from 
(
  select ce.* 
  from synpuf_110k.condition_era ce
where ce.condition_concept_id in (select concept_id from  synpuf_110k_results.w6ihc0q4codesets where codeset_id = 2)
) c


-- End Condition Era Criteria

union all
-- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.w6ihc0q4codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 3))
) c


-- End Drug Exposure Criteria

  ) e
	join synpuf_110k.observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(cast(op.observation_period_start_date as date), interval 180 DAY) <= e.start_date and DATE_ADD(cast(e.start_date as date), interval 0 DAY) <= op.observation_period_end_date
) p
where p.ordinal = 1
-- End Primary Events

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
 FROM (
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date asc) as ordinal, cast(pe.visit_occurrence_id  as int64) as visit_occurrence_id
  from primary_events pe
  
) qe
where qe.ordinal = 1
;

--- Inclusion Rule Inserts

create table synpuf_110k_results.w6ihc0q4inclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.w6ihc0q4included_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.w6ihc0q4qualified_events q
    left join synpuf_110k_results.w6ihc0q4inclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.w6ihc0q4strategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1095 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1095 DAY) else start_date end as end_date

FROM
synpuf_110k_results.w6ihc0q4included_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.w6ihc0q4cohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.w6ihc0q4strategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.w6ihc0q4included_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.w6ihc0q4final_cohort
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
			from synpuf_110k_results.w6ihc0q4cohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.w6ihc0q4cohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.w6ihc0q4cohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 8;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 8 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.w6ihc0q4final_cohort co
;



DELETE FROM synpuf_110k_results.w6ihc0q4strategy_ends WHERE True;
drop table synpuf_110k_results.w6ihc0q4strategy_ends;


DELETE FROM synpuf_110k_results.w6ihc0q4cohort_rows WHERE True;
drop table synpuf_110k_results.w6ihc0q4cohort_rows;

DELETE FROM synpuf_110k_results.w6ihc0q4final_cohort WHERE True;
drop table synpuf_110k_results.w6ihc0q4final_cohort;

DELETE FROM synpuf_110k_results.w6ihc0q4inclusion_events WHERE True;
drop table synpuf_110k_results.w6ihc0q4inclusion_events;

DELETE FROM synpuf_110k_results.w6ihc0q4qualified_events WHERE True;
drop table synpuf_110k_results.w6ihc0q4qualified_events;

DELETE FROM synpuf_110k_results.w6ihc0q4included_events WHERE True;
drop table synpuf_110k_results.w6ihc0q4included_events;

DELETE FROM synpuf_110k_results.w6ihc0q4codesets WHERE True;
drop table synpuf_110k_results.w6ihc0q4codesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.gh4qx14mcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (40228152)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40228152)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (313217)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (313217)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 2 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (314665)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (314665)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 3 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (40241331)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40241331)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 4 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (43013024)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (43013024)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 5 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (42898160)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (42898160)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 6 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (40483762,4123927,4086294,4137269,4137272,4062333,40481548,4109386,4301458,4301459,2720815,2720814,2720817,2720816,2720812,2720813,2617270,2721445,2721700,2721702,40664432,2721701,2721703,2720811,38003372,2721699,38003368,38003366,38003370,38003369,38003373,38003371,38003367,38003131,38003066,38003036,38003046,38003076,4082084,4140947,38003056,915618,915614,915615,915616,915619,915620,915617,4062044,2514512,4086777)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40483762,4123927,4086294,4137269,4137272,4062333,40481548,4109386,4301458,4301459,2720815,2720814,2720817,2720816,2720812,2720813,2617270,2721445,2721700,2721702,40664432,2721701,2721703,2720811,38003372,2721699,38003368,38003366,38003370,38003369,38003373,38003371,38003367,38003131,38003066,38003036,38003046,38003076,4082084,4140947,38003056,915618,915614,915615,915616,915619,915620,915617,4062044,2514512,4086777)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 7 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (4060089,4195003,44782431,4013355,4165384,2617335,43020459,312773,4020159,44783274,315273,4110937,2001447,2001448,4119522,4145884,2617334,4339971,4121484,4013356,4181749,4304541)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4060089,4195003,44782431,4013355,4165384,2617335,43020459,312773,4020159,44783274,315273,4110937,2001447,2001448,4119522,4145884,2617334,4339971,4121484,4013356,4181749,4304541)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 8 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1310149)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1310149)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 9 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (2101660,2101635,2101634,2104836,2103931,2105103,2104837,2104835,2000075,2000076,2000074,2000073,4001859,4134857,4207955,2005902,4162099,2000085,2000084,2000083,4010119,2000070,2000072,2000069,2000071,2000080,2000081,2000079,2000078,45887894,2104839,2104838,2104840,4266062,2105128,2105129,2000082,2005891,2005904,4203771,2005903)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (2101660,2101635,2101634,2104836,2103931,2105103,2104837,2104835,2000075,2000076,2000074,2000073,4001859,4134857,4207955,2005902,4162099,2000085,2000084,2000083,4010119,2000070,2000072,2000069,2000071,2000080,2000081,2000079,2000078,45887894,2104839,2104838,2104840,4266062,2105128,2105129,2000082,2005891,2005904,4203771,2005903)
  and c.invalid_reason is null

) i
left join
(
  select concept_id from synpuf_110k.concept where concept_id in (2104914)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (2104914)
  and c.invalid_reason is null

) e on i.concept_id = e.concept_id
where e.concept_id is null
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 10 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (4126124,4092504,435649,40480136,4181476,44786469,4120120,2101833,4137616,313232,4300099,4297919,4297658,4099603,44782924,44786470,44786471,43533281,4324124,2003564,4300106,4046829,2109584,4021107,4197300,4324754,4002215,4022805,2003626,40664909,2109586,2109589,4163566,37521745,4346636,4346505,4347789,2721092,4322471,4343000)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4126124,4092504,435649,40480136,4181476,44786469,4120120,2101833,4137616,313232,4300099,4297919,4297658,4099603,44782924,44786470,44786471,43533281,4324124,2003564,4300106,4046829,2109584,4021107,4197300,4324754,4002215,4022805,2003626,40664909,2109586,2109589,4163566,37521745,4346636,4346505,4347789,2721092,4322471,4343000)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 11 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (318775,444247)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (318775,444247)
  and c.invalid_reason is null

) i
left join
(
  select concept_id from synpuf_110k.concept where concept_id in (435887,195562,4179912,318137,199837,438820,4235812,4187790)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (435887,195562,4179912,318137,199837,438820,4235812,4187790)
  and c.invalid_reason is null

) e on i.concept_id = e.concept_id
where e.concept_id is null
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 12 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (40480461,440417,40479606)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40480461,440417,40479606)
  and c.invalid_reason is null

) i
left join
(
  select concept_id from synpuf_110k.concept where concept_id in (435026)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (435026)
  and c.invalid_reason is null

) e on i.concept_id = e.concept_id
where e.concept_id is null
) c;
insert into synpuf_110k_results.gh4qx14mcodesets (codeset_id, concept_id)
select 13 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1112807)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1112807)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.gh4qx14mqualified_events
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
  select de.* , row_number() over (partition by de.person_id order by de.drug_exposure_start_date, de.drug_exposure_id) as ordinal
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.gh4qx14mcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 13))
) c
join synpuf_110k.person p on c.person_id = p.person_id
where c.drug_exposure_start_date >= DATE(2010, 10, 19)
and EXTRACT(YEAR from c.drug_exposure_start_date) - p.year_of_birth >= 65
and c.ordinal = 1
-- End Drug Exposure Criteria

  ) e
	join synpuf_110k.observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(cast(op.observation_period_start_date as date), interval 183 DAY) <= e.start_date and DATE_ADD(cast(e.start_date as date), interval 0 DAY) <= op.observation_period_end_date
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

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_0
 AS
SELECT
0 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
inner join
(
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.gh4qx14mcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) >= 1
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
inner join
(
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.gh4qx14mcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 2))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) >= 1
-- End Correlated Criteria

    ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) > 0
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_1
 AS
SELECT
1 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.gh4qx14mcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 8))
) c


-- End Drug Exposure Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_2
 AS
SELECT
2 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.gh4qx14mcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 3))
) c


-- End Drug Exposure Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.gh4qx14mcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 4))
) c


-- End Drug Exposure Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

    ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 2
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_3
 AS
SELECT
3 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Visit Occurrence Criteria
select c.person_id, c.visit_occurrence_id as event_id, c.visit_start_date as start_date, c.visit_end_date as end_date,
       c.visit_concept_id as target_concept_id, c.visit_occurrence_id,
       c.visit_start_date as sort_date
from 
(
  select vo.* 
  from synpuf_110k.visit_occurrence vo
join synpuf_110k_results.gh4qx14mcodesets codesets on ((vo.visit_concept_id = codesets.concept_id and codesets.codeset_id = 5))
) c


-- End Visit Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval 0 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Procedure Occurrence Criteria
select c.person_id, c.procedure_occurrence_id as event_id, c.procedure_date as start_date, DATE_ADD(cast(c.procedure_date as date), interval 1 DAY) as end_date,
       c.procedure_concept_id as target_concept_id, c.visit_occurrence_id,
       c.procedure_date as sort_date
from 
(
  select po.* 
  from synpuf_110k.procedure_occurrence po
join synpuf_110k_results.gh4qx14mcodesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 6))
) c


-- End Procedure Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 2 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(cast(c.observation_date as date), interval 1 DAY) as end_date,
       c.observation_concept_id as target_concept_id, c.visit_occurrence_id,
       c.observation_date as sort_date
from 
(
  select o.* 
  from synpuf_110k.observation o
join synpuf_110k_results.gh4qx14mcodesets codesets on ((o.observation_concept_id = codesets.concept_id and codesets.codeset_id = 6))
) c


-- End Observation Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

     ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 3
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_4
 AS
SELECT
4 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.gh4qx14mcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 10))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Procedure Occurrence Criteria
select c.person_id, c.procedure_occurrence_id as event_id, c.procedure_date as start_date, DATE_ADD(cast(c.procedure_date as date), interval 1 DAY) as end_date,
       c.procedure_concept_id as target_concept_id, c.visit_occurrence_id,
       c.procedure_date as sort_date
from 
(
  select po.* 
  from synpuf_110k.procedure_occurrence po
join synpuf_110k_results.gh4qx14mcodesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 10))
) c


-- End Procedure Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 2 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(cast(c.observation_date as date), interval 1 DAY) as end_date,
       c.observation_concept_id as target_concept_id, c.visit_occurrence_id,
       c.observation_date as sort_date
from 
(
  select o.* 
  from synpuf_110k.observation o
join synpuf_110k_results.gh4qx14mcodesets codesets on ((o.observation_concept_id = codesets.concept_id and codesets.codeset_id = 10))
) c


-- End Observation Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

     ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 3
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_5
 AS
SELECT
5 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.gh4qx14mcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 7))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Procedure Occurrence Criteria
select c.person_id, c.procedure_occurrence_id as event_id, c.procedure_date as start_date, DATE_ADD(cast(c.procedure_date as date), interval 1 DAY) as end_date,
       c.procedure_concept_id as target_concept_id, c.visit_occurrence_id,
       c.procedure_date as sort_date
from 
(
  select po.* 
  from synpuf_110k.procedure_occurrence po
join synpuf_110k_results.gh4qx14mcodesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 7))
) c


-- End Procedure Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 2 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(cast(c.observation_date as date), interval 1 DAY) as end_date,
       c.observation_concept_id as target_concept_id, c.visit_occurrence_id,
       c.observation_date as sort_date
from 
(
  select o.* 
  from synpuf_110k.observation o
join synpuf_110k_results.gh4qx14mcodesets codesets on ((o.observation_concept_id = codesets.concept_id and codesets.codeset_id = 7))
) c


-- End Observation Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

     ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 3
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_6
 AS
SELECT
6 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.gh4qx14mcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 11))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.gh4qx14mcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 12))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

    ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 2
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_7
 AS
SELECT
7 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.gh4qx14mqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.gh4qx14mqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.gh4qx14mqualified_events p
left join
(
  -- Begin Procedure Occurrence Criteria
select c.person_id, c.procedure_occurrence_id as event_id, c.procedure_date as start_date, DATE_ADD(cast(c.procedure_date as date), interval 1 DAY) as end_date,
       c.procedure_concept_id as target_concept_id, c.visit_occurrence_id,
       c.procedure_date as sort_date
from 
(
  select po.* 
  from synpuf_110k.procedure_occurrence po
join synpuf_110k_results.gh4qx14mcodesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 9))
) c


-- End Procedure Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(cast(p.start_date as date), interval -183 DAY) and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) = 0
-- End Correlated Criteria

   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.gh4qx14minclusion_events
 AS
SELECT
inclusion_rule_id, person_id, event_id

FROM
(select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_0
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_1
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_2
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_3
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_4
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_5
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_6
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.gh4qx14minclusion_7) i;
DELETE FROM synpuf_110k_results.gh4qx14minclusion_0 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_0;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_1 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_1;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_2 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_2;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_3 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_3;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_4 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_4;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_5 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_5;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_6 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_6;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_7 WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_7;


CREATE TABLE synpuf_110k_results.gh4qx14mincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.gh4qx14mqualified_events q
    left join synpuf_110k_results.gh4qx14minclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  where (mg.inclusion_rule_mask = power(cast(2  as int64),8)-1)

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- custom era strategy

CREATE TABLE synpuf_110k_results.gh4qx14mdrugtarget
 AS WITH ctepersons  as (select distinct person_id  as person_id from synpuf_110k_results.gh4qx14mincluded_events
)

 SELECT person_id, drug_exposure_start_date, drug_exposure_end_date
 FROM (
	select de.person_id, drug_exposure_start_date,  coalesce(drug_exposure_end_date, DATE_ADD(cast(drug_exposure_start_date as date), interval days_supply DAY), DATE_ADD(cast(drug_exposure_start_date as date), interval 1 DAY)) as drug_exposure_end_date 
	from synpuf_110k.drug_exposure de
	join ctepersons p on de.person_id = p.person_id
	join synpuf_110k_results.gh4qx14mcodesets cs on cs.codeset_id = 8 and de.drug_concept_id = cs.concept_id

	union all

	select de.person_id, drug_exposure_start_date,  coalesce(drug_exposure_end_date, DATE_ADD(cast(drug_exposure_start_date as date), interval days_supply DAY), DATE_ADD(cast(drug_exposure_start_date as date), interval 1 DAY)) as drug_exposure_end_date 
	from synpuf_110k.drug_exposure de
	join ctepersons p on de.person_id = p.person_id
	join synpuf_110k_results.gh4qx14mcodesets cs on cs.codeset_id = 8 and de.drug_source_concept_id = cs.concept_id
) e
;

CREATE TABLE synpuf_110k_results.gh4qx14mstrategy_ends
 AS
SELECT
et.event_id, et.person_id, eras.era_end_date as end_date

FROM
synpuf_110k_results.gh4qx14mincluded_events et
join 
(
   select ends.person_id, min(drug_exposure_start_date) as era_start_date, DATE_ADD(cast(ends.era_end_date as date), interval 0 DAY) as era_end_date
   from (
     select de.person_id, de.drug_exposure_start_date, min(e.end_date) as era_end_date
     from synpuf_110k_results.gh4qx14mdrugtarget de
    join 
    (
      --cteEndDates
      select person_id, DATE_ADD(cast(event_date as date), interval -1 * 3 DAY) as end_date -- unpad the end date by 3
      from
      (
				select person_id, event_date, event_type, 
				max(start_ordinal) over (partition by person_id order by event_date, event_type rows unbounded preceding) as start_ordinal,
				row_number() over (partition by person_id order by event_date, event_type) as overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
				from
				(
					-- select the start dates, assigning a row number to each
					select person_id, drug_exposure_start_date as event_date, 0 as event_type, row_number() over (partition by person_id order by drug_exposure_start_date) as start_ordinal
					from synpuf_110k_results.gh4qx14mdrugtarget d

					union all

					-- add the end dates with NULL as the row number, padding the end dates by 3 to allow a grace period for overlapping ranges.
					select person_id, DATE_ADD(cast(drug_exposure_end_date as date), interval 3 DAY), 1 as event_type, null
					from synpuf_110k_results.gh4qx14mdrugtarget d
				) rawdata
      ) e
      where 2 * e.start_ordinal - e.overall_ord = 0
    ) e on de.person_id = e.person_id and e.end_date >= de.drug_exposure_start_date
     group by  de.person_id, de.drug_exposure_start_date
   ) ends
   group by  ends.person_id, ends.era_end_date
 ) eras on eras.person_id = et.person_id 
where et.start_date between eras.era_start_date and eras.era_end_date;

DELETE FROM synpuf_110k_results.gh4qx14mdrugtarget WHERE True;
drop table synpuf_110k_results.gh4qx14mdrugtarget;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.gh4qx14mcohort_rows
 AS WITH cohort_ends   as ( select event_id as event_id,person_id as person_id,op_end_date  as end_date  from synpuf_110k_results.gh4qx14mincluded_events
union all
-- End Date Strategy
 select event_id, person_id, end_date  from synpuf_110k_results.gh4qx14mstrategy_ends

union all
-- Censor Events
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.gh4qx14mincluded_events i
join
(
-- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.gh4qx14mcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 8))
) c


-- End Drug Exposure Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
   group by  i.event_id, i.person_id

union all
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.gh4qx14mincluded_events i
join
(
-- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.gh4qx14mcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 4))
) c


-- End Drug Exposure Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
 group by  i.event_id, i.person_id

union all
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.gh4qx14mincluded_events i
join
(
-- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.gh4qx14mcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 3))
) c


-- End Drug Exposure Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
 group by  i.event_id, i.person_id

union all
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.gh4qx14mincluded_events i
join
(
-- Begin Procedure Occurrence Criteria
select c.person_id, c.procedure_occurrence_id as event_id, c.procedure_date as start_date, DATE_ADD(cast(c.procedure_date as date), interval 1 DAY) as end_date,
       c.procedure_concept_id as target_concept_id, c.visit_occurrence_id,
       c.procedure_date as sort_date
from 
(
  select po.* 
  from synpuf_110k.procedure_occurrence po
join synpuf_110k_results.gh4qx14mcodesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 10))
) c


-- End Procedure Occurrence Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
 group by  i.event_id, i.person_id

union all
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.gh4qx14mincluded_events i
join
(
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.gh4qx14mcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 10))
) c


-- End Condition Occurrence Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
 group by  i.event_id, i.person_id

union all
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.gh4qx14mincluded_events i
join
(
-- Begin Visit Occurrence Criteria
select c.person_id, c.visit_occurrence_id as event_id, c.visit_start_date as start_date, c.visit_end_date as end_date,
       c.visit_concept_id as target_concept_id, c.visit_occurrence_id,
       c.visit_start_date as sort_date
from 
(
  select vo.* 
  from synpuf_110k.visit_occurrence vo
join synpuf_110k_results.gh4qx14mcodesets codesets on ((vo.visit_concept_id = codesets.concept_id and codesets.codeset_id = 5))
) c


-- End Visit Occurrence Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
 group by  i.event_id, i.person_id

union all
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.gh4qx14mincluded_events i
join
(
-- Begin Procedure Occurrence Criteria
select c.person_id, c.procedure_occurrence_id as event_id, c.procedure_date as start_date, DATE_ADD(cast(c.procedure_date as date), interval 1 DAY) as end_date,
       c.procedure_concept_id as target_concept_id, c.visit_occurrence_id,
       c.procedure_date as sort_date
from 
(
  select po.* 
  from synpuf_110k.procedure_occurrence po
join synpuf_110k_results.gh4qx14mcodesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 6))
) c


-- End Procedure Occurrence Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
 group by  i.event_id, i.person_id


         ), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.gh4qx14mincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.gh4qx14mfinal_cohort
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
			from synpuf_110k_results.gh4qx14mcohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.gh4qx14mcohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.gh4qx14mcohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 11;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 11 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.gh4qx14mfinal_cohort co
;



DELETE FROM synpuf_110k_results.gh4qx14mstrategy_ends WHERE True;
drop table synpuf_110k_results.gh4qx14mstrategy_ends;


DELETE FROM synpuf_110k_results.gh4qx14mcohort_rows WHERE True;
drop table synpuf_110k_results.gh4qx14mcohort_rows;

DELETE FROM synpuf_110k_results.gh4qx14mfinal_cohort WHERE True;
drop table synpuf_110k_results.gh4qx14mfinal_cohort;

DELETE FROM synpuf_110k_results.gh4qx14minclusion_events WHERE True;
drop table synpuf_110k_results.gh4qx14minclusion_events;

DELETE FROM synpuf_110k_results.gh4qx14mqualified_events WHERE True;
drop table synpuf_110k_results.gh4qx14mqualified_events;

DELETE FROM synpuf_110k_results.gh4qx14mincluded_events WHERE True;
drop table synpuf_110k_results.gh4qx14mincluded_events;

DELETE FROM synpuf_110k_results.gh4qx14mcodesets WHERE True;
drop table synpuf_110k_results.gh4qx14mcodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.uz4lxlktcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.uz4lxlktcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1502905)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1502905)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.uz4lxlktqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.uz4lxlktcodesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.uz4lxlktinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.uz4lxlktincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.uz4lxlktqualified_events q
    left join synpuf_110k_results.uz4lxlktinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results

;

-- date offset strategy

CREATE TABLE synpuf_110k_results.uz4lxlktstrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.uz4lxlktincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.uz4lxlktcohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.uz4lxlktstrategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.uz4lxlktincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.uz4lxlktfinal_cohort
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
			from synpuf_110k_results.uz4lxlktcohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.uz4lxlktcohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.uz4lxlktcohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 12;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 12 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.uz4lxlktfinal_cohort co
;



DELETE FROM synpuf_110k_results.uz4lxlktstrategy_ends WHERE True;
drop table synpuf_110k_results.uz4lxlktstrategy_ends;


DELETE FROM synpuf_110k_results.uz4lxlktcohort_rows WHERE True;
drop table synpuf_110k_results.uz4lxlktcohort_rows;

DELETE FROM synpuf_110k_results.uz4lxlktfinal_cohort WHERE True;
drop table synpuf_110k_results.uz4lxlktfinal_cohort;

DELETE FROM synpuf_110k_results.uz4lxlktinclusion_events WHERE True;
drop table synpuf_110k_results.uz4lxlktinclusion_events;

DELETE FROM synpuf_110k_results.uz4lxlktqualified_events WHERE True;
drop table synpuf_110k_results.uz4lxlktqualified_events;

DELETE FROM synpuf_110k_results.uz4lxlktincluded_events WHERE True;
drop table synpuf_110k_results.uz4lxlktincluded_events;

DELETE FROM synpuf_110k_results.uz4lxlktcodesets WHERE True;
drop table synpuf_110k_results.uz4lxlktcodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.xffx8d8ucodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.xffx8d8ucodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1560171)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1560171)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.xffx8d8uqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.xffx8d8ucodesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.xffx8d8uinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.xffx8d8uincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.xffx8d8uqualified_events q
    left join synpuf_110k_results.xffx8d8uinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.xffx8d8ustrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 548 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 548 DAY) else start_date end as end_date

FROM
synpuf_110k_results.xffx8d8uincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.xffx8d8ucohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.xffx8d8ustrategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.xffx8d8uincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.xffx8d8ufinal_cohort
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
			from synpuf_110k_results.xffx8d8ucohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.xffx8d8ucohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.xffx8d8ucohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 10;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 10 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.xffx8d8ufinal_cohort co
;



DELETE FROM synpuf_110k_results.xffx8d8ustrategy_ends WHERE True;
drop table synpuf_110k_results.xffx8d8ustrategy_ends;


DELETE FROM synpuf_110k_results.xffx8d8ucohort_rows WHERE True;
drop table synpuf_110k_results.xffx8d8ucohort_rows;

DELETE FROM synpuf_110k_results.xffx8d8ufinal_cohort WHERE True;
drop table synpuf_110k_results.xffx8d8ufinal_cohort;

DELETE FROM synpuf_110k_results.xffx8d8uinclusion_events WHERE True;
drop table synpuf_110k_results.xffx8d8uinclusion_events;

DELETE FROM synpuf_110k_results.xffx8d8uqualified_events WHERE True;
drop table synpuf_110k_results.xffx8d8uqualified_events;

DELETE FROM synpuf_110k_results.xffx8d8uincluded_events WHERE True;
drop table synpuf_110k_results.xffx8d8uincluded_events;

DELETE FROM synpuf_110k_results.xffx8d8ucodesets WHERE True;
drop table synpuf_110k_results.xffx8d8ucodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.jxtdew7wcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.jxtdew7wcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1525215)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1525215)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.jxtdew7wqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.jxtdew7wcodesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.jxtdew7winclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.jxtdew7wincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.jxtdew7wqualified_events q
    left join synpuf_110k_results.jxtdew7winclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results

;

-- date offset strategy

CREATE TABLE synpuf_110k_results.jxtdew7wstrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.jxtdew7wincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.jxtdew7wcohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.jxtdew7wstrategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.jxtdew7wincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.jxtdew7wfinal_cohort
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
			from synpuf_110k_results.jxtdew7wcohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.jxtdew7wcohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.jxtdew7wcohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 9;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 9 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.jxtdew7wfinal_cohort co
;



DELETE FROM synpuf_110k_results.jxtdew7wstrategy_ends WHERE True;
drop table synpuf_110k_results.jxtdew7wstrategy_ends;


DELETE FROM synpuf_110k_results.jxtdew7wcohort_rows WHERE True;
drop table synpuf_110k_results.jxtdew7wcohort_rows;

DELETE FROM synpuf_110k_results.jxtdew7wfinal_cohort WHERE True;
drop table synpuf_110k_results.jxtdew7wfinal_cohort;

DELETE FROM synpuf_110k_results.jxtdew7winclusion_events WHERE True;
drop table synpuf_110k_results.jxtdew7winclusion_events;

DELETE FROM synpuf_110k_results.jxtdew7wqualified_events WHERE True;
drop table synpuf_110k_results.jxtdew7wqualified_events;

DELETE FROM synpuf_110k_results.jxtdew7wincluded_events WHERE True;
drop table synpuf_110k_results.jxtdew7wincluded_events;

DELETE FROM synpuf_110k_results.jxtdew7wcodesets WHERE True;
drop table synpuf_110k_results.jxtdew7wcodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.ovjnhih3codesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.ovjnhih3codesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1567198)

) i
) c;


CREATE TABLE synpuf_110k_results.ovjnhih3qualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.ovjnhih3codesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.ovjnhih3inclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.ovjnhih3included_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.ovjnhih3qualified_events q
    left join synpuf_110k_results.ovjnhih3inclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.ovjnhih3strategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.ovjnhih3included_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.ovjnhih3cohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.ovjnhih3strategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.ovjnhih3included_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.ovjnhih3final_cohort
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
			from synpuf_110k_results.ovjnhih3cohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.ovjnhih3cohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.ovjnhih3cohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 13;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 13 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.ovjnhih3final_cohort co
;



DELETE FROM synpuf_110k_results.ovjnhih3strategy_ends WHERE True;
drop table synpuf_110k_results.ovjnhih3strategy_ends;


DELETE FROM synpuf_110k_results.ovjnhih3cohort_rows WHERE True;
drop table synpuf_110k_results.ovjnhih3cohort_rows;

DELETE FROM synpuf_110k_results.ovjnhih3final_cohort WHERE True;
drop table synpuf_110k_results.ovjnhih3final_cohort;

DELETE FROM synpuf_110k_results.ovjnhih3inclusion_events WHERE True;
drop table synpuf_110k_results.ovjnhih3inclusion_events;

DELETE FROM synpuf_110k_results.ovjnhih3qualified_events WHERE True;
drop table synpuf_110k_results.ovjnhih3qualified_events;

DELETE FROM synpuf_110k_results.ovjnhih3included_events WHERE True;
drop table synpuf_110k_results.ovjnhih3included_events;

DELETE FROM synpuf_110k_results.ovjnhih3codesets WHERE True;
drop table synpuf_110k_results.ovjnhih3codesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.hnibghk9codesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.hnibghk9codesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1580747)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1580747)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.hnibghk9qualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.hnibghk9codesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.hnibghk9inclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.hnibghk9included_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.hnibghk9qualified_events q
    left join synpuf_110k_results.hnibghk9inclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.hnibghk9strategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.hnibghk9included_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.hnibghk9cohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.hnibghk9strategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.hnibghk9included_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.hnibghk9final_cohort
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
			from synpuf_110k_results.hnibghk9cohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.hnibghk9cohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.hnibghk9cohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 19;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 19 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.hnibghk9final_cohort co
;



DELETE FROM synpuf_110k_results.hnibghk9strategy_ends WHERE True;
drop table synpuf_110k_results.hnibghk9strategy_ends;


DELETE FROM synpuf_110k_results.hnibghk9cohort_rows WHERE True;
drop table synpuf_110k_results.hnibghk9cohort_rows;

DELETE FROM synpuf_110k_results.hnibghk9final_cohort WHERE True;
drop table synpuf_110k_results.hnibghk9final_cohort;

DELETE FROM synpuf_110k_results.hnibghk9inclusion_events WHERE True;
drop table synpuf_110k_results.hnibghk9inclusion_events;

DELETE FROM synpuf_110k_results.hnibghk9qualified_events WHERE True;
drop table synpuf_110k_results.hnibghk9qualified_events;

DELETE FROM synpuf_110k_results.hnibghk9included_events WHERE True;
drop table synpuf_110k_results.hnibghk9included_events;

DELETE FROM synpuf_110k_results.hnibghk9codesets WHERE True;
drop table synpuf_110k_results.hnibghk9codesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.wqgmeflccodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.wqgmeflccodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1547504)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1547504)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.wqgmeflcqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.wqgmeflccodesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.wqgmeflcinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.wqgmeflcincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.wqgmeflcqualified_events q
    left join synpuf_110k_results.wqgmeflcinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.wqgmeflcstrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.wqgmeflcincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.wqgmeflccohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.wqgmeflcstrategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.wqgmeflcincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.wqgmeflcfinal_cohort
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
			from synpuf_110k_results.wqgmeflccohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.wqgmeflccohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.wqgmeflccohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 18;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 18 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.wqgmeflcfinal_cohort co
;



DELETE FROM synpuf_110k_results.wqgmeflcstrategy_ends WHERE True;
drop table synpuf_110k_results.wqgmeflcstrategy_ends;


DELETE FROM synpuf_110k_results.wqgmeflccohort_rows WHERE True;
drop table synpuf_110k_results.wqgmeflccohort_rows;

DELETE FROM synpuf_110k_results.wqgmeflcfinal_cohort WHERE True;
drop table synpuf_110k_results.wqgmeflcfinal_cohort;

DELETE FROM synpuf_110k_results.wqgmeflcinclusion_events WHERE True;
drop table synpuf_110k_results.wqgmeflcinclusion_events;

DELETE FROM synpuf_110k_results.wqgmeflcqualified_events WHERE True;
drop table synpuf_110k_results.wqgmeflcqualified_events;

DELETE FROM synpuf_110k_results.wqgmeflcincluded_events WHERE True;
drop table synpuf_110k_results.wqgmeflcincluded_events;

DELETE FROM synpuf_110k_results.wqgmeflccodesets WHERE True;
drop table synpuf_110k_results.wqgmeflccodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.eyqs06vhcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.eyqs06vhcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (19059796)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (19059796)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.eyqs06vhqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.eyqs06vhcodesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.eyqs06vhinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.eyqs06vhincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.eyqs06vhqualified_events q
    left join synpuf_110k_results.eyqs06vhinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.eyqs06vhstrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.eyqs06vhincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.eyqs06vhcohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.eyqs06vhstrategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.eyqs06vhincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.eyqs06vhfinal_cohort
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
			from synpuf_110k_results.eyqs06vhcohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.eyqs06vhcohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.eyqs06vhcohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 17;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 17 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.eyqs06vhfinal_cohort co
;



DELETE FROM synpuf_110k_results.eyqs06vhstrategy_ends WHERE True;
drop table synpuf_110k_results.eyqs06vhstrategy_ends;


DELETE FROM synpuf_110k_results.eyqs06vhcohort_rows WHERE True;
drop table synpuf_110k_results.eyqs06vhcohort_rows;

DELETE FROM synpuf_110k_results.eyqs06vhfinal_cohort WHERE True;
drop table synpuf_110k_results.eyqs06vhfinal_cohort;

DELETE FROM synpuf_110k_results.eyqs06vhinclusion_events WHERE True;
drop table synpuf_110k_results.eyqs06vhinclusion_events;

DELETE FROM synpuf_110k_results.eyqs06vhqualified_events WHERE True;
drop table synpuf_110k_results.eyqs06vhqualified_events;

DELETE FROM synpuf_110k_results.eyqs06vhincluded_events WHERE True;
drop table synpuf_110k_results.eyqs06vhincluded_events;

DELETE FROM synpuf_110k_results.eyqs06vhcodesets WHERE True;
drop table synpuf_110k_results.eyqs06vhcodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.c028h831codesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.c028h831codesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1597756)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1597756)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.c028h831qualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.c028h831codesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.c028h831inclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.c028h831included_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.c028h831qualified_events q
    left join synpuf_110k_results.c028h831inclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.c028h831strategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.c028h831included_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.c028h831cohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.c028h831strategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.c028h831included_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.c028h831final_cohort
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
			from synpuf_110k_results.c028h831cohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.c028h831cohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.c028h831cohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 14;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 14 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.c028h831final_cohort co
;



DELETE FROM synpuf_110k_results.c028h831strategy_ends WHERE True;
drop table synpuf_110k_results.c028h831strategy_ends;


DELETE FROM synpuf_110k_results.c028h831cohort_rows WHERE True;
drop table synpuf_110k_results.c028h831cohort_rows;

DELETE FROM synpuf_110k_results.c028h831final_cohort WHERE True;
drop table synpuf_110k_results.c028h831final_cohort;

DELETE FROM synpuf_110k_results.c028h831inclusion_events WHERE True;
drop table synpuf_110k_results.c028h831inclusion_events;

DELETE FROM synpuf_110k_results.c028h831qualified_events WHERE True;
drop table synpuf_110k_results.c028h831qualified_events;

DELETE FROM synpuf_110k_results.c028h831included_events WHERE True;
drop table synpuf_110k_results.c028h831included_events;

DELETE FROM synpuf_110k_results.c028h831codesets WHERE True;
drop table synpuf_110k_results.c028h831codesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.ypatstwpcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.ypatstwpcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1503297)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1503297)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.ypatstwpqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.ypatstwpcodesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.ypatstwpinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.ypatstwpincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.ypatstwpqualified_events q
    left join synpuf_110k_results.ypatstwpinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.ypatstwpstrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.ypatstwpincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.ypatstwpcohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.ypatstwpstrategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.ypatstwpincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.ypatstwpfinal_cohort
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
			from synpuf_110k_results.ypatstwpcohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.ypatstwpcohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.ypatstwpcohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 16;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 16 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.ypatstwpfinal_cohort co
;



DELETE FROM synpuf_110k_results.ypatstwpstrategy_ends WHERE True;
drop table synpuf_110k_results.ypatstwpstrategy_ends;


DELETE FROM synpuf_110k_results.ypatstwpcohort_rows WHERE True;
drop table synpuf_110k_results.ypatstwpcohort_rows;

DELETE FROM synpuf_110k_results.ypatstwpfinal_cohort WHERE True;
drop table synpuf_110k_results.ypatstwpfinal_cohort;

DELETE FROM synpuf_110k_results.ypatstwpinclusion_events WHERE True;
drop table synpuf_110k_results.ypatstwpinclusion_events;

DELETE FROM synpuf_110k_results.ypatstwpqualified_events WHERE True;
drop table synpuf_110k_results.ypatstwpqualified_events;

DELETE FROM synpuf_110k_results.ypatstwpincluded_events WHERE True;
drop table synpuf_110k_results.ypatstwpincluded_events;

DELETE FROM synpuf_110k_results.ypatstwpcodesets WHERE True;
drop table synpuf_110k_results.ypatstwpcodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.mgxtu72ycodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.mgxtu72ycodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1559684)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1559684)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.mgxtu72yqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Drug Era Criteria
select c.person_id, c.drug_era_id as event_id, c.drug_era_start_date as start_date, c.drug_era_end_date as end_date,
       c.drug_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.drug_era_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.mgxtu72ycodesets where codeset_id = 0)
) c


-- End Drug Era Criteria

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

create table synpuf_110k_results.mgxtu72yinclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);

CREATE TABLE synpuf_110k_results.mgxtu72yincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.mgxtu72yqualified_events q
    left join synpuf_110k_results.mgxtu72yinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.mgxtu72ystrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 1 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 1 DAY) else start_date end as end_date

FROM
synpuf_110k_results.mgxtu72yincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.mgxtu72ycohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,end_date  as end_date from synpuf_110k_results.mgxtu72ystrategy_ends

), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
	 as end_date from (
	  select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
	  from synpuf_110k_results.mgxtu72yincluded_events i
	  join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
	) f
	where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.mgxtu72yfinal_cohort
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
			from synpuf_110k_results.mgxtu72ycohort_rows
		
			union all
		

			select
				person_id
				, DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
				, 1 as event_type
				, null
			from synpuf_110k_results.mgxtu72ycohort_rows
		) rawdata
	) e
	where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
		 as person_id,c.start_date
		 as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.mgxtu72ycohort_rows c
	join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_c4sv5wsi where cohort_definition_id = 15;
insert into synpuf_110k_results.temp_cohort_c4sv5wsi (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 15 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.mgxtu72yfinal_cohort co
;



DELETE FROM synpuf_110k_results.mgxtu72ystrategy_ends WHERE True;
drop table synpuf_110k_results.mgxtu72ystrategy_ends;


DELETE FROM synpuf_110k_results.mgxtu72ycohort_rows WHERE True;
drop table synpuf_110k_results.mgxtu72ycohort_rows;

DELETE FROM synpuf_110k_results.mgxtu72yfinal_cohort WHERE True;
drop table synpuf_110k_results.mgxtu72yfinal_cohort;

DELETE FROM synpuf_110k_results.mgxtu72yinclusion_events WHERE True;
drop table synpuf_110k_results.mgxtu72yinclusion_events;

DELETE FROM synpuf_110k_results.mgxtu72yqualified_events WHERE True;
drop table synpuf_110k_results.mgxtu72yqualified_events;

DELETE FROM synpuf_110k_results.mgxtu72yincluded_events WHERE True;
drop table synpuf_110k_results.mgxtu72yincluded_events;

DELETE FROM synpuf_110k_results.mgxtu72ycodesets WHERE True;
drop table synpuf_110k_results.mgxtu72ycodesets;