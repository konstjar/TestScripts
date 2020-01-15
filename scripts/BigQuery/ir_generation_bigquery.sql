create table synpuf_110k_results.jb67xkbmcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.jb67xkbmcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where 0=1
) i
) c;
insert into synpuf_110k_results.jb67xkbmcodesets (codeset_id, concept_id)
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (316866)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (316866)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.jb67xkbmcodesets (codeset_id, concept_id)
select 2 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (316139)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (316139)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.jb67xkbmcodesets (codeset_id, concept_id)
select 3 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1308842)

) i
) c;
insert into synpuf_110k_results.jb67xkbmcodesets (codeset_id, concept_id)
select 4 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where 0=1
) i
) c;
insert into synpuf_110k_results.jb67xkbmcodesets (codeset_id, concept_id)
select 5 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (192855,2617208,2617223,45473170,4092691,45581152,45561747,45436352,45571462,45755324,40320129,40385855,45566652,4094409,45469941,40385856,4097577,45443002,1571950,44825227,44825228,44835711,44820640,44831012,44833333,44821755,44828756,44834516,44834515,45595727,1567715,44833324,45537806,45581264,35206455,45581267,45576195,1576214,45576194,1576213,45605158,35225348,35225347,35225346,35225345,35225349,45537636,44800307,40519171,45471615,40380245,44813431,45449609,40380244,45513074,44794980,40376617,45493219,45480040,44801444,40385836,45476721,44794562,45503163,40385421,44800478,40385849,45493217,45428190,44798515,45438118,44798509,40521920)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (192855,2617208,2617223,45473170,4092691,45581152,45561747,45436352,45571462,45755324,40320129,40385855,45566652,4094409,45469941,40385856,4097577,45443002,1571950,44825227,44825228,44835711,44820640,44831012,44833333,44821755,44828756,44834516,44834515,45595727,1567715,44833324,45537806,45581264,35206455,45581267,45576195,1576214,45576194,1576213,45605158,35225348,35225347,35225346,35225345,35225349,45537636,44800307,40519171,45471615,40380245,44813431,45449609,40380244,45513074,44794980,40376617,45493219,45480040,44801444,40385836,45476721,44794562,45503163,40385421,44800478,40385849,45493217,45428190,44798515,45438118,44798509,40521920)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.jb67xkbmcodesets (codeset_id, concept_id)
select 6 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (40235485,1351557,1346686,1347384,1367500,40226742,1317640)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40235485,1351557,1346686,1347384,1367500,40226742,1317640)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.jb67xkbmqualified_events
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
  select de.* , row_number() over (partition by de.person_id order by de.drug_era_start_date, de.drug_era_id) as ordinal
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.jb67xkbmcodesets where codeset_id = 3)
) c

where DATE_DIFF(cast(c.drug_era_end_date as date), cast(c.drug_era_start_date as date), DAY) >= 30
and c.ordinal = 1
-- End Drug Era Criteria

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
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from primary_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from primary_events p
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
  join synpuf_110k_results.jb67xkbmcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) >= 1
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from primary_events p
inner join
(
  -- Begin Condition Era Criteria
select c.person_id, c.condition_era_id as event_id, c.condition_era_start_date as start_date,
       c.condition_era_end_date as end_date, c.condition_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.condition_era_start_date as sort_date
from 
(
  select ce.* 
  from synpuf_110k.condition_era ce
where ce.condition_concept_id in (select concept_id from  synpuf_110k_results.jb67xkbmcodesets where codeset_id = 2)
) c


-- End Condition Era Criteria

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

) qe
where qe.ordinal = 1
;

--- Inclusion Rule Inserts

CREATE TABLE synpuf_110k_results.jb67xkbminclusion_0
 AS
SELECT
0 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.jb67xkbmqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.jb67xkbmqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.jb67xkbmqualified_events p
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
  join synpuf_110k_results.jb67xkbmcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 5))
) c


-- End Condition Occurrence Criteria

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

CREATE TABLE synpuf_110k_results.jb67xkbminclusion_1
 AS
SELECT
1 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.jb67xkbmqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.jb67xkbmqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.jb67xkbmqualified_events p
inner join
(
  -- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.jb67xkbmcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 6))
) c


-- End Drug Exposure Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) >= 1
-- End Correlated Criteria

   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.jb67xkbminclusion_events
 AS
SELECT
inclusion_rule_id, person_id, event_id

FROM
(select inclusion_rule_id, person_id, event_id from synpuf_110k_results.jb67xkbminclusion_0
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.jb67xkbminclusion_1) i;
DELETE FROM synpuf_110k_results.jb67xkbminclusion_0 WHERE True;
drop table synpuf_110k_results.jb67xkbminclusion_0;

DELETE FROM synpuf_110k_results.jb67xkbminclusion_1 WHERE True;
drop table synpuf_110k_results.jb67xkbminclusion_1;


CREATE TABLE synpuf_110k_results.jb67xkbmincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.jb67xkbmqualified_events q
    left join synpuf_110k_results.jb67xkbminclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  where (mg.inclusion_rule_mask = power(cast(2  as int64),2)-1)

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.jb67xkbmstrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 30 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 30 DAY) else start_date end as end_date

FROM
synpuf_110k_results.jb67xkbmincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.jb67xkbmcohort_rows
 AS WITH cohort_ends   as ( select event_id as event_id,person_id as person_id,end_date  as end_date  from synpuf_110k_results.jb67xkbmstrategy_ends

union all
-- Censor Events
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.jb67xkbmincluded_events i
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
join synpuf_110k_results.jb67xkbmcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 6))
) c


-- End Drug Exposure Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
  group by  i.event_id, i.person_id


  ), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
   as end_date from (
    select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
    from synpuf_110k_results.jb67xkbmincluded_events i
    join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
  ) f
  where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.jb67xkbmfinal_cohort
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
      from synpuf_110k_results.jb67xkbmcohort_rows
    
      union all
    

      select
        person_id
        , DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
        , 1 as event_type
        , null
      from synpuf_110k_results.jb67xkbmcohort_rows
    ) rawdata
  ) e
  where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
     as person_id,c.start_date
     as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.jb67xkbmcohort_rows c
  join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
   group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_yyxyd8h0 where cohort_definition_id = 5;
insert into synpuf_110k_results.temp_cohort_yyxyd8h0 (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 5 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.jb67xkbmfinal_cohort co
;



DELETE FROM synpuf_110k_results.jb67xkbmstrategy_ends WHERE True;
drop table synpuf_110k_results.jb67xkbmstrategy_ends;


DELETE FROM synpuf_110k_results.jb67xkbmcohort_rows WHERE True;
drop table synpuf_110k_results.jb67xkbmcohort_rows;

DELETE FROM synpuf_110k_results.jb67xkbmfinal_cohort WHERE True;
drop table synpuf_110k_results.jb67xkbmfinal_cohort;

DELETE FROM synpuf_110k_results.jb67xkbminclusion_events WHERE True;
drop table synpuf_110k_results.jb67xkbminclusion_events;

DELETE FROM synpuf_110k_results.jb67xkbmqualified_events WHERE True;
drop table synpuf_110k_results.jb67xkbmqualified_events;

DELETE FROM synpuf_110k_results.jb67xkbmincluded_events WHERE True;
drop table synpuf_110k_results.jb67xkbmincluded_events;

DELETE FROM synpuf_110k_results.jb67xkbmcodesets WHERE True;
drop table synpuf_110k_results.jb67xkbmcodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.tgzocwtycodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.tgzocwtycodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where 0=1
) i
) c;
insert into synpuf_110k_results.tgzocwtycodesets (codeset_id, concept_id)
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (316866)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (316866)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.tgzocwtycodesets (codeset_id, concept_id)
select 2 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (316139)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (316139)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.tgzocwtycodesets (codeset_id, concept_id)
select 3 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1308842)

) i
) c;
insert into synpuf_110k_results.tgzocwtycodesets (codeset_id, concept_id)
select 4 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where 0=1
) i
) c;
insert into synpuf_110k_results.tgzocwtycodesets (codeset_id, concept_id)
select 5 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (192855,2617208,2617223,45473170,4092691,45581152,45561747,45436352,45571462,45755324,40320129,40385855,45566652,4094409,45469941,40385856,4097577,45443002,1571950,44825227,44825228,44835711,44820640,44831012,44833333,44821755,44828756,44834516,44834515,45595727,1567715,44833324,45537806,45581264,35206455,45581267,45576195,1576214,45576194,1576213,45605158,35225348,35225347,35225346,35225345,35225349,45537636,44800307,40519171,45471615,40380245,44813431,45449609,40380244,45513074,44794980,40376617,45493219,45480040,44801444,40385836,45476721,44794562,45503163,40385421,44800478,40385849,45493217,45428190,44798515,45438118,44798509,40521920)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (192855,2617208,2617223,45473170,4092691,45581152,45561747,45436352,45571462,45755324,40320129,40385855,45566652,4094409,45469941,40385856,4097577,45443002,1571950,44825227,44825228,44835711,44820640,44831012,44833333,44821755,44828756,44834516,44834515,45595727,1567715,44833324,45537806,45581264,35206455,45581267,45576195,1576214,45576194,1576213,45605158,35225348,35225347,35225346,35225345,35225349,45537636,44800307,40519171,45471615,40380245,44813431,45449609,40380244,45513074,44794980,40376617,45493219,45480040,44801444,40385836,45476721,44794562,45503163,40385421,44800478,40385849,45493217,45428190,44798515,45438118,44798509,40521920)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.tgzocwtycodesets (codeset_id, concept_id)
select 6 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (40235485,1351557,1346686,1347384,1367500,40226742,1317640)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40235485,1351557,1346686,1347384,1367500,40226742,1317640)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.tgzocwtyqualified_events
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
  select de.* , row_number() over (partition by de.person_id order by de.drug_era_start_date, de.drug_era_id) as ordinal
  from synpuf_110k.drug_era de
where de.drug_concept_id in (select concept_id from  synpuf_110k_results.tgzocwtycodesets where codeset_id = 6)
) c

where DATE_DIFF(cast(c.drug_era_end_date as date), cast(c.drug_era_start_date as date), DAY) >= 30
and c.ordinal = 1
-- End Drug Era Criteria

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
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from primary_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from primary_events p
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
  join synpuf_110k_results.tgzocwtycodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) >= 1
-- End Correlated Criteria

union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from primary_events p
inner join
(
  -- Begin Condition Era Criteria
select c.person_id, c.condition_era_id as event_id, c.condition_era_start_date as start_date,
       c.condition_era_end_date as end_date, c.condition_concept_id as target_concept_id, cast(null  as int64) as visit_occurrence_id,
       c.condition_era_start_date as sort_date
from 
(
  select ce.* 
  from synpuf_110k.condition_era ce
where ce.condition_concept_id in (select concept_id from  synpuf_110k_results.tgzocwtycodesets where codeset_id = 2)
) c


-- End Condition Era Criteria

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

) qe
where qe.ordinal = 1
;

--- Inclusion Rule Inserts

CREATE TABLE synpuf_110k_results.tgzocwtyinclusion_0
 AS
SELECT
0 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.tgzocwtyqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.tgzocwtyqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.tgzocwtyqualified_events p
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
  join synpuf_110k_results.tgzocwtycodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 5))
) c


-- End Condition Occurrence Criteria

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

CREATE TABLE synpuf_110k_results.tgzocwtyinclusion_1
 AS
SELECT
1 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.tgzocwtyqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.tgzocwtyqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.tgzocwtyqualified_events p
inner join
(
  -- Begin Drug Exposure Criteria
select c.person_id, c.drug_exposure_id as event_id, c.drug_exposure_start_date as start_date,
       coalesce(c.drug_exposure_end_date, DATE_ADD(cast(c.drug_exposure_start_date as date), interval 1 DAY)) as end_date, c.drug_concept_id as target_concept_id, c.visit_occurrence_id,
       c.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  from synpuf_110k.drug_exposure de
join synpuf_110k_results.tgzocwtycodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 3))
) c


-- End Drug Exposure Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) >= 1
-- End Correlated Criteria

   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.tgzocwtyinclusion_events
 AS
SELECT
inclusion_rule_id, person_id, event_id

FROM
(select inclusion_rule_id, person_id, event_id from synpuf_110k_results.tgzocwtyinclusion_0
union all
select inclusion_rule_id, person_id, event_id from synpuf_110k_results.tgzocwtyinclusion_1) i;
DELETE FROM synpuf_110k_results.tgzocwtyinclusion_0 WHERE True;
drop table synpuf_110k_results.tgzocwtyinclusion_0;

DELETE FROM synpuf_110k_results.tgzocwtyinclusion_1 WHERE True;
drop table synpuf_110k_results.tgzocwtyinclusion_1;


CREATE TABLE synpuf_110k_results.tgzocwtyincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.tgzocwtyqualified_events q
    left join synpuf_110k_results.tgzocwtyinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  where (mg.inclusion_rule_mask = power(cast(2  as int64),2)-1)

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;

-- date offset strategy

CREATE TABLE synpuf_110k_results.tgzocwtystrategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(cast(start_date as date), interval 30 DAY) > start_date then DATE_ADD(cast(start_date as date), interval 30 DAY) else start_date end as end_date

FROM
synpuf_110k_results.tgzocwtyincluded_events;


-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.tgzocwtycohort_rows
 AS WITH cohort_ends   as ( select event_id as event_id,person_id as person_id,end_date  as end_date  from synpuf_110k_results.tgzocwtystrategy_ends

union all
-- Censor Events
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from synpuf_110k_results.tgzocwtyincluded_events i
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
join synpuf_110k_results.tgzocwtycodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 3))
) c


-- End Drug Exposure Criteria

) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
  group by  i.event_id, i.person_id


  ), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
   as end_date from (
    select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
    from synpuf_110k_results.tgzocwtyincluded_events i
    join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
  ) f
  where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.tgzocwtyfinal_cohort
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
      from synpuf_110k_results.tgzocwtycohort_rows
    
      union all
    

      select
        person_id
        , DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
        , 1 as event_type
        , null
      from synpuf_110k_results.tgzocwtycohort_rows
    ) rawdata
  ) e
  where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
     as person_id,c.start_date
     as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.tgzocwtycohort_rows c
  join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
   group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_yyxyd8h0 where cohort_definition_id = 6;
insert into synpuf_110k_results.temp_cohort_yyxyd8h0 (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 6 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.tgzocwtyfinal_cohort co
;



DELETE FROM synpuf_110k_results.tgzocwtystrategy_ends WHERE True;
drop table synpuf_110k_results.tgzocwtystrategy_ends;


DELETE FROM synpuf_110k_results.tgzocwtycohort_rows WHERE True;
drop table synpuf_110k_results.tgzocwtycohort_rows;

DELETE FROM synpuf_110k_results.tgzocwtyfinal_cohort WHERE True;
drop table synpuf_110k_results.tgzocwtyfinal_cohort;

DELETE FROM synpuf_110k_results.tgzocwtyinclusion_events WHERE True;
drop table synpuf_110k_results.tgzocwtyinclusion_events;

DELETE FROM synpuf_110k_results.tgzocwtyqualified_events WHERE True;
drop table synpuf_110k_results.tgzocwtyqualified_events;

DELETE FROM synpuf_110k_results.tgzocwtyincluded_events WHERE True;
drop table synpuf_110k_results.tgzocwtyincluded_events;

DELETE FROM synpuf_110k_results.tgzocwtycodesets WHERE True;
drop table synpuf_110k_results.tgzocwtycodesets;

///////////////////////////////////////////////////////////////

create table synpuf_110k_results.kqopy53tcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.kqopy53tcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (443392)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (443392)
  and c.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.kqopy53tcodesets (codeset_id, concept_id)
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (443392)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (443392)
  and c.invalid_reason is null

) i
) c;


CREATE TABLE synpuf_110k_results.kqopy53tqualified_events
 AS WITH primary_events   as (select p.ordinal  as event_id,p.person_id as person_id,p.start_date as start_date,p.end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,cast(p.visit_occurrence_id  as int64)  as visit_occurrence_id from (
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(cast(c.condition_start_date as date), interval 1 DAY)) as end_date,
       c.condition_concept_id as target_concept_id, c.visit_occurrence_id,
       c.condition_start_date as sort_date
from 
(
  select co.* 
  from synpuf_110k.condition_occurrence co
  join synpuf_110k_results.kqopy53tcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) c


-- End Condition Occurrence Criteria

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

create table synpuf_110k_results.kqopy53tinclusion_events (inclusion_rule_id INT64,
  person_id INT64,
  event_id INT64
);

CREATE TABLE synpuf_110k_results.kqopy53tincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.kqopy53tqualified_events q
    left join synpuf_110k_results.kqopy53tinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;



-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.kqopy53tcohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,op_end_date  as end_date from synpuf_110k_results.kqopy53tincluded_events
), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
   as end_date from (
    select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
    from synpuf_110k_results.kqopy53tincluded_events i
    join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
  ) f
  where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.kqopy53tfinal_cohort
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
      from synpuf_110k_results.kqopy53tcohort_rows
    
      union all
    

      select
        person_id
        , DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
        , 1 as event_type
        , null
      from synpuf_110k_results.kqopy53tcohort_rows
    ) rawdata
  ) e
  where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
     as person_id,c.start_date
     as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.kqopy53tcohort_rows c
  join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
   group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.temp_cohort_yyxyd8h0 where cohort_definition_id = 7;
insert into synpuf_110k_results.temp_cohort_yyxyd8h0 (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 7 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.kqopy53tfinal_cohort co
;





DELETE FROM synpuf_110k_results.kqopy53tcohort_rows WHERE True;
drop table synpuf_110k_results.kqopy53tcohort_rows;

DELETE FROM synpuf_110k_results.kqopy53tfinal_cohort WHERE True;
drop table synpuf_110k_results.kqopy53tfinal_cohort;

DELETE FROM synpuf_110k_results.kqopy53tinclusion_events WHERE True;
drop table synpuf_110k_results.kqopy53tinclusion_events;

DELETE FROM synpuf_110k_results.kqopy53tqualified_events WHERE True;
drop table synpuf_110k_results.kqopy53tqualified_events;

DELETE FROM synpuf_110k_results.kqopy53tincluded_events WHERE True;
drop table synpuf_110k_results.kqopy53tincluded_events;

DELETE FROM synpuf_110k_results.kqopy53tcodesets WHERE True;
drop table synpuf_110k_results.kqopy53tcodesets;