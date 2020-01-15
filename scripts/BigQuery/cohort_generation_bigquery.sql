create table synpuf_110k_results.z0k5duppcodesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;

insert into synpuf_110k_results.z0k5duppcodesets (codeset_id, concept_id)
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (1118084)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1118084)
  and c.invalid_reason is null
union distinct select distinct cr.concept_id_1 as concept_id
from
(
  select concept_id from synpuf_110k.concept where concept_id in (1118084)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1118084)
  and c.invalid_reason is null

) c
join synpuf_110k.concept_relationship cr on c.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason is null

) i
) c;
insert into synpuf_110k_results.z0k5duppcodesets (codeset_id, concept_id)
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from synpuf_110k.concept where concept_id in (4280942,28779,198798,4112183,194382,192671,196436,4338225)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4280942,28779,198798,4112183,192671,4338225)
  and c.invalid_reason is null

) i
left join
(
  select concept_id from synpuf_110k.concept where concept_id in (194158)
union distinct select c.concept_id
  from synpuf_110k.concept c
  join synpuf_110k.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (194158)
  and c.invalid_reason is null

) e on i.concept_id = e.concept_id
where e.concept_id is null
) c;


CREATE TABLE synpuf_110k_results.z0k5duppqualified_events
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
join synpuf_110k_results.z0k5duppcodesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) c


-- End Drug Exposure Criteria

  ) e
  join synpuf_110k.observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(cast(op.observation_period_start_date as date), interval 30 DAY) <= e.start_date and DATE_ADD(cast(e.start_date as date), interval 0 DAY) <= op.observation_period_end_date
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

CREATE TABLE synpuf_110k_results.z0k5duppinclusion_0
 AS
SELECT
0 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  from synpuf_110k_results.z0k5duppqualified_events pe
  
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from synpuf_110k_results.z0k5duppqualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from synpuf_110k_results.z0k5duppqualified_events p
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
  join synpuf_110k_results.z0k5duppcodesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) c


-- End Condition Occurrence Criteria

) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(cast(p.start_date as date), interval 0 DAY)
  group by  p.person_id, p.event_id
 having count(a.target_concept_id) <= 0
-- End Correlated Criteria

   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;

CREATE TABLE synpuf_110k_results.z0k5duppinclusion_events
 AS
SELECT
inclusion_rule_id, person_id, event_id

FROM
(select inclusion_rule_id, person_id, event_id from synpuf_110k_results.z0k5duppinclusion_0) i;
DELETE FROM synpuf_110k_results.z0k5duppinclusion_0 WHERE True;
drop table synpuf_110k_results.z0k5duppinclusion_0;


CREATE TABLE synpuf_110k_results.z0k5duppincluded_events
 AS WITH cteincludedevents  as (select event_id as event_id,person_id as person_id,start_date as start_date,end_date as end_date,op_start_date as op_start_date,op_end_date as op_end_date,row_number() over (partition by person_id order by start_date asc)  as ordinal from (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from synpuf_110k_results.z0k5duppqualified_events q
    left join synpuf_110k_results.z0k5duppinclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  where (mg.inclusion_rule_mask = power(cast(2  as int64),1)-1)

)
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteincludedevents results
where results.ordinal = 1
;



-- generate cohort periods into #final_cohort
CREATE TABLE synpuf_110k_results.z0k5duppcohort_rows
 AS WITH cohort_ends   as (select event_id as event_id,person_id as person_id,op_end_date  as end_date from synpuf_110k_results.z0k5duppincluded_events
), first_ends   as (select f.person_id as person_id,f.start_date as start_date,f.end_date
   as end_date from (
    select i.event_id, i.person_id, i.start_date, e.end_date, row_number() over (partition by i.person_id, i.event_id order by e.end_date) as ordinal 
    from synpuf_110k_results.z0k5duppincluded_events i
    join cohort_ends e on i.event_id = e.event_id and i.person_id = e.person_id and e.end_date >= i.start_date
  ) f
  where f.ordinal = 1
)
 SELECT person_id, start_date, end_date
 FROM first_ends;

CREATE TABLE synpuf_110k_results.z0k5duppfinal_cohort
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
      from synpuf_110k_results.z0k5duppcohort_rows
    
      union all
    

      select
        person_id
        , DATE_ADD(cast(end_date as date), interval 0 DAY) as end_date
        , 1 as event_type
        , null
      from synpuf_110k_results.z0k5duppcohort_rows
    ) rawdata
  ) e
  where (2 * e.start_ordinal) - e.overall_ord = 0
), cteends   as ( select c.person_id
     as person_id,c.start_date
     as start_date,min(e.end_date)  as end_date  from synpuf_110k_results.z0k5duppcohort_rows c
  join cteenddates e on c.person_id = e.person_id and e.end_date >= c.start_date
   group by  c.person_id, c.start_date
 )
  SELECT person_id, min(start_date) as start_date, end_date
 FROM cteends
 group by  1, 3 ;

delete from synpuf_110k_results.cohort where cohort_definition_id = 2;
insert into synpuf_110k_results.cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 2 as cohort_definition_id, person_id, start_date, end_date 
from synpuf_110k_results.z0k5duppfinal_cohort co
;


-- Find the event that is the 'best match' per person.  
-- the 'best match' is defined as the event that satisfies the most inclusion rules.
-- ties are solved by choosing the event that matches the earliest inclusion rule, and then earliest.

CREATE TABLE synpuf_110k_results.z0k5duppbest_events
 AS
SELECT
q.person_id, q.event_id

FROM
synpuf_110k_results.z0k5duppqualified_events q
join (
  select r.person_id, r.event_id, row_number() over (partition by r.person_id order by r.rule_count desc,r.min_rule_id asc, r.start_date asc) as rank_value
  from (
     select q.person_id, q.event_id, coalesce(cast(count(distinct i.inclusion_rule_id) as int64), 0) as rule_count, coalesce(cast(min(i.inclusion_rule_id) as int64), 0) as min_rule_id, q.start_date
     from synpuf_110k_results.z0k5duppqualified_events q
    left join synpuf_110k_results.z0k5duppinclusion_events i on q.person_id = i.person_id and q.event_id = i.event_id
     group by  q.person_id, q.event_id, q.start_date
   ) r
) ranked on q.person_id = ranked.person_id and q.event_id = ranked.event_id
where ranked.rank_value = 1
;

-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)
-- 0: all events
-- 1: best event


-- BEGIN: Inclusion Impact Analysis - event
-- calculte matching group counts
delete from synpuf_110k_results.cohort_inclusion_result where cohort_definition_id = 2 and mode_id = 0;
insert into synpuf_110k_results.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
 select 2 as cohort_definition_id, inclusion_rule_mask, COUNT(*) as person_count, 0 as mode_id
 from (
   select q.person_id, q.event_id, cast(sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0))  as int64) as inclusion_rule_mask
   from synpuf_110k_results.z0k5duppqualified_events q
  left join synpuf_110k_results.z0k5duppinclusion_events i on q.person_id = i.person_id and q.event_id = i.event_id
   group by  q.person_id, q.event_id
 ) mg -- matching groups
 group by  2 ;

-- calculate gain counts 
delete from synpuf_110k_results.cohort_inclusion_stats where cohort_definition_id = 2 and mode_id = 0;
insert into synpuf_110k_results.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select ir.cohort_definition_id, ir.rule_sequence, coalesce(cast(t.person_count as int64), 0) as person_count, coalesce(cast(sr.person_count as int64), 0) gain_count, eventtotal.total, 0 as mode_id
from synpuf_110k_results.cohort_inclusion ir
left join
(
   select i.inclusion_rule_id, COUNT(i.event_id) as person_count
   from synpuf_110k_results.z0k5duppqualified_events q
  join synpuf_110k_results.z0k5duppinclusion_events i on q.person_id = i.person_id and q.event_id = i.event_id
   group by  i.inclusion_rule_id
 ) t on ir.rule_sequence = t.inclusion_rule_id
cross join (select count(*) as total_rules from synpuf_110k_results.cohort_inclusion where cohort_definition_id = 2) ruletotal
cross join (select COUNT(event_id) as total from synpuf_110k_results.z0k5duppqualified_events) eventtotal
left join synpuf_110k_results.cohort_inclusion_result sr on sr.mode_id = 0 and sr.cohort_definition_id = 2 and (power(cast(2  as int64),ruletotal.total_rules) - power(cast(2  as int64),ir.rule_sequence) - 1) = sr.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule' 
where ir.cohort_definition_id = 2
;

-- calculate totals
delete from synpuf_110k_results.cohort_summary_stats where cohort_definition_id = 2 and mode_id = 0;
insert into synpuf_110k_results.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select 2 as cohort_definition_id, pc.total as person_count, coalesce(cast(fc.total as int64), 0) as final_count, 0 as mode_id
from
(select COUNT(event_id) as total from synpuf_110k_results.z0k5duppqualified_events) pc,
(select sum(sr.person_count) as total
  from synpuf_110k_results.cohort_inclusion_result sr
  cross join (select count(*) as total_rules from synpuf_110k_results.cohort_inclusion where cohort_definition_id = 2) ruletotal
  where sr.mode_id = 0 and sr.cohort_definition_id = 2 and sr.inclusion_rule_mask = power(cast(2  as int64),ruletotal.total_rules)-1
) fc
;

-- END: Inclusion Impact Analysis - event

-- BEGIN: Inclusion Impact Analysis - person
-- calculte matching group counts
delete from synpuf_110k_results.cohort_inclusion_result where cohort_definition_id = 2 and mode_id = 1;
insert into synpuf_110k_results.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
 select 2 as cohort_definition_id, inclusion_rule_mask, COUNT(*) as person_count, 1 as mode_id
 from (
   select q.person_id, q.event_id, cast(sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0))  as int64) as inclusion_rule_mask
   from synpuf_110k_results.z0k5duppbest_events q
  left join synpuf_110k_results.z0k5duppinclusion_events i on q.person_id = i.person_id and q.event_id = i.event_id
   group by  q.person_id, q.event_id
 ) mg -- matching groups
 group by  2 ;

-- calculate gain counts 
delete from synpuf_110k_results.cohort_inclusion_stats where cohort_definition_id = 2 and mode_id = 1;
insert into synpuf_110k_results.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select ir.cohort_definition_id, ir.rule_sequence, coalesce(cast(t.person_count as int64), 0) as person_count, coalesce(cast(sr.person_count as int64), 0) gain_count, eventtotal.total, 1 as mode_id
from synpuf_110k_results.cohort_inclusion ir
left join
(
   select i.inclusion_rule_id, COUNT(i.event_id) as person_count
   from synpuf_110k_results.z0k5duppbest_events q
  join synpuf_110k_results.z0k5duppinclusion_events i on q.person_id = i.person_id and q.event_id = i.event_id
   group by  i.inclusion_rule_id
 ) t on ir.rule_sequence = t.inclusion_rule_id
cross join (select count(*) as total_rules from synpuf_110k_results.cohort_inclusion where cohort_definition_id = 2) ruletotal
cross join (select COUNT(event_id) as total from synpuf_110k_results.z0k5duppbest_events) eventtotal
left join synpuf_110k_results.cohort_inclusion_result sr on sr.mode_id = 1 and sr.cohort_definition_id = 2 and (power(cast(2  as int64),ruletotal.total_rules) - power(cast(2  as int64),ir.rule_sequence) - 1) = sr.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule' 
where ir.cohort_definition_id = 2
;

-- calculate totals
delete from synpuf_110k_results.cohort_summary_stats where cohort_definition_id = 2 and mode_id = 1;
insert into synpuf_110k_results.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select 2 as cohort_definition_id, pc.total as person_count, coalesce(cast(fc.total as int64), 0) as final_count, 1 as mode_id
from
(select COUNT(event_id) as total from synpuf_110k_results.z0k5duppbest_events) pc,
(select sum(sr.person_count) as total
  from synpuf_110k_results.cohort_inclusion_result sr
  cross join (select count(*) as total_rules from synpuf_110k_results.cohort_inclusion where cohort_definition_id = 2) ruletotal
  where sr.mode_id = 1 and sr.cohort_definition_id = 2 and sr.inclusion_rule_mask = power(cast(2  as int64),ruletotal.total_rules)-1
) fc
;

-- END: Inclusion Impact Analysis - person

-- BEGIN: Censored Stats

-- END: Censored Stats

DELETE FROM synpuf_110k_results.z0k5duppbest_events WHERE True;
drop table synpuf_110k_results.z0k5duppbest_events;





DELETE FROM synpuf_110k_results.z0k5duppcohort_rows WHERE True;
drop table synpuf_110k_results.z0k5duppcohort_rows;

DELETE FROM synpuf_110k_results.z0k5duppfinal_cohort WHERE True;
drop table synpuf_110k_results.z0k5duppfinal_cohort;

DELETE FROM synpuf_110k_results.z0k5duppinclusion_events WHERE True;
drop table synpuf_110k_results.z0k5duppinclusion_events;

DELETE FROM synpuf_110k_results.z0k5duppqualified_events WHERE True;
drop table synpuf_110k_results.z0k5duppqualified_events;

DELETE FROM synpuf_110k_results.z0k5duppincluded_events WHERE True;
drop table synpuf_110k_results.z0k5duppincluded_events;

DELETE FROM synpuf_110k_results.z0k5duppcodesets WHERE True;
drop table synpuf_110k_results.z0k5duppcodesets;
