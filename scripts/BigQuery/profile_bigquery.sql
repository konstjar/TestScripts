select gender_concept_id, year_of_birth, concept_name as gender
from synpuf_110k.person p
join synpuf_110k.concept c on p.gender_concept_id = c.concept_id
where person_id = <person_id>

/////////////////////// observation periods

select observation_period_id, observation_period_start_date as start_date, observation_period_end_date as end_date, concept_name observation_period_type
from synpuf_110k.observation_period op
join synpuf_110k.concept c on c.concept_id = op.period_type_concept_id
where person_id = <person_id>

/////////////////////// simplified records

select 'drug' as domain, drug_concept_id concept_id, concept_name, drug_exposure_start_date start_date, drug_exposure_end_date end_date
from synpuf_110k.drug_exposure d
join synpuf_110k.concept c on d.drug_concept_id = c.concept_id
where person_id = <person_id>

union all

select 'drugera' as domain, drug_concept_id concept_id, concept_name, drug_era_start_date start_date, drug_era_end_date end_date 
from synpuf_110k.drug_era 
join synpuf_110k.concept c on c.concept_id = drug_era.drug_concept_id
where person_id = <person_id>  

union all 

select 'condition' as domain, condition_concept_id concept_id, concept_name, condition_start_date start_date, condition_end_date end_date
from synpuf_110k.condition_occurrence co
join synpuf_110k.concept c on co.condition_concept_id = c.concept_id
where person_id = <person_id>

union all

select 'conditionera' as domain, condition_concept_id concept_id, concept_name, condition_era_start_date start_date, condition_era_end_date end_date 
from synpuf_110k.condition_era
join synpuf_110k.concept c on c.concept_id = condition_era.condition_concept_id
where person_id = <person_id>  

union  all

select 'observation' as domain, observation_concept_id concept_id, concept_name, observation_date start_date, observation_date end_date 
from synpuf_110k.observation
join synpuf_110k.concept c on c.concept_id = observation.observation_concept_id
where person_id = <person_id>  

union all

select 'visit' as domain, visit_concept_id concept_id, concept_name, visit_start_date start_date, visit_end_date end_date 
from synpuf_110k.visit_occurrence
join synpuf_110k.concept c on c.concept_id = visit_occurrence.visit_concept_id
where person_id = <person_id> 

union all

select 'death' as domain, death_type_concept_id concept_id, concept_name, death_date start_date, death_date end_date
from synpuf_110k.death d
join synpuf_110k.concept c on d.death_type_concept_id = c.concept_id
where person_id = <person_id>

union  all

select 'measurement' as domain, measurement_concept_id concept_id, concept_name, measurement_date start_date, measurement_date end_date
from synpuf_110k.measurement m
join synpuf_110k.concept c on m.measurement_concept_id = c.concept_id
where person_id = <person_id>

union  all

select 'device' as domain, device_concept_id concept_id, concept_name, device_exposure_start_date start_date, device_exposure_end_date end_date 
from synpuf_110k.device_exposure de
join synpuf_110k.concept c on de.device_concept_id = c.concept_id
where person_id = <person_id>

union  all

select 'procedure' as domain, procedure_concept_id concept_id, concept_name, procedure_date start_date, procedure_date end_date 
from synpuf_110k.procedure_occurrence po
join synpuf_110k.concept c on po.procedure_concept_id = c.concept_id
where person_id = <person_id>

union all

select 'specimen' as domain, specimen_concept_id concept_id, concept_name, specimen_date start_date, specimen_date end_date 
from synpuf_110k.specimen s
join synpuf_110k.concept c on s.specimen_concept_id = c.concept_id
where person_id = <person_id>

/////////////////////// cohorts

select subject_id, cohort_definition_id, cohort_start_date, cohort_end_date
from synpuf_110k_results.cohort
where subject_id = ?