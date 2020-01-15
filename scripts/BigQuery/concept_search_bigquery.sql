select concept_id, concept_name, IFNULL(standard_concept,'N') standard_concept, IFNULL(invalid_reason,'V') invalid_reason, concept_code, concept_class_id, domain_id, vocabulary_id
from synpuf_110k.concept
where (lower(concept_name) like %ibuprofen% or lower(concept_code) like %ibuprofen% or cast(concept_id as STRING) = ibuprofen)

order by concept_name asc