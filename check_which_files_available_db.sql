--what files are available
with
	variables as (select 'WO41554' as sid)

select
	*
	--,distinct source_file_name

from
	bodi_transform_prod.SOURCE_FILES
	
where
	source_file_rec_id in (
		select distinct
			source_file_rec_id 
		
		from
			bodi_transform_prod.assays, variables
		
		where
			study_id = sid)
	or source_file_rec_id in (
		select distinct
			source_file_rec_id 
		
		from
			bodi_transform_prod.specimens, variables
		
		where
			study_id = sid)
	or source_file_rec_id in (
		select distinct
			source_file_rec_id 
		
		from
			bodi_transform_prod.samples, variables
		
		where
			study_id = sid)
			
	or source_file_rec_id in (
		select distinct
			source_file_rec_id 
		
		from
			bodi_transform_prod.patients
		
		where
			study_rec_id in (
				select
					distinct study_rec_id
			
				from
					bodi_transform_prod.studies, variables
					
				where
					study_id = sid))

order by
	source_file_name;