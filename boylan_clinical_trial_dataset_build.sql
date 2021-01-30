--replace view BODI_Project_Pub.BODI70 as
with 
    gcore as
    	(select
			s.study_id
			, s.therapeutic_area
			, s.full_title
			, s.molecule_name
			, pa.screening_number
			, pa.patient_number
			, pa.arm_name
			, pa.cohort_name
			, pa.indication
			, pa.gender
			, pa.DOB
			, pa.site_number
			, pa.site_name
			, pa.site_country
			, pa.principal_investigator
			, pv.visit_name
			, pv.visit_date
			, row_number() over (partition by pa.screening_number order by pv.visit_date desc) most_recent_visit
		
		from
			BODI_TRANSFORM_PROD.GCOR_DI_Studies s
			
		left join
			BODI_TRANSFORM_PROD.GCOR_DI_Patients pa
			on s.study_rec_id = pa.study_rec_id
			
		left join
			BODI_TRANSFORM_PROD.GCOR_DI_Patient_Visits pv
			on pa.patient_rec_id = pv.patient_rec_id
			
		where	
			s.study_id = 'GO41596'
		
--		order by
--			pa.screening_number
--			, pv.visit_date
		), 
			
	q2s_spec as
		(select
			a.screening_number
			, a.roche_specimen_id
		    , a.collect_datetime
		    , a.visit_name
		    , b.visit_date
		    , c.visit_time
		    , a.source_file_rec_id
		
		from
			bodi_transform_prod.specimens a
			
		left join	
			(select
				"VALUE" as visit_date
				, origin_rec_id
				
			from	
				bodi_transform_prod.tdi_flex_data
				
			where
				entity_name = 'Specimens'
				and attribute_name = 'VISIT_DATE') b
			on a.origin_rec_id = b.origin_rec_id		
			
		left join
			(select
				"VALUE" as visit_time
				, origin_rec_id
				
			from	
				bodi_transform_prod.tdi_flex_data
				
			where
				entity_name = 'Specimens'
				and attribute_name = 'VISIT_TIME') c
			on a.origin_rec_id = c.origin_rec_id	
			
		where
			study_id = 'GO41596'
			and vendor_name = 'Q2S'),			
			
	q2s_assay as
		(select
	      a.screening_number
	      , a.roche_specimen_id
	      , a.assay_target
	      , a.zbtest_cd
	      , a.assay_result
	      , a.uom
	      , b."VALUE" test_confirmed_date
	      , a.source_file_rec_id
			
		from
			bodi_transform_prod.assays a
			
		left join
			bodi_transform_prod.tdi_flex_data b
			on a.origin_rec_id = b.origin_rec_id
			
		where
			a.study_id = 'GO41596'
			and a.vendor_name = 'Q2S'
			and b.attribute_name = 'TEST_CONFIRMED_DATE'),
			
	tdq_spec as
		(select
			enrollment_id
--			, screening_number --missing from TDQ source data
			, visit_kit_type
--			, roche_specimen_id
			, case when roche_specimen_id = '1100101' then '110011' else roche_specimen_id end as roche_specimen_id -- older TDQ files had it correct; this matches with the HGX sample id
			, collection_method
			, collect_datetime
			, harvest_location
			, surgical_path_id
			, source_file_rec_id
			
		from
			bodi_transform_prod.specimens
			
		where
			study_id = 'GO41596'
			and vendor_name is null), --TDQ file
	
	hgx_assay as
		(select
			c.screening_number
			, a.roche_specimen_id
			, a.zbtest_cd
			, a.assay_result
			, a.source_file_rec_id
	      	, b."VALUE" vendor_sample_id
	      	, d."VALUE" hgx_accession_number
	      	
	    from
	    	bodi_transform_prod.assays a
			
		left join
			bodi_transform_prod.tdi_flex_data b
			on a.origin_rec_id = b.origin_rec_id
			
		left join --get screening number from HGX RESULTS file because it is not provided in the HGX INVENTORY file
			(select distinct
				screening_number
				, roche_specimen_id
				
			from	
				bodi_transform_prod.assays
				
			where
				study_id = 'GO41596'
				and vendor_name = 'Histogenex'
			) c
			on a.roche_specimen_id = c.roche_specimen_id
			
		left join
			bodi_transform_prod.tdi_flex_data d
			on a.origin_rec_id = d.origin_rec_id
			
		where
			a.study_id = 'GO41596'
			and a.vendor_name = 'HGX_INVENTORY'
			and b.attribute_name = 'VENDOR_SAMPLE_ID'	
			and d.attribute_name = 'HGX_ACCESSION_NUMBER'
		
		union
		
		select
			a.screening_number
			, a.roche_specimen_id
			, a.zbtest_cd
			, a.assay_result
			, a.source_file_rec_id
	      	, b."VALUE" vendor_sample_id
	      	, 'null' hgx_accession_number
			
		from
			bodi_transform_prod.assays a
			
		left join
			bodi_transform_prod.tdi_flex_data b
			on a.origin_rec_id = b.origin_rec_id
			
		where
			a.study_id = 'GO41596'
			and a.vendor_name = 'Histogenex'
			and b.attribute_name = 'VENDOR_SAMPLE_ID'
	      	)
					
select
	*
	
from
	(select distinct
		study_id 					gcore_pa_study_id
		, therapeutic_area			gcore_pa_therapeutic_area
		, full_title				gcore_pa_full_title
		, molecule_name				gcore_pa_molecule_name
		, screening_number			gcore_pa_screening_number
		, patient_number			gcore_pa_patient_number
		, arm_name					gcore_pa_arm_name
		, cohort_name				gcore_pa_cohort_name
		, indication				gcore_pa_indication
		, gender					gcore_pa_gender
		, DOB						gcore_pa_DOB
		, site_number				gcore_pa_site_number
		, site_name					gcore_pa_site_name
		, site_country				gcore_pa_site_country
		, principal_investigator	gcore_pa_principal_investigator
	
	from
		gcore
	) gcore_pa
	
left join
	(select
		screening_number scrn
--		, visit_name
		, case when visit_name = 'Screening' then 'Screening'
			when visit_name = 'Enrollment Visit' or visit_name like 'Medication Assignment%' then 'Enrolled'
			when visit_name like '%Discontin%' then 'Discontinued'
			when visit_name like 'Screen Fail%' then 'Screen Failed'
			when visit_name like 'Re%' then 'Re-screened'
			else 'Other'
			end as subject_status

	from
		gcore
	
	where
		most_recent_visit = 1
	) gcore_status
	on gcore_pa.gcore_pa_screening_number = gcore_status.scrn
	
full join
	(	
	select distinct
		gcore_pv.screening_number	gcore_pv_screening_number
		, gcore_pv.patient_number	gcore_pv_patient_number
		, gcore_pv.visit_name		gcore_pv_visit_name
		, gcore_pv.visit_date		gcore_pv_visit_date
		, q2s.screening_number		q2s_screening_number
		, q2s.roche_specimen_id		q2s_roche_specimen_id
		, q2s.collect_datetime		q2s_collect_datetime
		, q2s.visit_name			q2s_visit_name
		, cast( substring( q2s.visit_date, 1, 10 ) as date)			q2s_visit_date
		, q2s.visit_time			q2s_visit_time
		, q2s.assay_target			q2s_assay_target
		, q2s.zbtest_cd				q2s_zbtest_cd
		, q2s.assay_result			q2s_assay_result
		, q2s.uom					q2s_uom
		, q2s.test_confirmed_date	q2s_test_confirmed_date
		, q2s.source_file_name		q2s_source_file_name
		, hgx.screening_number		hgx_screening_number
		, hgx.enrollment_id			hgx_enrollment_id
		, hgx.visit_kit_type		hgx_visit_kit_type
		, hgx.roche_specimen_id		hgx_roche_specimen_id
		, hgx.collection_method		hgx_collection_method
		, hgx.collect_datetime		hgx_collect_datetime
		, hgx.harvest_location		hgx_harvest_location
		, hgx.surgical_path_id		hgx_surgical_path_id
		, hgx.vendor_sample_id		hgx_vendor_sample_id
		, hgx.hgx_accession_number	hgx_hgx_accession_number		
		, hgx.zbtest_cd				hgx_zbtest_cd
		, hgx.assay_result			hgx_assay_result
		, hgx.hgx_source_file_name
		, hgx.tdq_source_file_name
	
	from
		(
		select distinct
			screening_number
			, patient_number
			, visit_name
			, visit_date
			
		from
			gcore
		) gcore_pv
	
	full join			
		(select distinct
			q2s_spec.screening_number
			, q2s_spec.roche_specimen_id
			, q2s_spec.collect_datetime
			, q2s_spec.visit_name
			, q2s_spec.visit_date
			, q2s_spec.visit_time		
			, q2s_assay.assay_target
			, q2s_assay.zbtest_cd
			, q2s_assay.assay_result
			, q2s_assay.uom
			, q2s_assay.test_confirmed_date
			, q2s_sou.source_file_name
			
		from
			q2s_spec
		
		inner join
			q2s_assay
			on q2s_spec.roche_specimen_id = q2s_assay.roche_specimen_id
			and q2s_spec.screening_number = q2s_assay.screening_number
			
		left join
			(select
				source_file_rec_id
            	, source_file_name
        
            from
            	bodi_transform_prod.source_files) q2s_sou
        		on q2s_assay.source_file_rec_id = q2s_sou.source_file_rec_id
		) q2s
		on gcore_pv.visit_name = q2s.zbtest_cd --pick two fields that do not match because we want the buckets of data on different rows
	
	full join
		(select 
			tdq_spec.enrollment_id
			, tdq_spec.visit_kit_type
			, hgx_assay.roche_specimen_id
			, tdq_spec.collection_method
			, tdq_spec.collect_datetime
			, tdq_spec.harvest_location
			, tdq_spec.surgical_path_id
			, tdq_sou.source_file_name	tdq_source_file_name
			, hgx_assay.screening_number
			, hgx_assay.vendor_sample_id
			, hgx_assay.hgx_accession_number			
			, hgx_assay.zbtest_cd
			, hgx_assay.assay_result
			, hgx_sou.source_file_name	hgx_source_file_name
			
		from
			hgx_assay
			
		left join
			tdq_spec
			on hgx_assay.vendor_sample_id = tdq_spec.roche_specimen_id
--			and hgx_assay.screening_number = tdq_spec.enrollment_id --same value for both fields
					
		left join
			(select
				source_file_rec_id
            	, source_file_name
        
            from
            	bodi_transform_prod.source_files) hgx_sou
        		on hgx_assay.source_file_rec_id = hgx_sou.source_file_rec_id
        					
		left join
			(select
				source_file_rec_id
            	, source_file_name
        
            from
            	bodi_transform_prod.source_files) tdq_sou
        		on tdq_spec.source_file_rec_id = tdq_sou.source_file_rec_id
		) hgx
		on q2s.assay_result = hgx.zbtest_cd --pick two fields that do not match because we want the buckets of data on different rows
	
	) data_cat
	on gcore_pa.gcore_pa_screening_number = coalesce(data_cat.gcore_pv_screening_number, data_cat.q2s_screening_number, data_cat.hgx_screening_number)
;