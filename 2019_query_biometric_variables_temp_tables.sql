-----------------------------------------------------
-------------------- Biometric ----------------------
-------------------- Biometric ----------------------
-------------------- Biometric ----------------------
-----------------------------------------------------

DECLARE @START_DATE AS DATE = '2014-01-01'
DECLARE @END_DATE AS DATE = '2018-01-01'


SELECT --top 1000
	mem.MemberID
	, bs.MemberScreeningID
	, mem.GroupID
	, hpg.GroupName
	, mem.HealthPlanID
	, hpg.HealthPlanName
	, mem.Gender
	, mem.Relationship
	, floor( cast(getdate()-(mem.Birthdate) as decimal)/365.25 ) as 'Age'
	--, year(bs.ScreeningDate) as 'Year'
	--, month(bs.ScreeningDate) as 'Month'
	--, datepart(quarter, bs.ScreeningDate) as 'Quarter'
	, CONVERT(date, bs.ScreeningDate) as 'ScreeningDate'
	, bs.IsPregnant
	, bs.IsFasting
	--, bsr.smokeflag
	, bsr.BMI
	--, bsr.HeightInches
	, bsr.WeightLbs
	--, bsr.WaistCircumference
	, bsr.Cholesterol
	, bsr.HDL
	, bsr.LDL
	, bsr.Triglycerides
	, bsr.Systolic
	, bsr.Diastolic
	, bsr.Glucose
	--, bsr.Diabetes
	
	, case when (bsr.HDL < 40 and mem.Gender='M') or (bsr.HDL < 50 and mem.Gender='F') then 1 else 0 end as METS_HDL
	, case when (bsr.Triglycerides >= 150) then 1 else 0 end as METS_Trig
	, case when (bsr.BMI >= 30) or (bsr.WaistCircumference >= 40 and mem.Gender='M') or (bsr.WaistCircumference >= 35 and mem.Gender='F') then 1 else 0 end as METS_Obesity
	, case when (bsr.Systolic >= 130) or (bsr.Diastolic >= 85) then 1 else 0 end as METS_BP
	, case when (bs.IsFasting = 1 and bsr.Glucose >= 100) or (bs.IsFasting = 0 and bsr.Glucose >= 140) then 1 else 0 end as METS_Glucose
	
	, ROW_NUMBER() OVER (PARTITION BY mem.MemberID order by mem.MemberID, bs.ScreeningDate) AS 'First'
	, ROW_NUMBER() OVER (PARTITION BY mem.MemberID order by mem.MemberID, bs.ScreeningDate desc) AS 'Last'

INTO
	#basepop_biometrics
FROM
	DA_Production.prod.healthplangroup hpg

INNER JOIN
	DA_Production.prod.Member mem
	ON hpg.GroupID = mem.GroupID

INNER JOIN
	DA_Production.prod.BiometricsScreening bs 
	ON mem.MemberID = bs.MemberID 
	AND mem.GroupID = bs.GroupID

INNER JOIN 
	DA_Production.prod.BiometricsScreeningResults bsr 
	ON bs.MemberScreeningID = bsr.MemberScreeningID

WHERE 
bs.IsPregnant <> 1
and bs.ScreeningDate >= @START_DATE and bs.ScreeningDate < @END_DATE






--only keep first and last screenings, but not if they were the same screening
delete from #basepop_biometrics 
where First + Last = 2 
or (First <> 1 and Last <> 1)


----------------------------------------------------------------------------------------------------------

/* #basepop_0_fasting */ --create sequence flags

select ba.*
, case when (ba.METS_HDL + ba.METS_Trig + ba.METS_Obesity + ba.METS_BP + ba.METS_Glucose) >= 3 then 1 else 0 end as METS
	
into 
	#basepop_fasting_0

from
	#basepop_biometrics ba 

where 
	ba.IsFasting=1

----------------------------------------------------------------------------------------------------------

/* #basepop_0_not_fasting */ --create sequence flags


select ba.*
, case when (ba.METS_HDL + ba.METS_Trig + ba.METS_Obesity + ba.METS_BP + ba.METS_Glucose) >= 3 then 1 else 0 end as METS
	
into 
	#basepop_not_fasting_0

from #basepop_biometrics ba 

where 
	ba.IsFasting=0



----------------------------------------------------------------------------------------------------------



select 
	a.MemberID as [MemberID]
	, a.GroupID as [GroupID]
	, a.GroupName as [GroupName]
	, a.HealthPlanID as [HealthPlanID]
	, a.HealthPlanName as [HealthPlanName]
	, a.Gender as [Gender]
	, a.Age as [Age]
	, a.Relationship as [Relationship]
	, a.MemberScreeningID as [MemberScreeningID_t1]
	--, a.Year as [Year_t1]
	--, a.Month as [Month_t1]
	--, a.Quarter as [Quarter_t1]
	, a.ScreeningDate as [ScreeningDate_t1]
	, a.IsPregnant as [IsPregnant_t1]
	, a.IsFasting as [IsFasting_t1]
	--, a.smokeflag as [smokeflag_t1]
	, a.BMI as [BMI_t1]
	--, a.HeightInches as [HeightInches_t1]
	, a.WeightLbs as [WeightLbs_t1]
	--, a.WaistCircumference as [WaistCircumference_t1]
	, a.Cholesterol as [Cholesterol_t1]
	, a.HDL as [HDL_t1]
	, a.LDL as [LDL_t1]
	, a.Triglycerides as [Triglycerides_t1]
	, a.Systolic as [Systolic_t1]
	, a.Diastolic as [Diastolic_t1]
	, a.Glucose as [Glucose_t1]
	--, a.Diabetes as [Diabetes_t1]
	, a.METS_HDL as [METS_HDL_t1]
	, a.METS_Trig as [METS_Trig_t1]
	, a.METS_Obesity as [METS_Obesity_t1]
	, a.METS_BP as [METS_BP_t1]
	, a.METS_Glucose as [METS_Glucose_t1]
	, a.METS as [METS_t1]

	, b.MemberScreeningID as [MemberScreeningID_t2]
	--, b.Year as [Year_t2]
	--, b.Month as [Month_t2]
	--, b.Quarter as [Quarter_t2]
	, b.ScreeningDate as [ScreeningDate_t2]
	, b.IsPregnant as [IsPregnant_t2]
	, b.IsFasting 	as [IsFasting_t2]
	--, b.smokeflag 	as [smokeflag_t2]
	, b.BMI as [BMI_t2]
	--, b.HeightInches as [HeightInches_t2]
	, b.WeightLbs as [WeightLbs_t2]
	--, b.WaistCircumference as [WaistCircumference_t2]
	, b.Cholesterol as [Cholesterol_t2]
	, b.HDL as [HDL_t2]
	, b.LDL as [LDL_t2]
	, b.Triglycerides as [Triglycerides_t2]
	, b.Systolic as [Systolic_t2]
	, b.Diastolic as [Diastolic_t2]
	, b.Glucose as [Glucose_t2]
	--, b.Diabetes as [Diabetes_t2]
	, b.METS_HDL as [METS_HDL_t2]
	, b.METS_Trig as [METS_Trig_t2]
	, b.METS_Obesity as [METS_Obesity_t2]
	, b.METS_BP as [METS_BP_t2]
	, b.METS_Glucose as [METS_Glucose_t2]
	, b.METS as [METS_t2]

	, a.First
	, b.Last
	, a.Last as 'Number_of_events'

	, DATEDIFF(day, a.ScreeningDate, b.ScreeningDate) as 'DateDifference'
	
into #basepop_fasting

from (select * from #basepop_fasting_0 where First=1 and Last <> 1) a

inner join 
	(select * from #basepop_fasting_0 where Last=1 and First <> 1) b
	on a.MemberID = b.MemberID
	and a.GroupID = b.GroupID

where DATEDIFF(DAY, DATEADD(year, 1, a.ScreeningDate), b.ScreeningDate) between -90 and 90
	or DATEDIFF(DAY, DATEADD(year, 2, a.ScreeningDate), b.ScreeningDate) between -90 and 90
	or DATEDIFF(DAY, DATEADD(year, 3, a.ScreeningDate), b.ScreeningDate) between -90 and 90

--in other words, only members whose last screening mostly adhered to the annual screening regimen i.e. +/- 3 months from their screening anniversaries as long as they had the same fasting status 
--any screenings in no-man's-land are removed
--To be included in the analysis, members had to have completed a screening within 3 months of a one-year interval from their initial t1 screening with the same fasting status. In other words, the analysis cohort only included members that adhered to the annual screening protocol.


		
--where DATEDIFF(day, a.ScreeningDate, b.ScreeningDate) between 273 and 457
--	or DATEDIFF(DAY, DATEADD(year, 1, a.ScreeningDate), b.ScreeningDate) between 273 and 457
--	or DATEDIFF(DAY, DATEADD(year, 2, a.ScreeningDate), b.ScreeningDate) between 273 and 457

----------------------------------------------------------------------------------------------------------


select 
	a.MemberID as [MemberID]
	, a.GroupID as [GroupID]
	, a.GroupName as [GroupName]
	, a.HealthPlanID as [HealthPlanID]
	, a.HealthPlanName as [HealthPlanName]
	, a.Gender as [Gender]
	, a.Age as [Age]
	, a.Relationship as [Relationship]
	, a.MemberScreeningID as [MemberScreeningID_t1]
	--, a.Year as [Year_t1]
	--, a.Month as [Month_t1]
	--, a.Quarter as [Quarter_t1]
	, a.ScreeningDate as [ScreeningDate_t1]
	, a.IsPregnant as [IsPregnant_t1]
	, a.IsFasting as [IsFasting_t1]
	--, a.smokeflag as [smokeflag_t1]
	, a.BMI as [BMI_t1]
	--, a.HeightInches as [HeightInches_t1]
	, a.WeightLbs as [WeightLbs_t1]
	--, a.WaistCircumference as [WaistCircumference_t1]
	, a.Cholesterol as [Cholesterol_t1]
	, a.HDL as [HDL_t1]
	, a.LDL as [LDL_t1]
	, a.Triglycerides as [Triglycerides_t1]
	, a.Systolic as [Systolic_t1]
	, a.Diastolic as [Diastolic_t1]
	, a.Glucose as [Glucose_t1]
	--, a.Diabetes as [Diabetes_t1]
	, a.METS_HDL as [METS_HDL_t1]
	, a.METS_Trig as [METS_Trig_t1]
	, a.METS_Obesity as [METS_Obesity_t1]
	, a.METS_BP as [METS_BP_t1]
	, a.METS_Glucose as [METS_Glucose_t1]
	, a.METS as [METS_t1]

	, b.MemberScreeningID as [MemberScreeningID_t2]
	--, b.Year as [Year_t2]
	--, b.Month as [Month_t2]
	--, b.Quarter as [Quarter_t2]
	, b.ScreeningDate as [ScreeningDate_t2]
	, b.IsPregnant as [IsPregnant_t2]
	, b.IsFasting 	as [IsFasting_t2]
	--, b.smokeflag 	as [smokeflag_t2]
	, b.BMI as [BMI_t2]
	--, b.HeightInches as [HeightInches_t2]
	, b.WeightLbs as [WeightLbs_t2]
	--, b.WaistCircumference as [WaistCircumference_t2]
	, b.Cholesterol as [Cholesterol_t2]
	, b.HDL as [HDL_t2]
	, b.LDL as [LDL_t2]
	, b.Triglycerides as [Triglycerides_t2]
	, b.Systolic as [Systolic_t2]
	, b.Diastolic as [Diastolic_t2]
	, b.Glucose as [Glucose_t2]
	--, b.Diabetes as [Diabetes_t2]
	, b.METS_HDL as [METS_HDL_t2]
	, b.METS_Trig as [METS_Trig_t2]
	, b.METS_Obesity as [METS_Obesity_t2]
	, b.METS_BP as [METS_BP_t2]
	, b.METS_Glucose as [METS_Glucose_t2]
	, b.METS as [METS_t2]

	, a.First
	, b.Last
	, a.Last as 'Number_of_events'

	, DATEDIFF(day, a.ScreeningDate, b.ScreeningDate) as 'DateDifference'
	
into #basepop_not_fasting

from (select * from #basepop_not_fasting_0 where First=1 and Last <> 1) a

inner join 
	(select * from #basepop_not_fasting_0 where Last=1 and First <> 1) b
	on a.MemberID = b.MemberID
	and a.GroupID = b.GroupID

where DATEDIFF(DAY, DATEADD(year, 1, a.ScreeningDate), b.ScreeningDate) between -90 and 90
	or DATEDIFF(DAY, DATEADD(year, 2, a.ScreeningDate), b.ScreeningDate) between -90 and 90
	or DATEDIFF(DAY, DATEADD(year, 3, a.ScreeningDate), b.ScreeningDate) between -90 and 90




----------------------------------------------------------------------------------------------------------



/* UNION */
select 
	*
into
	#cohort --#basepop_union 

from
	(select * from #basepop_fasting 
	union
	select * from #basepop_not_fasting
	) as #temp_union

order by memberid, ScreeningDate_t1


--drop table #temp_union



 



 




	/* Coaching --add number of completed coaching calls, and Challenges*/ 
SELECT
	src.*, 
	case when appt.CompletedCalls is null then 0 else appt.CompletedCalls end as Completed_Calls,
	CASE WHEN appt.CompletedCalls > 0 THEN 1 ELSE 0 END AS CoachingParticipant,
	CASE WHEN ch.MemberID IS NOT NULL THEN 1 ELSE 0 END AS ChallengeParticipant,
	CASE WHEN clas.MemberID IS NOT NULL THEN 1 ELSE 0 END AS OnlineClassParticipant

INTO
	#cohort_with_coaching_00

FROM
	#cohort src

LEFT JOIN
	(
	Select 
		app.MemberID, 
		src_1.MemberScreeningID_t1, 
		count(app.MemberID) as [CompletedCalls]
	From 
		DA_Production.prod.appointment app
	inner join 
		#cohort src_1 
		on app.MemberID = src_1.MemberID
	Where 
		app.AppointmentStatusName='Call Completed'
		AND app.AppointmentBeginDate between src_1.ScreeningDate_t1 and src_1.ScreeningDate_t2
	group by 
		app.MemberID, 
		src_1.MemberScreeningID_t1	
	) appt
	ON src.MemberID = appt.MemberID
	and appt.MemberScreeningID_t1 = src.MemberScreeningID_t1

LEFT JOIN
	(
	SELECT 
		chal.MemberID,
		src_2.MemberScreeningID_t1,
		1 AS 'Challenge'
	FROM
		da_production.prod.challenge chal
	INNER JOIN	
		#cohort src_2
		ON (chal.MemberID = src_2.MemberID)
		AND chal.CompletionDate between src_2.ScreeningDate_t1 and src_2.ScreeningDate_t2
	GROUP BY
		chal.MemberID,
		src_2.MemberScreeningID_t1	
	) ch
	ON src.MemberID = ch.MemberID
	AND src.MemberScreeningID_t1 = ch.MemberScreeningID_t1

LEFT JOIN
	(
	SELECT 
		cla.MemberID,
		src_3.MemberScreeningID_t1,
		1 AS 'Classes'
	FROM
		da_production.prod.WebClass cla
	INNER JOIN	
		#cohort src_3
		ON (cla.MemberID = src_3.MemberID)
		AND cla.CompleteDate between src_3.ScreeningDate_t1 and src_3.ScreeningDate_t2
	GROUP BY
		cla.MemberID,
		src_3.MemberScreeningID_t1	
	) clas
	ON src.MemberID = clas.MemberID
	AND src.MemberScreeningID_t1 = clas.MemberScreeningID_t1











	

select src_1.*
	, 1 as eligible_yr1
	
	, case when DATEDIFF(DAY, DATEADD(year, 2, src_1.ScreeningDate_t1), src_1.ScreeningDate_t2) between -90 and 90 then 1
		when DATEDIFF(DAY, DATEADD(year, 3, src_1.ScreeningDate_t1), src_1.ScreeningDate_t2) between -90 and 90 then 1
		else 0
	end as eligible_yr2

	, case when DATEDIFF(DAY, DATEADD(year, 3, src_1.ScreeningDate_t1), src_1.ScreeningDate_t2) between -90 and 90 then 1
		else 0
	end as eligible_yr3
	
into #cohort_with_coaching_000

from #cohort_with_coaching_00 src_1













select src_1.*
	, case when DATEDIFF(DAY, DATEADD(year, 1, src_1.ScreeningDate_t1), src_1.ScreeningDate_t2) between -90 and 90 then src_1.ScreeningDate_t2 --one year +/- 3 months
		else DATEADD(year, 1, src_1.ScreeningDate_t1)
	end as date_y1
	
	, case when DATEDIFF(DAY, DATEADD(year, 2, src_1.ScreeningDate_t1), src_1.ScreeningDate_t2) between -90 and 90 then src_1.ScreeningDate_t2 --one year +/- 3 months
		else DATEADD(year, 2, src_1.ScreeningDate_t1)
	end as date_y2
	
	, case when DATEDIFF(DAY, DATEADD(year, 3, src_1.ScreeningDate_t1), src_1.ScreeningDate_t2) between -90 and 90 then src_1.ScreeningDate_t2 --one year +/- 3 months
		else DATEADD(year, 3, src_1.ScreeningDate_t1)
	end as date_y3
	
into #cohort_with_coaching_0

from #cohort_with_coaching_000 src_1







SELECT
	src.*
	, isnull(appt.CompletedCalls_y1,0) as 'Completed_Calls_y1' 

INTO
	#cohort_with_coaching_0_cv1

FROM
	#cohort_with_coaching_0 src

LEFT JOIN
	(
	Select 
		app.MemberID, 
		src_1.MemberScreeningID_t1, 
		count(app.MemberID) as [CompletedCalls_y1]
	From 
		DA_Production.prod.appointment app
	inner join 
		#cohort_with_coaching_0 src_1 
		on app.MemberID = src_1.MemberID
	Where 
		app.AppointmentStatusName='Call Completed'
		AND app.AppointmentEndDate between src_1.ScreeningDate_t1 and src_1.date_y1
	group by 
		app.MemberID, 
		src_1.MemberScreeningID_t1	
	) appt
	ON src.MemberID = appt.MemberID
	and appt.MemberScreeningID_t1 = src.MemberScreeningID_t1












SELECT
	src.*
	, case when eligible_yr2 = 1 then isnull(appt.CompletedCalls_y2,0) else 0 end as 'Completed_Calls_y2'

INTO
	#cohort_with_coaching_0_cv2

FROM
	#cohort_with_coaching_0_cv1 src

LEFT JOIN
	(
	Select 
		app.MemberID, 
		src_1.MemberScreeningID_t1, 
		count(app.MemberID) as [CompletedCalls_y2]
	From 
		DA_Production.prod.appointment app
	inner join 
		#cohort_with_coaching_0 src_1 
		on app.MemberID = src_1.MemberID
	Where 
		app.AppointmentStatusName='Call Completed'
		AND app.AppointmentEndDate between DATEADD(year, 1, src_1.ScreeningDate_t1) and src_1.date_y2
	group by 
		app.MemberID, 
		src_1.MemberScreeningID_t1	
	) appt
	ON src.MemberID = appt.MemberID
	and appt.MemberScreeningID_t1 = src.MemberScreeningID_t1
	

		




SELECT
	src.*
	, case when eligible_yr3 = 1 then isnull(appt.CompletedCalls_y3,0) else 0 end as 'Completed_Calls_y3'

INTO
	#cohort_with_coaching_0_cv3

FROM
	#cohort_with_coaching_0_cv2 src

LEFT JOIN
	(
	Select 
		app.MemberID, 
		src_1.MemberScreeningID_t1, 
		count(app.MemberID) as [CompletedCalls_y3]
	From 
		DA_Production.prod.appointment app
	inner join 
		#cohort_with_coaching_0 src_1 
		on app.MemberID = src_1.MemberID
	Where 
		app.AppointmentStatusName='Call Completed'

		AND app.AppointmentEndDate between DATEADD(year, 2, src_1.ScreeningDate_t1) and src_1.date_y3

	group by 
		app.MemberID, 
		src_1.MemberScreeningID_t1	
	) appt
	ON src.MemberID = appt.MemberID
	and appt.MemberScreeningID_t1 = src.MemberScreeningID_t1 













select 
	a.*
	, cast(Completed_Calls * 365.25 as decimal) / DateDifference as calls_per_year
	, case when Completed_Calls >= 4 then '4+ Sessions' 
		when Completed_Calls between 1 and 3 then '1-3 Sessions' 
		else 'No Sessions' end as 'Coaching'

	
into #cohort_with_coaching

from #cohort_with_coaching_0_cv3 a


--select top 100 * from #cohort_with_coaching where calls_per_year > 0 order by MemberID, ScreeningDate_t1








select distinct cwc.MemberID
	, app.AppointmentEndDate
	, app.AppointmentID
	, case 
		when pe.TerminationDate is not null and app.AppointmentEndDate between pe.EnrollmentDate and pe.TerminationDate then pe.ProgramName 
		when pe.TerminationDate is null and app.AppointmentEndDate between pe.EnrollmentDate and getdate() then pe.ProgramName
			else '' end as ProgramName

into #transition_table

from 
	#cohort_with_coaching cwc

inner join 
	DA_Production.prod.appointment app 
	on cwc.MemberID=app.MemberID
inner join
	DA_Production.prod.ProgramEnrollment pe 
	on cwc.MemberID=pe.MemberID
where 
	pe.ProgramName in ('Health Improvement','Stress Management','Weight Management','Tobacco Cessation')
	and app.AppointmentStatusName='Call Completed'
	and ( (pe.TerminationDate is not null and app.AppointmentEndDate between pe.EnrollmentDate and pe.TerminationDate) or (pe.TerminationDate is null and app.AppointmentEndDate between pe.EnrollmentDate and getdate()) )
order by 
	app.AppointmentEndDate





/* Transition Table Ranking of program with the most calls*/
select 
	tt.MemberID
	, tt.ProgramName
	, count(tt.ProgramName) 'Count_of_ProgramName'
	, cwc.ScreeningDate_t1
	, cwc.ScreeningDate_t2 
	, ROW_NUMBER() OVER (PARTITION BY tt.MemberID, cwc.ScreeningDate_t1 order by tt.MemberID, cwc.ScreeningDate_t1, count(tt.ProgramName) desc) AS ProgramRank

into 
	#transition_table_rank

from 
	#transition_table tt

inner join 
	#cohort_with_coaching cwc 
	on tt.MemberID=cwc.MemberID

where 
	tt.AppointmentEndDate between cwc.ScreeningDate_t1 and cwc.ScreeningDate_t2 
	group by tt.MemberID, tt.ProgramName, cwc.ScreeningDate_t1, cwc.ScreeningDate_t2
	order by tt.MemberID, count(tt.ProgramName) desc


/* #ProgramEnrollmentRank1 */
select
	cwc.*
	, case when cwc.Completed_Calls > 0 and ttr1.ProgramRank = 1 then ttr1.ProgramName else 'NULL' end as 'Primary_Program_Enrollment'
	, case when cwc.Completed_Calls > 0 and ttr1.ProgramRank = 1 then ttr1.Count_of_ProgramName else 0 end as 'Primary_Program_Enrollment_Count'

into #ProgramEnrollmentRank1

from 
	#cohort_with_coaching cwc

left join 
	(select * from #transition_table_rank 
	where ProgramRank=1 ) ttr1
	 	on cwc.MemberID=ttr1.MemberID
		and cwc.ScreeningDate_t1=ttr1.ScreeningDate_t1

/* #ProgramEnrollmentRank2 */
select 
	per1.*
	, case when per1.Completed_Calls > 0 and ttr2.ProgramRank = 2 then ttr2.ProgramName else 'NULL' end as 'Secondary_Program_Enrollment'
	, case when per1.Completed_Calls > 0 and ttr2.ProgramRank = 2 then ttr2.Count_of_ProgramName else 0 end as 'Secondary_Program_Enrollment_Count'
	
into #ProgramEnrollmentRank2

from 
	#ProgramEnrollmentRank1 per1

left join 
	(select * from #transition_table_rank 
		where ProgramRank=2) ttr2
		on per1.MemberID=ttr2.MemberID
		and per1.ScreeningDate_t1=ttr2.ScreeningDate_t1








------------------------------------------------------------------------
--Connected!
select 
	a.MemberID
	, a.FactMovementMeasureDailyMaxID
	, cwc.ScreeningDate_t1
	, cwc.ScreeningDate_t2
	, a.ActivityDateTime
	, datepart(week, a.ActivityDateTime) as WeekOfActivity_Conn
	, year(a.ActivityDateTime) as YearOfActivity_Conn
	, a.MovementMeasureValue

into #Connected

from
	#ProgramEnrollmentRank2 cwc

left join
	Da_production.dbo.FactMovementMeasureDailyMax a
	on cwc.MemberID = a.MemberID

where
	a.ActivityDateTime between cwc.ScreeningDate_t1 and cwc.ScreeningDate_t2
	and a.DimMovementMeasureType='Steps'


--select top 1000 * from #Connected order by MemberID, ActivityDateTime



select --*
	MemberID
	, YearOfActivity_Conn
	, WeekOfActivity_Conn
	, sum(MovementMeasureValue) as WeekSteps
	, case when sum(MovementMeasureValue) >= 49000 then 1 else 0 end as EngagedThatWeekFlag_Conn

into #Connected_2

from #Connected

where WeekOfActivity_Conn <= 52

group by MemberID, WeekOfActivity_Conn, YearOfActivity_Conn



--select top 1000 * from #Connected_2 order by MemberID, YearOfActivity, WeekOfActivity


 


  select
	cwc.*,
	CASE WHEN isnull(b.TotalSteps,0) > 0 THEN 1 ELSE 0 END AS ConnectedParticipant,    --BB 6-11-18 changed ConnectedParticipant to be any steps and not based on weeks engaged.
	case when b.WeeksEngaged_Conn is null then 0 else b.WeeksEngaged_Conn end as WeeksEngaged_Conn,
	CONVERT(int,DATEDIFF(WEEK, ScreeningDate_t1, ScreeningDate_t2)) as WeeksBetweent1t2,
	case when isnull(b.WeeksEngaged_Conn, 0) >= ( 0.7 * CONVERT(int, DATEDIFF(WEEK, ScreeningDate_t1, ScreeningDate_t2)) )  --number of engaged weeks >= 70% of weeks between t1 and t2
		then 1 else 0 end as ConnectedEngagedFlag

into
	#per2

from
	#ProgramEnrollmentRank2 cwc

left join
	(
	select
		MemberID
		, sum(EngagedThatWeekFlag_Conn) as WeeksEngaged_Conn
		, sum(WeekSteps) as TotalSteps

	from 
		#Connected_2
	group by 
		MemberID
	) b
	on cwc.MemberID = b.MemberID


-------------------------

/* Flags */  --for significant changes in Biometric Outcomes Measurements
select
	per2.*

	, case when (per2.Glucose_t1 >= 100 and per2.IsFasting_t1 = 1) or (per2.Glucose_t1 >= 140 and per2.IsFasting_t1 = 0) then 1 else 0 end as Glucose_ModerateHigh_t1
	, case when (per2.Glucose_t2 >= 100 and per2.IsFasting_t2 = 1) or (per2.Glucose_t2 >= 140 and per2.IsFasting_t2 = 0) then 1 else 0 end as Glucose_ModerateHigh_t2
	
	, case when per2.Systolic_t1 >= 120 or per2.Diastolic_t1 >= 80 then 1 else 0 end as BP_ModerateHigh_t1
	, case when per2.Systolic_t2 >= 120 or per2.Diastolic_t2 >= 80 then 1 else 0 end as BP_ModerateHigh_t2
	
	, case when per2.Cholesterol_t1 >= 200 then 1 else 0 end as Cholesterol_ModerateHigh_t1
	, case when per2.Cholesterol_t2 >= 200 then 1 else 0 end as Cholesterol_ModerateHigh_t2
	
	, case when per2.HDL_t1 <= 59 then 1 else 0 end as HDL_ModerateHigh_t1
	, case when per2.HDL_t2 <= 59 then 1 else 0 end as HDL_ModerateHigh_t2
	
	, case when per2.LDL_t1 >= 130 then 1 else 0 end as LDL_ModerateHigh_t1
	, case when per2.LDL_t2 >= 130 then 1 else 0 end as LDL_ModerateHigh_t2
	
	, case when per2.Triglycerides_t1 >= 150 then 1 else 0 end as Trig_ModerateHigh_t1
	, case when per2.Triglycerides_t2 >= 150 then 1 else 0 end as Trig_ModerateHigh_t2
	
	--, case when (per2.Cholesterol_t1 / per2.HDL_t1) >= 3.5 then 1 else 0 end as CholRatio__ModerateHigh_t1
	--, case when (per2.Cholesterol_t2 / per2.HDL_t2) >= 3.5 then 1 else 0 end as CholRatio__ModerateHigh_t2
	
	, case when (per2.BMI_t1 >= 25 or per2.BMI_t1 < 18.5) then 1 else 0 end as BMI_ModerateHigh_t1
	, case when (per2.BMI_t2 >= 25 or per2.BMI_t2 < 18.5) then 1 else 0 end as BMI_ModerateHigh_t2
	
	--, case when (per2.WaistCircumference_t1 >= 41 and per2.Gender = 'M') or (per2.WaistCircumference_t1 >= 36 and per2.Gender = 'F') then 1 else 0 end as Waist_ModerateHigh_t1
	--, case when (per2.WaistCircumference_t2 >= 41 and per2.Gender = 'M') or (per2.WaistCircumference_t2 >= 36 and per2.Gender = 'F') then 1 else 0 end as Waist_ModerateHigh_t2


	,case when ( cast((per2.WeightLbs_t2 - per2.WeightLbs_t1) as decimal) / per2.WeightLbs_t1 ) <= (-.05) then 1 else 0 end as '5% Weight Loss'
	,case when ( cast((per2.LDL_t2 - per2.LDL_t1) as decimal) / per2.LDL_t1 ) <= (-.10) then 1 else 0 end as '10% LDL Reduction'
	,case when per2.HDL_t2 - per2.HDL_t1 >= 5 then 1 else 0 end as '5 mgdl HDL Increase'
	,case when per2.Triglycerides_t2 - per2.Triglycerides_t1 <= -40 then 1 else 0 end as '40 mgdl Triglycerides Reduction'
	,case when (per2.Systolic_t2 - per2.Systolic_t1 <= -5) then 1 else 0 end as '5 mmHg Systolic Reduction'
	,case when (per2.Diastolic_t2 - per2.Diastolic_t1 <= -5) then 1 else 0 end as '5 mmHg Diastolic Reduction'
	,case when (per2.Glucose_t2 - per2.Glucose_t1 <= -20) and per2.IsFasting_t1 = 1 and per2.IsFasting_t2 = 1 then 1 else 0 end as '20 mgdl Glucose Reduction'
	,case when (per2.METS_BP_t2 + per2.METS_Glucose_t2 + per2.METS_HDL_t2 + per2.METS_Obesity_t2 + per2.METS_Trig_t2) < 
		(per2.METS_BP_t1 + per2.METS_Glucose_t1 + per2.METS_HDL_t1 + per2.METS_Obesity_t1 + per2.METS_Trig_t1) then 1 else 0 end as 'Reduction Of At Least One METs Risk' --'Reduction_by_one_METs_risk_factor_Flag'
	,case when per2.METS_t1 = 1 and per2.METS_t2 = 0 then 1 else 0 end as 'METs Resolution'


	, case when per2.Systolic_t1 is not null and per2.Systolic_t2 is not null and per2.Systolic_t1 > 0 and per2.Systolic_t2 > 0
		then 1 else 0 end as has_t1_t2_Systolic
	
	, case when per2.Diastolic_t1 is not null and per2.Diastolic_t2 is not null and per2.Diastolic_t1 > 0 and per2.Diastolic_t2 > 0
		then 1 else 0 end as has_t1_t2_Diastolic
	
	, case when per2.Glucose_t1 is not null and per2.Glucose_t2 is not null and per2.Glucose_t1 > 0 and per2.Glucose_t2 > 0
		then 1 else 0 end as has_t1_t2_Glucose
	
	, case when per2.Triglycerides_t1 is not null and per2.Triglycerides_t2 is not null and per2.Triglycerides_t1 > 0 and per2.Triglycerides_t2 > 0
		then 1 else 0 end as has_t1_t2_Triglycerides
	
	, case when per2.BMI_t1 is not null and per2.BMI_t2 is not null and per2.BMI_t1 > 0 and per2.BMI_t2 > 0
		then 1 else 0 end as has_t1_t2_BMI
	
	, case when per2.Cholesterol_t1 is not null and per2.Cholesterol_t2 is not null and per2.Cholesterol_t1 > 0 and per2.Cholesterol_t2 > 0
		then 1 else 0 end as has_t1_t2_Cholesterol
	
	, case when per2.HDL_t1 is not null and per2.HDL_t2 is not null and per2.HDL_t1 > 0 and per2.HDL_t2 > 0
		then 1 else 0 end as has_t1_t2_HDL
	
	, case when per2.LDL_t1 is not null and per2.LDL_t2 is not null and per2.LDL_t1 > 0 and per2.LDL_t2 > 0
		then 1 else 0 end as has_t1_t2_LDL
	
	, case when per2.METS_t1 is not null and per2.METS_t2 is not null then 1 else 0 end as has_t1_t2_METS

	
	, case when per2.METS_BP_t1 is not null and
		per2.METS_Glucose_t1 is not null and
		per2.METS_HDL_t1 is not null and
		per2.METS_Obesity_t1 is not null and
		per2.METS_Trig_t1 is not null and
		per2.METS_BP_t2 is not null and
		per2.METS_Glucose_t2 is not null and
		per2.METS_HDL_t2 is not null and
		per2.METS_Obesity_t2 is not null and
		per2.METS_Trig_t2 is not null
	then 1 else 0 end as has_t1_t2_METS_all_scores



	, case when per2.ChallengeParticipant + per2.CoachingParticipant + per2.OnlineClassParticipant + per2.ConnectedParticipant = 0
		then 1 else 0 end as NonParticipant
	
into 
	#cohort_with_flags

from 
	#per2 per2



select * from #cohort_with_flags order by MemberID, ScreeningDate_t1

--select * from #cohort_with_flags where YearDifference_factor='3-4 Years' order by MemberID, ScreeningDate_t1







drop table #basepop_biometrics
drop table #basepop_fasting_0
drop table #basepop_not_fasting_0
drop table #basepop_fasting
drop table #basepop_not_fasting
drop table #cohort
drop table #cohort_with_coaching_00
drop table #cohort_with_coaching_0
drop table #cohort_with_coaching_000
drop table #cohort_with_coaching_0_cv1
drop table #cohort_with_coaching_0_cv2
drop table #cohort_with_coaching_0_cv3
drop table #cohort_with_coaching
drop table #transition_table
drop table #transition_table_rank
drop table #ProgramEnrollmentRank1
drop table #ProgramEnrollmentRank2
drop table #cohort_with_flags


 --6-19-18
 --181199
 --3:30
