WITH DNR AS(
SELECT DISTINCT stay_id
FROM physionet-data.mimiciv_icu.chartevents
WHERE value LIKE '%DNR%'
), Intubation AS (
SELECT DISTINCT p.stay_id, p.starttime, p.endtime, p.ventilation_status
FROM physionet-data.mimiciv_derived.ventilation AS p
WHERE p.ventilation_status = 'InvasiveVent'
), HFNC AS (
with a as(
SELECT DISTINCT v.stay_id, v.starttime, v.endtime, v.ventilation_status, RANK() OVER (PARTITION BY stay_id ORDER BY v.starttime ASC) AS rank
FROM physionet-data.mimiciv_derived.ventilation as v
WHERE v.ventilation_status = 'HFNC')
select stay_id, starttime, endtime, ventilation_status
from a
where rank =1
), HFNC_noDNR AS (
SELECT DISTINCT hf.stay_id
FROM HFNC as hf
EXCEPT DISTINCT SELECT dnr.stay_id FROM DNR as dnr
), Death AS (
SELECT DISTINCT icu.stay_id, deathtime
FROM physionet-data.mimiciv_hosp.admissions AS a
INNER JOIN physionet-data.mimiciv_icu.icustays AS icu ON icu.hadm_id = a.hadm_id
WHERE a.hospital_expire_flag = 1
), Hos_Death AS (
SELECT DISTINCT h.stay_id, deathtime
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN Death as d ON h.stay_id = d.stay_id
), Int_failure AS (
SELECT DISTINCT h.stay_id
FROM HFNC as h
INNER JOIN Intubation as Int ON h.stay_id = Int.stay_id
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
WHERE DATE_DIFF(h.endtime, Int.starttime, hour) > -24 and DATE_DIFF(h.endtime, Int.starttime, hour) < 0
), urine AS(
WITH a AS(
SELECT urineoutput, RANK() OVER (PARTITION BY stay_id ORDER BY charttime ASC) AS rank, stay_id
FROM physionet-data.mimiciv_derived.urine_output
)
SELECT urineoutput, rank, stay_id
FROM a
WHERE rank = 1
), fail AS(
WITH failure AS(
SELECT DISTINCT stay_id
FROM Hos_Death as h
UNION DISTINCT SELECT stay_id FROM Int_failure
)
SELECT failure.stay_id, case
WHEN failure.stay_id is not null then 1
END AS label
FROM failure
), fio2 as(
WITH a as(
SELECT h.stay_id, value, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_hosp.labevents as i ON icu.hadm_id = i.hadm_id
WHERE itemid in (50816)
)
select a.stay_id, a.value
FROM a
WHERE rank = 1
), o2flow as(
WITH a as
(
SELECT h.stay_id, o2_flow, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.oxygen_delivery as g ON h.stay_id = g.stay_id
)
SELECT a.stay_id, a.o2_flow
FROM a
WHERE rank = 1
),fio2_6hr as(
WITH a as(
SELECT h.stay_id, value, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_hosp.labevents as i ON icu.hadm_id = i.hadm_id
WHERE itemid in (50816) AND DATE_DIFF(i.CHARTTIME, h.starttime, hour)>=6 AND DATE_DIFF(i.CHARTTIME, h.starttime, hour)<=12
AND value is not null
)
SELECT a.stay_id, a.value
FROM a
WHERE rank = 1
), fio2_12hr as(
WITH a as(
SELECT h.stay_id, value, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_hosp.labevents as i ON icu.hadm_id = i.hadm_id
WHERE itemid in (50816) AND DATE_DIFF(i.CHARTTIME, h.starttime, hour)>=12
AND value is not null
)
SELECT a.stay_id, a.value
FROM a
WHERE rank = 1
),spo2_6hr as(
WITH a as(
SELECT h.stay_id, spo2, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=6 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND spo2 is not null
)
SELECT a.stay_id, a.spo2
FROM a
WHERE rank = 1
),spo2_12hr as(
WITH a as(
SELECT h.stay_id, spo2, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=12
AND spo2 is not null
)
SELECT a.stay_id, a.spo2
FROM a
WHERE rank = 1
),rr_6hr as(
WITH a as(
SELECT h.stay_id, resp_rate, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=6 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND resp_rate is not null
)
SELECT a.stay_id, a.resp_rate
FROM a
WHERE rank = 1
),rr_12hr as(
WITH a as(
SELECT h.stay_id, resp_rate, RANK() OVER (PARTITION BY h.stay_id ORDER BY charttime ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=12
AND resp_rate is not null
)
SELECT a.stay_id, a.resp_rate
FROM a
WHERE rank = 1
), death_time as(
SELECT DATE_DIFF(d.deathtime, h.starttime, hour) as label, h.stay_id
FROM HFNC as h
INNER JOIN Hos_Death as d
ON h.stay_id = d.stay_id
), race_gender as(
SELECT DISTINCT icu.stay_id,(CASE
WHEN a.race LIKE '%WHITE%' THEN 'WHITE'
WHEN a.race LIKE '%BLACK%' THEN 'BLACK'
WHEN a.race LIKE '%HISPANIC%' THEN 'HISPANIC'
WHEN a.race LIKE '%ASIAN%' THEN 'ASIAN'
ELSE 'OTHER'
END) AS race,
b.gender
FROM physionet-data.mimiciv_hosp.admissions AS a
INNER JOIN physionet-data.mimiciv_icu.icustays AS icu ON icu.hadm_id = a.hadm_id
INNER JOIN physionet-data.mimiciv_hosp.patients AS b ON icu.subject_id = b.subject_id
), so2min as(
WITH a as(
SELECT h.stay_id, so2, RANK() OVER (PARTITION BY h.stay_id ORDER BY so2 ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND so2 is not null
)
SELECT a.stay_id, a.so2
FROM a
WHERE rank = 1
), so2max as(
WITH a as(
SELECT h.stay_id, so2, RANK() OVER (PARTITION BY h.stay_id ORDER BY so2 DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND so2 is not null
)
SELECT a.stay_id, a.so2
FROM a
WHERE rank = 1
),po2min as(
WITH a as(
SELECT h.stay_id, po2, RANK() OVER (PARTITION BY h.stay_id ORDER BY po2 ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND po2 is not null
)
SELECT a.stay_id, a.po2
FROM a
WHERE rank = 1
),po2max as(
WITH a as(
SELECT h.stay_id, po2, RANK() OVER (PARTITION BY h.stay_id ORDER BY po2 DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND po2 is not null
)
SELECT a.stay_id, a.po2
FROM a
WHERE rank = 1
),pco2min as(
WITH a as(
SELECT h.stay_id, pco2, RANK() OVER (PARTITION BY h.stay_id ORDER BY pco2 ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND pco2 is not null
)
SELECT a.stay_id, a.pco2
FROM a
WHERE rank = 1
),pco2max as(
WITH a as(
SELECT h.stay_id, pco2, RANK() OVER (PARTITION BY h.stay_id ORDER BY pco2 DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND pco2 is not null
)
SELECT a.stay_id, a.pco2
FROM a
WHERE rank = 1
),ffmin as(
WITH a as(
SELECT h.stay_id, fio2, RANK() OVER (PARTITION BY h.stay_id ORDER BY fio2 ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND fio2 is not null
)
SELECT a.stay_id, a.fio2
FROM a
WHERE rank = 1
),ffmax as(
WITH a as(
SELECT h.stay_id, fio2, RANK() OVER (PARTITION BY h.stay_id ORDER BY fio2 DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND fio2 is not null
)
SELECT a.stay_id, a.fio2
FROM a
WHERE rank = 1
),phmin as(
WITH a as(
SELECT h.stay_id, ph, RANK() OVER (PARTITION BY h.stay_id ORDER BY ph ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND ph is not null
)
SELECT a.stay_id, a.ph
FROM a
WHERE rank = 1
),phmax as(
WITH a as(
SELECT h.stay_id, ph, RANK() OVER (PARTITION BY h.stay_id ORDER BY ph DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND ph is not null
)
SELECT a.stay_id, a.ph
FROM a
WHERE rank = 1
),hemomin as(
WITH a as(
SELECT h.stay_id, hemoglobin, RANK() OVER (PARTITION BY h.stay_id ORDER BY hemoglobin ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND hemoglobin is not null
)
SELECT a.stay_id, hemoglobin
FROM a
WHERE rank = 1
),hemomax as(
WITH a as(
SELECT h.stay_id, hemoglobin, RANK() OVER (PARTITION BY h.stay_id ORDER BY hemoglobin DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.bg as v ON icu.hadm_id = v.hadm_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>= -12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND hemoglobin is not null
)
SELECT a.stay_id, hemoglobin
FROM a
WHERE rank = 1
),tmin as(
WITH a as(
SELECT h.stay_id, temperature, RANK() OVER (PARTITION BY h.stay_id ORDER BY temperature ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=-12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND temperature is not null
)
SELECT a.stay_id, temperature
FROM a
WHERE rank =1
),tmax as(
WITH a as(
SELECT h.stay_id, temperature, RANK() OVER (PARTITION BY h.stay_id ORDER BY temperature DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=-12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND temperature is not null
)
SELECT a.stay_id, temperature
FROM a
WHERE rank =1
),hrmax as(
WITH a as(
SELECT h.stay_id, heart_rate , RANK() OVER (PARTITION BY h.stay_id ORDER BY heart_rate DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=-12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND heart_rate is not null
)
SELECT a.stay_id, heart_rate
FROM a
WHERE rank =1
), respmin as(
WITH a as(
SELECT h.stay_id, resp_rate , RANK() OVER (PARTITION BY h.stay_id ORDER BY resp_rate ASC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=-12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND resp_rate is not null
)
SELECT a.stay_id, resp_rate
FROM a
WHERE rank =1
),respmax as(
WITH a as(
SELECT h.stay_id, resp_rate , RANK() OVER (PARTITION BY h.stay_id ORDER BY resp_rate DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.vitalsign as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.charttime, h.starttime, hour)>=-12 AND DATE_DIFF(v.charttime, h.starttime, hour)<=12
AND resp_rate is not null
)
SELECT a.stay_id, resp_rate
FROM a
WHERE rank =1
), wt as(
WITH a as(
SELECT h.stay_id, weight , RANK() OVER (PARTITION BY h.stay_id ORDER BY v.endtime DESC) AS rank
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.weight_durations as v ON icu.stay_id = v.stay_id
WHERE DATE_DIFF(v.endtime, h.starttime, hour)>=-24 AND DATE_DIFF(v.endtime, h.starttime, hour)<=24
AND weight is not null
)
SELECT a.stay_id, weight
FROM a
WHERE rank =1
)
SELECT DISTINCT h.stay_id, h.starttime,age, rg.race, rg.gender, fio2.value as FiO2, o2flow.o2_flow, fio2_6hr.value as FiO2_6hr, fio2_12hr.value as FiO2_12hr,spo2_6hr.spo2 as SpO2_6hr, spo2_12hr.spo2 as SpO2_12hr,rr_6hr.resp_rate as resp_6hr, rr_12hr.resp_rate as resp_12hr, fail.label, so2min.so2 as so2_min, so2max.so2 as so2_max, po2min.po2 as po2_min, po2max.po2 as po2_max, pco2min.pco2 as pco2_min, pco2max.pco2 as pco2_max, ffmin.fio2 as fio2_min, ffmax.fio2 as fio2_max, phmin.ph as ph_min, phmax.ph as ph_max, hemomin.hemoglobin as hemoglobin_min, hemomax.hemoglobin as hemoglobin_max, tmin.temperature as temperature_min, tmax.temperature as temperature_max, hrmax.heart_rate as heart_rate_max, respmin.resp_rate as resp_rate_min,respmax.resp_rate as resp_rate_max, urineoutput, height, wt.weight
FROM HFNC as h
INNER JOIN HFNC_noDNR as HnD ON h.stay_id = HnD.stay_id
INNER JOIN physionet-data.mimiciv_derived.icustay_detail as icu ON h.stay_id = icu.stay_id
INNER JOIN physionet-data.mimiciv_derived.age as a ON icu.hadm_id = a.hadm_id
INNER JOIN race_gender as rg ON h.stay_id = rg.stay_id
LEFT OUTER JOIN urine as b ON h.stay_id = b.stay_id
LEFT OUTER JOIN tmin on h.stay_id = tmin.stay_id
LEFT OUTER JOIN tmax on h.stay_id = tmax.stay_id
LEFT OUTER JOIN hrmax on h.stay_id = hrmax.stay_id
LEFT OUTER JOIN o2flow on h.stay_id= o2flow.stay_id
LEFT OUTER JOIN fail ON h.stay_id = fail.stay_id
LEFT OUTER JOIN fio2 as fio2 ON h.stay_id = fio2.stay_id
LEFT OUTER JOIN fio2_6hr as fio2_6hr ON h.stay_id = fio2_6hr.stay_id
LEFT OUTER JOIN fio2_6hr as fio2_12hr ON h.stay_id = fio2_12hr.stay_id
LEFT OUTER JOIN spo2_6hr as spo2_6hr ON h.stay_id = spo2_6hr.stay_id
LEFT OUTER JOIN spo2_12hr as spo2_12hr ON h.stay_id = spo2_12hr.stay_id
LEFT OUTER JOIN rr_6hr as rr_6hr ON h.stay_id = rr_6hr.stay_id
LEFT OUTER JOIN rr_12hr as rr_12hr ON h.stay_id = rr_12hr.stay_id
LEFT OUTER JOIN death_time as death_time ON h.stay_id = death_time.stay_id
LEFT OUTER JOIN so2min ON h.stay_id = so2min.stay_id
LEFT OUTER JOIN so2max ON h.stay_id = so2max.stay_id
LEFT OUTER JOIN po2min ON h.stay_id = po2min.stay_id
LEFT OUTER JOIN po2max ON h.stay_id = po2max.stay_id
LEFT OUTER JOIN pco2min ON h.stay_id = pco2min.stay_id
LEFT OUTER JOIN pco2max ON h.stay_id = pco2max.stay_id
LEFT OUTER JOIN ffmin ON h.stay_id = ffmin.stay_id
LEFT OUTER JOIN ffmax ON h.stay_id = ffmax.stay_id
LEFT OUTER JOIN phmin ON h.stay_id = phmin.stay_id
LEFT OUTER JOIN phmax ON h.stay_id = phmax.stay_id
LEFT OUTER JOIN hemomin ON h.stay_id = hemomin.stay_id
LEFT OUTER JOIN hemomax ON h.stay_id = hemomax.stay_id
LEFT OUTER JOIN respmin ON h.stay_id = respmin.stay_id
LEFT OUTER JOIN respmax ON h.stay_id = respmax.stay_id
LEFT OUTER JOIN physionet-data.mimiciv_derived.height as hi ON h.stay_id = hi.stay_id
LEFT OUTER JOIN wt ON h.stay_id = wt.stay_id
where death_time.label <=720 or death_time.label is null
