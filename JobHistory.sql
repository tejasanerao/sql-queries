;WITH JobSummary AS (
    -- Get job-level summary rows (one per run)
    SELECT 
        j.job_id,
        j.name AS JobName,
        h.instance_id,
		CAST(STUFF(STUFF(CAST(h.run_date AS CHAR(8)), 5, 0, '-'), 8, 0, '-') + ' ' 
		+ STUFF(STUFF(RIGHT('000000' + CAST(h.run_time AS VARCHAR(6)), 6),3, 0, ':'),6, 0, ':') AS DATETIME)
		AS JobStartTime,
        DATEADD(SECOND,
            (h.run_duration / 10000 * 3600) + ((h.run_duration % 10000) / 100 * 60) + (h.run_duration % 100),
			CAST(STUFF(STUFF(CAST(h.run_date AS CHAR(8)), 5, 0, '-'), 8, 0, '-') + ' ' 
		+ STUFF(STUFF(RIGHT('000000' + CAST(h.run_time AS VARCHAR(6)), 6),3, 0, ':'),6, 0, ':') AS DATETIME)
        ) AS JobEndTime,
        (h.run_duration / 10000 * 3600) + ((h.run_duration % 10000) / 100 * 60) + (h.run_duration % 100) AS TotalJobSeconds,
        ROW_NUMBER() OVER (PARTITION BY j.job_id ORDER BY 
		CAST(STUFF(STUFF(CAST(h.run_date AS CHAR(8)), 5, 0, '-'), 8, 0, '-') + ' ' 
		+ STUFF(STUFF(RIGHT('000000' + CAST(h.run_time AS VARCHAR(6)), 6),3, 0, ':'),6, 0, ':') AS DATETIME) DESC) AS RunNumber
    FROM msdb.dbo.sysjobs j
    INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
    WHERE j.name = 'FileLoadAndProcess'       -- change job name as needed
      AND h.step_id = 0         -- job summary rows only
),
StepsRaw AS (
    -- All step-level rows (each with its step start time)
    SELECT 
        j.job_id,
        j.name AS JobName,
        h.step_id,
        h.step_name,
        msdb.dbo.agent_datetime(h.run_date, h.run_time) AS StepStartTime
    FROM msdb.dbo.sysjobs j
    INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
    WHERE j.name = 'FileLoadAndProcess'
      AND h.step_id > 0
),
StepsPerRun AS (
    -- Match step rows to the job run (by time window), then compute next-step start with LEAD()
    SELECT
        js.job_id,
        js.JobName,
        js.JobStartTime,
        js.JobEndTime,
        s.step_id,
        s.step_name,
        s.StepStartTime,
        LEAD(s.StepStartTime) OVER (PARTITION BY js.JobStartTime ORDER BY s.StepStartTime) AS NextStepStart
    FROM JobSummary js
    INNER JOIN StepsRaw s
        ON s.StepStartTime >= js.JobStartTime
       AND s.StepStartTime < js.JobEndTime  -- step must start during the job run
),
StepDurations AS (
    -- Calculate actual duration for each matched step:
    -- duration = next step start - this step start, else job end - this step start
    SELECT
        job_id,
        JobName,
        JobStartTime,
        JobEndTime,
        step_id,
        step_name,
        StepStartTime,
        ISNULL(NextStepStart, JobEndTime) AS EffectiveEndTime,
        DATEDIFF(SECOND, StepStartTime, ISNULL(NextStepStart, JobEndTime)) AS StepSeconds
    FROM StepsPerRun
),
Aggregated AS (
    -- Aggregate step durations by job run and step-name patterns
    SELECT
        js.JobName,
        js.JobStartTime,
        js.JobEndTime,
        js.TotalJobSeconds,
        ISNULL(SUM(CASE WHEN sd.step_name LIKE '%File Load%' THEN sd.StepSeconds END), 0) AS Step1Seconds,
        ISNULL(SUM(CASE WHEN sd.step_name LIKE '%Process File%' THEN sd.StepSeconds END), 0) AS Step2Seconds
    FROM JobSummary js
    LEFT JOIN StepDurations sd
        ON sd.JobStartTime = js.JobStartTime  -- matched runs
    GROUP BY js.JobName, js.JobStartTime, js.JobEndTime, js.TotalJobSeconds
)
SELECT
    a.JobName,
    a.JobStartTime,
    a.JobEndTime,

    -- format total job duration as HH:MM:SS
    RIGHT('0' + CAST(a.TotalJobSeconds / 3600 AS VARCHAR),2) + ':' +
    RIGHT('0' + CAST((a.TotalJobSeconds % 3600) / 60 AS VARCHAR),2) + ':' +
    RIGHT('0' + CAST(a.TotalJobSeconds % 60 AS VARCHAR),2) AS TotalJobDuration,

    -- Step1 (File Load) duration
    RIGHT('0' + CAST(a.Step1Seconds / 3600 AS VARCHAR),2) + ':' +
    RIGHT('0' + CAST((a.Step1Seconds % 3600) / 60 AS VARCHAR),2) + ':' +
    RIGHT('0' + CAST(a.Step1Seconds % 60 AS VARCHAR),2) AS Step1Duration,

    -- Step2 (Process File) duration
    RIGHT('0' + CAST(a.Step2Seconds / 3600 AS VARCHAR),2) + ':' +
    RIGHT('0' + CAST((a.Step2Seconds % 3600) / 60 AS VARCHAR),2) + ':' +
    RIGHT('0' + CAST(a.Step2Seconds % 60 AS VARCHAR),2) AS Step2Duration,

    -- count of rows inserted into ABC during job window
    COUNT(abc.ID) AS RecordsInserted
FROM Aggregated a
INNER JOIN ABC abc ON abc.CreatedDate BETWEEN a.JobStartTime AND a.JobEndTime
GROUP BY
    a.JobName, a.JobStartTime, a.JobEndTime, a.TotalJobSeconds, a.Step1Seconds, a.Step2Seconds
ORDER BY a.JobStartTime DESC;
