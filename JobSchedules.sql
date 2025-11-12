SELECT 
    j.name AS JobName,
    s.name AS ScheduleName,
    CASE s.enabled 
        WHEN 1 THEN 'Enabled' 
        ELSE 'Disabled' 
    END AS ScheduleStatus,
    CASE sc.freq_type
        WHEN 1 THEN 'One Time'
        WHEN 4 THEN 'Daily'
        WHEN 8 THEN 'Weekly'
        WHEN 16 THEN 'Monthly'
        WHEN 32 THEN 'Monthly, Relative to Frequency Interval'
        WHEN 64 THEN 'When SQL Server Agent Starts'
        WHEN 128 THEN 'When CPU Idle'
        ELSE 'Other'
    END AS FrequencyType,
    CASE 
        WHEN sc.freq_type = 4 THEN 
            'Every ' + CAST(sc.freq_interval AS VARCHAR(10)) + ' day(s)'
        WHEN sc.freq_type = 8 THEN 
            'Every ' + CAST(sc.freq_recurrence_factor AS VARCHAR(10)) + 
            ' week(s) on ' + 
            CASE sc.freq_interval
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 4 THEN 'Tuesday'
                WHEN 8 THEN 'Wednesday'
                WHEN 16 THEN 'Thursday'
                WHEN 32 THEN 'Friday'
                WHEN 64 THEN 'Saturday'
                ELSE 'Multiple Days'
            END
        WHEN sc.freq_type = 16 THEN 
            'Day ' + CAST(sc.freq_interval AS VARCHAR(10)) + 
            ' of every ' + CAST(sc.freq_recurrence_factor AS VARCHAR(10)) + ' month(s)'
        ELSE ''
    END AS FrequencyDescription,
    RIGHT('000000' + CAST(sc.active_start_time AS VARCHAR(6)), 6) AS StartTimeRaw,
    STUFF(STUFF(RIGHT('000000' + CAST(sc.active_start_time AS VARCHAR(6)), 6),3,0,':'),6,0,':') AS StartTimeFormatted,
    CASE 
        WHEN s.enabled = 1 AND j.enabled = 1 THEN 'Active' 
        ELSE 'Inactive' 
    END AS JobScheduleStatus,
    js.next_run_date,
    js.next_run_time,
    CASE 
        WHEN js.next_run_date = 0 THEN NULL
        ELSE
            CAST(
                STUFF(STUFF(CAST(js.next_run_date AS CHAR(8)),5,0,'-'),8,0,'-') + ' ' +
                STUFF(STUFF(RIGHT('000000' + CAST(js.next_run_time AS VARCHAR(6)),6),3,0,':'),6,0,':')
                AS DATETIME
            )
    END AS NextRunDateTime
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
INNER JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
INNER JOIN msdb.dbo.sysschedules sc ON js.schedule_id = sc.schedule_id
WHERE j.enabled = 1
ORDER BY j.name, s.name;
