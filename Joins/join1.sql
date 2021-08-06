from (
        select distinct jeditaskid,
            sum(
                Case
                    When jobstatus in ('failed', 'finished') THEN((endtime - starttime) * 86400 * actualcorecount)
                END
            ) as wallcore_all,
            sum(
                Case
                    When jobstatus = 'finished' THEN((endtime - starttime) * 86400 * actualcorecount)
                END
            ) as wallcore_finished,
            sum(
                Case
                    When jobstatus = 'failed' THEN((endtime - starttime) * 86400 * actualcorecount)
                END
            ) as wallcore_failed,
            sum(
                Case
                    When jobstatus in ('failed', 'finished') THEN hs06sec
                END
            ) as hs06sec_all,
            sum(
                Case
                    When jobstatus = 'finished' THEN hs06sec
                END
            ) as hs06sec_finished,
            sum(
                Case
                    When jobstatus = 'failed' THEN hs06sec
                END
            ) as hs06sec_failed
        from atlas_pandaarch.jobsarchived
        where modificationtime < '2021-01-01'
            and modificationtime > '2020-01-01'
            and processingtype = 'evgen'
        group by jeditaskid
    ) jobs hs06sec_failed,
    hs06sec_finished wallcore_finished wallcore_failed,
    input_events,
    output_events
select tasks.reqid as request,
    tasks.jeditaskid as task,
    tasks.status,
    dataset_input,
    dataset_output,
    events_input,
    events_output,
    hs06sec_all,
    hs06sec_finished,
    hs06sec_failed,
    wallcore_all,
    wallcore_finished,
    wallcore_failed (
        select task.reqid,
            task.jeditaskid,
            task.status,
            input_dataset.datasetname as dataset_input,
            output_dataset.datasetname as dataset_output,
            input_dataset.nevents as events_input,
            output_dataset.nevents as events_output
        from atlas_panda.jedi_tasks task,
            atlas_panda.jedi_datasets input_dataset,
            atlas_panda.jedi_datasets output_dataset
        where task.jeditaskid = input_dataset.jeditaskid
            and task.jeditaskid = output_dataset.jeditaskid
            and task.processingtype = 'evgen'
            and task.starttime > '2020-01-01'
            and task.endtime < '2021-01-01'
            and input_dataset.type = 'pseudo_input'
            and input_dataset.masterid is null
            and output_dataset.type = 'output'
            and output_dataset.masterid is null
    ) tasks on jobs.jeditaskid = tasks.jeditaskid find all tasks with * processingtype = 'evgen' find only ones that have in table jedi_datasets * input_dataset.type = 'pseudo_input' * output_dataset.type = 'output'