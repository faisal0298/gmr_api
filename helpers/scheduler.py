from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from typing import Union
import apscheduler


class BackgroundTaskHandler:
    def __init__(self) -> None:
        
        from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
        # client = MongoClient(
        #     host="mongodb://{}:{}@{}:{}/?directConnection=true&ssl=false".format(
        #         config.DATABASE_USERNAME,
        #         config.DATABASE_PASSWORD,
        #         config.DATABASE_HOST,
        #         27017,
        #         # config.DATABASE_AUTHENTICATION_SOURCE,
        #         # config.DATABASE_REPLICA_SET_NAME,
        #     )
        # )
        self.scheduler = BackgroundScheduler(
            daemon=False,
            executors={
                "threadpool": ThreadPoolExecutor(max_workers=9),
                "processpool": ProcessPoolExecutor(max_workers=3),
            },
            timezone="utc",
            job_defaults={
                "coalesce": True,
                "max_instances": 3,
                "misfire_grace_time": 60 * 60 * 24,
            },
        )
        # self.scheduler.add_jobstore(
        #     MongoDBJobStore(
        #         client=client,
        #         database="schedule_management_db",
        #         collection="jobs",
        #     ),
        #     alias="default",
        # )
        # try:
        self.scheduler.start()
        # except errors.NotPrimaryError as e:
        #     console_logger.debug(e)
            # os.kill(os.getpid(), signal.SIGINT)

        # self.scheduler.add_listener(self.event_listener, EVENT_JOB_ERROR)

    # def event_listener(self, event):
    #     console_logger.debug("The job crashed")
    #     console_logger.debug(event.exception)

    def get_job(self, task_name: str):
        try:
            return self.scheduler.get_job(task_name)
        except apscheduler.jobstores.base.JobLookupError:
            return None

    def run_job(
        self,
        task_name: str,
        func,
        func_args: Union[list, tuple, None] = None,
        func_kwargs: Union[dict, None] = None,
        trigger: str = "cron",
        **kwargs
    ):
        self.scheduler.add_job(
            func=func,
            replace_existing=True,
            trigger=trigger,
            args=func_args,
            kwargs=func_kwargs,
            id=task_name,
            timezone="utc",
            executor="threadpool",
            **kwargs
        )
        return {"detail": "success"}

    def pause_job(self, task_name: str):
        """
        pause job

        Parameters:
            task_name (str)
        Returns:
            None
        """
        self.scheduler.pause_job(job_id=task_name)

    def resume_job(self, task_name: str):
        """
        resume job

        Parameters:
            task_name (str)
        Returns:
            None
        """
        self.scheduler.resume_job(job_id=task_name)

    def reschedule_job(self, task_name: str, trigger: str, **kwargs):
        """
        reschedule job \n
        trigger: date, interval, cron \n
        ** kwargs for trigger type
                \n
                1. Date:
                            \a run_date (datetime|str) - the date/time to run the job at \n
                            \a timezone (datetime.tzinfo|str) - time zone for run_date if it doesn't have one already
                \n
                2. Interval:
                            \a weeks (int) - number of weeks to wait \n
                            \a days (int) - number of days to wait \n
                            \a hours (int) - number of hours to wait \n
                            \a minutes (int) - number of minutes to wait \n
                            \a seconds (int) - number of seconds to wait \n
                            \a start_date (datetime|str) - starting point for the interval calculation \n
                            \a end_date (datetime|str) - latest possible date/time to trigger on \n
                            \a timezone (datetime.tzinfo|str) - time zone to use for the date/time calculations \n
                            \a jitter (int|None) - delay the job execution by jitter seconds at most \n
                \n
                3. Cron:
                            \a year (int|str) - 4-digit year \n
                            \a month (int|str) - month (1-12) \n
                            \a day (int|str) - day of month (1-31) \n
                            \a week (int|str) - ISO week (1-53) \n
                            \a day_of_week (int|str) - number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun) \n
                            \a hour (int|str) - hour (0-23) \n
                            \a minute (int|str) - minute (0-59) \n
                            \a second (int|str) - second (0-59) \n
                            \a start_date (datetime|str) - earliest possible date/time to trigger on (inclusive) \n
                            \a end_date (datetime|str) - latest possible date/time to trigger on (inclusive) \n
                            \a timezone (datetime.tzinfo|str) - time zone to use for the date/time calculations (defaults to scheduler timezone) \n
                            \a jitter (int|None) - delay the job execution by jitter seconds at most \n
        """
        self.scheduler.reschedule_job(job_id=task_name, trigger=trigger, **kwargs)

    def remove_job(self, task_name: str):
        """
        remove job

        Parameters:
            task_name (str)
        Returns:
            None
        """
        self.scheduler.remove_job(job_id=task_name)

    def modify_job(self, task_name: str, **kwargs):
        """
        modify job: change job properties e.g. function
        **kwargs: func, args, max_instances, name
        """
        self.scheduler.modify_job(job_id=task_name, **kwargs)


backgroundTaskHandler = BackgroundTaskHandler()
