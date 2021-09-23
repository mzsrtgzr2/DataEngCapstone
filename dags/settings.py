

JOB_FLOW_OVERRIDES = {
    "Name": "COVID19-OPERATIONS",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

DATA_SOURCES_MAP = {
    'activity': (
        'https://query.data.world/s/nlzvfwgrwtkdbpmgzrpzwtyns44sjq',
        'activity.csv'
    ),
    'population': (
        'https://query.data.world/s/n2fh33k3646tkphijywr3tw5eeppyk',
        'population.csv'
    ),
    'vaccinations_global': (
        'https://raw.githubusercontent.com/'
        'govex/COVID-19/master/data_tables/vaccine_data/global_data/'
        'time_series_covid19_vaccine_global.csv',
        'vaccinations_global.csv'
    ),
    'vaccinations_usa_people': (
        'https://raw.githubusercontent.com/'
        'govex/COVID-19/master/data_tables/vaccine_data/us_data/time_series/'
        'vaccine_data_us_timeline.csv',
        'vaccinations_usa_people.csv'
    ),
    'vaccinations_usa_doeses': (
        'https://raw.githubusercontent.com/'
        'govex/COVID-19/master/data_tables/vaccine_data/us_data/time_series/'
        'vaccine_data_us_timeline.csv',
        'vaccinations_usa_doeses.csv'
    )
}