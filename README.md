# System Engineer Assessment

## Scenario
You are tasked with designing a production-ready data pipeline that extracts data from a public API endpoint, applies necessary transformations, and loads the results into a database table. The solution should be suitable for deployment in an operational environment.

This assessment is conducted through GitHub. You are expected to fork the repository, implement your solution in your fork, and submit via a pull request.


### Production Principles
Our philiosophy of "production" is that systems are expected to operate reliably, predictably, and without manual intervention. This section outlines the foundational principles that guide how we design, deploy, and operate production workloads.

* automation
* reproducibility and portability
* lightweight (minimal dependencies)
* scalable
* documented


### Pipeline Principles
At Halifax Stanfield, production data pipelines are built using a pipeline-as-code approach. Our principles emphasize maintainability, observability, and reliability, treating data workflows as engineered systems rather than adhoc scripts. Additional information and examples are available <a href="https://whipson.github.io/maestro/index.html" target="_blank">here</a>.


### Production Environment
Although **you are not responsible for deploying or orchestrating the pipeline**, your solution must be able to executed on a production-grade server. You can assume the following about the production server.

* Operating system is Linux
* It will have access to the internet
* Any system libraries or software your solution requires can be installed via standard linux commands provided that these steps are documented and follow our production principles


### Assessment Instructions
You are asked to develop a pipeline that will extract data from a public climate API endpoint, transform the incoming data, and load it to an existing SQLite table. You should assume this pipeline would output data into the existing table in a continuous, scheduled manner. 

The endpoint you will extract from is the Geomet hourly climate API endpoint: `https://api.weather.gc.ca/collections/climate-hourly/items`

Here are the detailed requirements of your pipeline:

* Extract hourly climate data for the latest available full date and where the CLIMATE_IDENTIFIER is `8202251`
* Match the incoming data with the schema for the existing `yhz_db.hiaa_geomet_hourly` table
* Perform a transformation on numeric columns converting NA/NULL values to 0
* Add a new column called `insert_time` corresponding to the current UTC timestamp (i.e., the time the data is being inserted)
* Load the transformed data into the existing SQLite table `yhz_db.hiaa_geomet_hourly`.


### Deliverable
The deliverable is a production-oriented data pipeline that satisfies the scenario and aligns with production principles. The submission should include:

- Code required to extract, transform, and load data from the API into a database table
- README.md describing the solution and how to run it in production (you should replace this README.md with your own)
- If applicable, configuration and system instructions necessary to execute the solution
- If applicable, packaging or runtime artifacts required to run the pipeline in a clean environment

The solution should be complete enough to evaluate structure, production thinking, and operational readiness, while remaining proportionate to the scope of the assignment.


### Assessment Evaluation Rubric
The following are the independent evaluation criteria for the assessment.

| **Evaluation Area** | **Value** |
| ------- | :----: |
| Pipeline code | 50% |
| Production principles | 30% |
| Runable | 20% |


#### Exclusions from Evaluation

The assessment is designed to evaluate engineering judgment and production-oriented thinking. The following elements will not factor into scoring:

- *Algorithmic efficiency or optimization*  
Performance tuning is not a focus of this exercise
- *Code style or elegance*  
Stylistic preferences or personal conventions will not be evaluated
- *Perfect execution*  
A non-executable submission will not automatically result in failure. Clear intent, sound structure, and well-documented reasoning will still be considered
- *Programming language choice*  
Any language or framework may be used


Candidates are also not expected to invest effort in the following areas unless directly relevant to your design:

- CI/CD pipeline implementation
- Enterprise-grade security hardening
- Extensive test suites or performance benchmarking
- Infrastructure provisioning beyond what is required to demonstrate execution
- UI development or dashboards

Submissions should remain proportionate to the scope of the scenario. Depth of engineering thinking is valued more than breadth of additional features.


#### Pipeline Code
When reviewing the pipeline code, evaluation will focus on the following:

- clarity of intent
- configuration and parameterization
- pipeline logic
- structure and organization


#### Production Principles
Evaluation will focus on how well the solution reflects production-oriented thinking, including:

- automation
- dependency management
- operational readiness
- reproducibility and portability


#### Runable
Evaluation will focus on how easily the solution can be executed in a clean environment, including:

- Low-friction execution
- Self-contained packaging
- System instructions


## How to Submit
This assessment is to be completed using GitHub as the primary collaboration and submission mechanism. Candidates are expected to fork this repository to your own GitHub account and implement your solution within that fork.

Access to the forked repository must be granted to the following GitHub users:

- Will Hipson (`whipson`)
- Ryan Garnett (`ryangarnett`)

Upon completion, the assessment must be submitted as a pull request from the candidate’s fork, directed to ryangarnett for review.

Any questions related to the assessment should be raised as a GitHub Issue within the forked repository. Please tag both ryangarnett and whipson to ensure visibility and response.

***The use of Generative AI is acceptable for this assignment***
