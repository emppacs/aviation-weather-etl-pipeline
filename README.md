# System Engineer Assessment
## Scenario
You are tasked with designing and implementing a production-ready data pipeline that extracts data from a provided API endpoint, applies necessary transformations, and loads the results into a database table. The solution should be suitable for deployment within an operational environment and will be evaluated within our production infrastructure prior to formal release.

This assessment is conducted through GitHub. You are expected to fork the repository, implement their solution within their fork, and submit their work through a pull request.



### Production Principles
Our philiosophy of "production" is that systems are expected to operate reliably, predictably, and with minimal manual intervention. This section outlines the foundational principles that guide how we design, deploy, and operate production workloads. These principles reflect our emphasis on engineered systems over ad hoc solutions, following a DataOps model, with attention to operational resilience, maintainability, and long-term sustainability.

- automation
- code base ("as-code")
- lite weight
- minimal dependencies
- monitoring
- notifications
- portable
- reproducible
- scalable
- system instructions



### Pipeline Principles
At Halifax Stanfield, production data pipelines are built using a pipeline-as-code approach. Pipelines are defined, version-controlled, and deployed as code, enabling transparency, repeatability, and peer review. This approach supports modular design, explicit dependencies, environment portability, and automated execution. Our principles emphasize maintainability, observability, and reliability, treating data workflows as engineered systems rather than ad hoc scripts. Additional information and examples are available [here](https://whipson.github.io/maestro/index.html).

<br>


## Assessment Objective
This assessment is to be completed using GitHub as the primary collaboration and submission mechanism. Candidates are expected to fork this repository to their own GitHub account and implement their solution within that fork.

Access to the forked repository must be granted to the following GitHub users:

- Will Hipson (`whipson`)
- Ryan Garnett (`ryangarnett`)

Upon completion, the assessment must be submitted as a pull request from the candidate’s fork, directed to ryangarnett for review.

Any questions related to the assessment should be raised as a GitHub Issue within the forked repository. Please tag both ryangarnett and whipson to ensure visibility and response.

<br>


### Production Environment
The production environment should be considered in the development of the data pipeline: 

| **System Component** | **Description** |
| ------- | --------------- |
| Operating system: | Linux (RHEL) |
| Internet access: | True |
| Technologies: | Runtime: R 4.3<br>Docker |

<br>


### Pipeline Description
The following are the requirements and assumptions to be used in the development of the data pipeline:

- Code base ETL (extract, transform, load) data pipeline
  - ETL pipeline must:
      - Extract
        - from API endpoint (Geomet API Endpoint: `https://api.weather.gc.ca/collections/climate-hourly/items`)
        - filter on climate id: `8202251`
      - Transform
        - match database schema
          - based on `yhz_db.hiaa_geomet_hourly`
        - convert `NA`
          - numeric columns to `0`
        - add `insert_time` as UTC
          - format `YYYY-MM-DD HH:MM:SS`
      - Load
        - insert into table (`hiaa_geomet_hourly`) SQLite database (`yhz_db.sqlite`)
- Pipeline runs daily
  - Note: the code/pipeline does not need to handle the orchestration/scheduling of the pipeline



### Deliverable
The deliverable is a production-oriented data pipeline implementation that satisfies the scenario described above and aligns with the stated pipeline and production principles. The submission should include:

- Source code required to extract, transform, and load data from the API into a database table
- Configuration and system instructions necessary to execute the solution
- Any packaging or runtime artifacts required to run the pipeline in a clean environment
- A concise README within the fork describing assumptions, design decisions, and how to run the solution

The solution should be complete enough to evaluate structure, production thinking, and operational readiness, while remaining proportionate to the scope of the assignment.

<br>


### Assessment Evaluation Rubric
The following are the independent evaluation criteria for the assessment.

| **Evaluation Area** | **Value** |
| ------- | :----: |
| Pipeline code | 50% |
| Production principles | 30% |
| Runable | 20% |

<br>


#### Exclusions from Evaluation

The assessment is designed to evaluate engineering judgment and production-oriented thinking. The following elements will not factor into scoring:

- *Algorithmic efficiency or optimization*  
Performance tuning is not a focus of this exercise
- *Code style or elegance*  
Stylistic preferences, personal conventions, or subjective notions of “clean code” will not be evaluated
- *Perfect execution*  
A non-executable submission will not automatically result in failure. Clear intent, sound structure, and well-documented reasoning will still be considered
- *Programming language choice*  
Any language or framework may be used

<br>

Candidates are also not expected to invest effort in the following areas unless directly relevant to their design:

- CI/CD pipeline implementation
- Enterprise-grade security hardening
- Extensive test suites or performance benchmarking
- Infrastructure provisioning beyond what is required to demonstrate execution
- UI development or dashboards


Submissions should remain proportionate to the scope of the scenario. Depth of engineering thinking is valued more than breadth of additional features.


<br>


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
- reproducibility and protability


#### Runable
Evaluation will focus on how easily the solution can be executed in a clean environment, including:

- Low-friction execution
- Self-contained packaging
- System instructions

<br>

***The use of Generative AI is acceptable for this assignment***
