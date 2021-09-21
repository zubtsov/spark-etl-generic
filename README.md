# The better way of writing Spark jobs
This is a generic framework for writing Apache Spark jobs in Java, Scala & Python (SQL + XML?).
Similar frameworks exist, but they have different abstraction level for the graph edges (e.g. job or intermediate table states)

# The goals are
1. To speed up the development & reduce the amount of boilerplate/plumbing code. 
What if we took the idea of the dependency injection (like in Spring/Guice etc.)
and the idea of having standard project structure (like in Maven) 
and apply it to the ETL pipelines domain?
This would allow us to get rid of manual dependency management between tables 
and to avoid re-inventing the wheel (code architecture/structure) each time.
Another side effect of automatic dependency management is increased parallelism 
i.e. independent tables are evaluated concurrently.
2. To improve code quality & maintainability. 
Each time a new data project/stream starts, developers may invent a new code architecture.
The code quality on such projects is often poor because there's a little time to properly design and
structure it. The main goal is to produce the data in the shortest time possible.
Another reason is that it's hard to write the code in OOP paradigm, because
you don't have tangible real-world objects which could be mapped to the object model,
so you have to come up with some clumsy artificial concepts.
Functional programming is not so widespread and easy to grasp.

# The main ideas are
1. To give a developer pre-defined object model which is
   1. simple & clear enough to learn & understand 
   2. flexible enough to cover most of the real-world scenarios
   3. customizable to cover non-standard cases

   Enforcing such standard model allows developers to build pipelines faster and to switch between projects easier.
2. To allow a developer to create just the bare minimum of configuration and logic needed.

# Implementation details and other requirements
1. To resolve dependencies between tables either at runtime (easier to implement, leverages reflection) 
or compile-time (harder to implement, requires code generation). Dependencies (tables) are resolved by name.
2. There are 2 kinds of tables: produced within the pipeline (internal) and existing independently (external).
3. There should be support for multiple pre-defined write strategies: table overwrite, partition overwrite, merge, append and custom strategy.
4. There should be support fur multiple pre-defined read strategies: full, incremental, rolling period and custom strategy.
5. There should be support for both stateless & stateful jobs.
6. There should be a way to pass configuration & contextual information to the job and within the job between different parts of application.
Configuration can be environment-specific (DEV/QA/UAT/PROD).
7. The framework must not impose any restrictions or limitations in regard to performance tuning & optimization comparing to the plain Spark applications.
8. Debugging and monitoring must be convenient and easy as well as writing tests.
9. TBD

# Tricky cases & advanced features
1. The case, when a table depends on its content after the last pipeline run.
2. Some tables might be optional and their presence affects the computations logic. Some columns might be optional e.g. 
table may not have a constant schema, so it can vary from one run to another. 
3. Sometimes, the number of input tables is not known in advance, but only at runtime. Most likely, they have some
pattern associated with paths/names etc. Regular expressions might be useful. It seems to be a completely different model
when external tables have to be scanned before the job starts, so instead of pulling external tables, we have to push them
to transformers. Such jobs are some kind of generic ingestion.
4. Annotations are convenient. But we need to be able to define computation graph dynamically at runtime.
5. The ability to partially customize the job. So that you can depend on the customized version in your ETLs 
to share some common project-specific logic/code across pipelines.
6. Conditional table skipping.
7. Allowing SQL's as table builders can be tricky if we don't want to supply their dependencies explicitly
8. Think about the case when we need to open multiple connections from worker nodes and reuse them. 
9. TBD

# Code examples
[Simple job example](src/test/scala/org/zubtsov/spark/etl1)

# Further development & evolution plan
Accumulate real-world use cases & scenarios to extend the functionality and improve flexibility 
without compromising the existing functionality.
Each case must be described & graphically represented.

# Open questions
1. Is it responsibility of a table to know where to be stored & where to be read from? Or readers/writers to know what tables
they can read & write? Or we can allow both...(not so easy)
2. Should table names be of a String type (name only) or we need to introduce some hierarchies/namespaces/additional attributes 
to identify a table at the framework level?
3. Do we need separate concepts for reading external & writing tables or only one for both? Think about benefits of each approach.
Should we force a developer to separate code for reading and writing? Do they need a shared state?
4. Is it general enough to assume that the builder can produce only one table?
5. How to incorporate & organize data quality validation?
6. Do we need the ability to create pre-/post- table/job actions?
7. Annotations + method signature conventions vs annotations + interfaces/abstract classes to define external tables?
8. Perhaps, we can leverage existing DI frameworks/containers if we use class fields instead of method parameters.

# Check out other frameworks
1. https://github.com/SETL-Framework/setl
2. https://github.com/airbnb/sputnik
3. https://github.com/apache/hudi
5. https://github.com/typelevel/frameless
N. https://github.com/pawl/awesome-etl

# TODO
Rename project/repository? TableNetSpark/TableWebSpark; maybe spark-table-graph/spark-table-dag
Empty strings as a default values for annotation parameters look a bit clumsy...
Configure Checkstyle, PMD, FindBugs
Support SQL (+ analyze it's dependencies to resolve them automatically)
Fix packages names
We should have some interfaces to ease default implementations support