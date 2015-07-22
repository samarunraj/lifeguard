This document will discuss the features that describe the scope of this project.

# Introduction #

This project will likely be built in phases. The first phase is what we'll implement first and the feature set described will likely be accurate. Features listed for subsequent phases are speculative and should be treated as such.


# Phase 1 #

This phase will focus on the core pool management features. It will focus on building a strong, reliable core that will be the basis of future releases. The following are items that are either features or descriptions of behavior;
  * Multiple server pools can be managed at once.
  * Approximate number of messages features in SQS will be used to estimate server needs in each pool.
  * The pool manager will be deployable as either a standalone application or in a .war file
  * Service instances will report busy/idle status to allow for a better assessment of server utilization
  * Messages passed will be in XML format with a schema to be defined

# Phase 2 #

This phase will focus on UI, with tools to manage and monitor the pools.