Design details

# Introduction #

This document will be a collection of notes about the design of lifeguard. It is broken into phases, much like the scope document.


# Phase 1 #

This is a list of design details that will go into the core pool manager
  * XML messages will be sent for work requests as well as for status logging.
  * XML messages will be sent for server instance busy/idle status
  * A logging interface will define an API for a status persistence layer. Initially an IBATIS implementation will be written for MySQL, but certainly others could replace this implementation.
  * A service list and workflow description mechanism needs to be defined. Could simply do this in MySQL, or could also write an interface to abstract this out and allow for different implementations.
  * Should we use Spring to plug the components together?
  * There will be an API to access the pool manager for monitoring and param setting. A WS layer could be built onto this in the future, but this allows either a web app or a standalone app to be built on top of the pool manager.
  * A base service implementation will be built to handle workflow message, status reporting, file transfer (with S3)
  * An ingestion class will be written to take data into the system for processing through a defined workflow. There will need to be at least one UI built for this class. (i.e. command line, GUI, web form).

### Pool Management Options ###

There are some measurements we can use to quantify the state of the pool and it's ability to handle the work load. This is a list of values are;
  * idle interval
  * busy interval
  * idle servers
  * busy servers
  * server duty cycle
  * pool size
  * number of messages in the work queue

If the pool is idle, there should be the min servers running.

If the pool is fully busy, all servers will be busy and the number of servers will be at the max.

If the pool has been busy (no idle servers) and the pool isn't at max size, when the ramp up delay has passed, more server will be added (based on a configurable value)

If the pool is partially idle (some servers busy, some servers idle), when the ramp down delay has passed, some servers should be shut down.

If there are messages in the queue, the pool should not be ramped down.

If there are messages in the queue and the pool is being ramped up, the number of servers added should be based on some combination of the ramp up interval and the number of messages in the queue.

It is important to note that since instance status messages might not arrive in order, it might be impossible to tell for certain, which servers are idle and which are busy. Further, the timestamp in those messages can't be used to track pool state without some means of storing message for a period of time to ensure nothing is processed out of sequence. To provide a better assessment of service instance usage each server will report the amount of time since it's last message as (LastInterval). This will be used to calculate real utilization. If an instance is truely idle, it might not report status for some time, so the pool manager will need to track time on the idle queue internally. All of this information will be used to come up with a load value for each server, and that will be used to determine pool ramp-up/ramp-down.

# Phase 2 #

This phase will include a UI to monitor the pool status and tweak them in real-time. I'm working with Dojo and JSON to put the UI together. The persistence layer will also be improved. The current status saver will be augmented with one using iBatis and possibly SimpleDB. This will also be designed to handle other persistence needs beyond status, like project, batch, workflow config.