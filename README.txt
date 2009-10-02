Since the dawn of concurrency research, there have been two camps: shared everything, and shared nothing. Most modern applications use threads for concurrency, a shared everything architecture.

Actors, however, use a shared nothing architecture where lightweight processes communicate with each other using message passing. Actors can change their state, create a new Actor, send a message to any Actor it has the Address of, and wait for a specific kind of message to arrive in it's mailbox.

Actors: What, Why, and How
===========================

What Is an Actor?
--------------------------------------------------------------------------------

* An Actor Is a Process
* An Actor Can Change It's Own State
* An Actor Can Create Another Actor and Get it's Address
* An Actor Can Send a Message To Any Addresses It Knows
* An Actor Can Wait for a Specific Message to Arrive in It's Mailbox

Why Use Actors?
--------------------------------------------------------------------------------

* Only an Actor Can Change It's Own State
* Each Actor Is a Process, Simplifying Control Flow
* Message Passing Is Easy to Distribute
* Most Exceptional Conditions Occur When Waiting for a Message
	* Isolates Error Handling Code
	* Makes It Easier to Build Fault Tolerant Distributed Systems

How Are Actors Implemented in Python Actors?
============================================

Use Eventlet's Green Threads to Implement Processes
--------------------------------------------------------------------------------

* This Doesn't Provide Real Isolation
* But Python Doesn't Provide Private Either

Copy Messages As They Are Sent Between Actors
--------------------------------------------------------------------------------

* This Provides Good Enough Isolation
* We Can Serialize/Deserialize With simplejson To Copy
	* This Also Makes Messages Network Safe
	* Other More Optimized Implementations Possible

A WSGI Application Exposes Actors to Network
--------------------------------------------------------------------------------

* Uses a Simple REST Protocol
	* PUT Spawns an Actor
	* POST Sends a Message to an Actor
	* GET Gets the Current State of the Actor
	* DELETE Sends a Killed Exception to the Actor

Problem: Imported Modules Leak State Between Actors
--------------------------------------------------------------------------------

* Possibility: Keep a Unique Copy of sys.modules for Every Actor
* Possibility: Seal Modules in Wrapper Object Preventing Modification
* Reality: Just Write Code that Doesn't Abuse Global Module State
