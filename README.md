## Academic Context

This project was completed as an **individual university project** for the Bachelor of Science in Computer Science at Stellenbosch University.

All design decisions, implementation, and testing were performed independently. The repository is shared publicly as part of my professional portfolio.

**Process Scheduling Simulation**

*Overview*

This project simulates process scheduling with multiple algorithms:

First-Come-First-Serve (FCFS)
Round-Robin (RR)
Priority Scheduling with Preemption
It manages process execution, resource allocation, preemption, and deadlock detection.

*Usage*

Compile and run the program:

./scheduler <init_data> <process_file> <algorithm> <time_quantum>

<algorithm>: 0 for Priority, 1 for Round-Robin, 2 for FCFS
<time_quantum>: Used for RR scheduling.
