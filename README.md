To run:
   go run .\main.go [input file]

Implementation notes:
------------------------
-  The way waitingTime and turnaround time were calculated in the provided FCFS implementation did not
   seem right to me based on what I read in the Zybooks readings, so I slightly changed them in the other
   three algorithm implementations (I left FCFS untouched):
      1) For the other scheduling algorithms, I implemented waitingTime differently by having the waitingTime 
         for a process be equal to the point where a process starts in the schedule, as this is consistent 
         with what is presesnted in the Zybooks readings
      2) I also implented turnaround time differently in the other algorithms by calculating turnaround time
         as the difference between a process's completion time (either finished or preempted) and the process's 
         arrival time, as this is also consistent with what is presesnted in the Zybooks readings as well as what
         I found from research on the Internet
-  For the Round-Robin scheduling function, since it was not otherwise specified, the time quantum used is 3