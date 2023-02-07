package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, 100)
		gantt           = make([]TimeSlice, 0)
	)

	ct := 0

	aTimesMap := make(map[int]Process)
	processesLeft := make(map[int]Process)
	aTimeNums := make([]int, 0, len(processes))
	for i := range processes {
		aTimesMap[int(processes[i].ArrivalTime)] = processes[i]
		processesLeft[int(processes[i].ProcessID)] = processes[i]
		aTimeNums = append(aTimeNums, int(processes[i].ArrivalTime))
	}
	sort.Ints(aTimeNums)

	currProcess := aTimesMap[aTimeNums[0]]
	var start int64
	for i, a := range aTimeNums {
		if i != 0 {
			serviceTime += int64(aTimeNums[i] - aTimeNums[i-1])
		}
		/*
			if a > 0 {
				waitingTime = serviceTime
			}
			totalWait += float64(waitingTime)
		*/
		//start := waitingTime

		if i == 0 {
			continue
		}

		if aTimesMap[a].Priority < currProcess.Priority {
			waitingTime = start
			totalWait += float64(waitingTime)

			completion := a
			lastCompletion = float64(completion)

			turnaround := completion - int(currProcess.ArrivalTime)
			totalTurnaround += float64(turnaround)

			schedule[ct] = []string{
				fmt.Sprint(currProcess.ProcessID),
				fmt.Sprint(currProcess.Priority),
				fmt.Sprint(completion - int(start)),
				fmt.Sprint(currProcess.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   currProcess.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})

			currProcess.BurstDuration -= int64(a)
			processesLeft[int(currProcess.ProcessID)] = currProcess

			currProcess = aTimesMap[a]

			start = int64(a)

			ct += 1
		}

		if int(currProcess.ArrivalTime+currProcess.BurstDuration) <= a || i == len(aTimeNums)-1 {
			waitingTime = start
			totalWait += float64(waitingTime)

			completion := start + currProcess.BurstDuration
			lastCompletion = float64(completion)

			turnaround := completion - currProcess.ArrivalTime
			totalTurnaround += float64(turnaround)

			schedule[ct] = []string{
				fmt.Sprint(currProcess.ProcessID),
				fmt.Sprint(currProcess.Priority),
				fmt.Sprint(completion - start),
				fmt.Sprint(currProcess.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			serviceTime = completion

			gantt = append(gantt, TimeSlice{
				PID:   currProcess.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})

			delete(processesLeft, int(currProcess.ProcessID))

			currProcess = aTimesMap[a]

			start = int64(a)

			ct += 1
		}
	}

	var priorityMap = make(map[int][]Process)

	for _, v := range processesLeft {
		val, ok := priorityMap[int(v.Priority)]

		if ok {
			priorityMap[int(v.Priority)] = append(val, v)
		} else {
			priorityMap[int(v.Priority)] = []Process{v}
		}
	}

	prioritys := make([]int, 0, len(priorityMap))
	for k, _ := range priorityMap {
		prioritys = append(prioritys, k)
	}

	sort.Ints(prioritys)
	for _, p := range prioritys {

		if len(priorityMap[p]) == 1 {
			process := priorityMap[p][0]

			//if process.ArrivalTime > 0 {
			waitingTime = serviceTime
			//}
			totalWait += float64(waitingTime)

			start := waitingTime

			completion := process.BurstDuration + waitingTime
			lastCompletion = float64(completion)

			turnaround := completion - process.ArrivalTime
			totalTurnaround += float64(turnaround)

			schedule[ct] = []string{
				fmt.Sprint(process.ProcessID),
				fmt.Sprint(process.Priority),
				fmt.Sprint(process.BurstDuration),
				fmt.Sprint(process.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			serviceTime += process.BurstDuration

			gantt = append(gantt, TimeSlice{
				PID:   process.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})

			ct += 1
		} else {
			aTimesMap := make(map[int]Process)
			aTimeNums := make([]int, 0, len(priorityMap[p]))
			for _, v := range priorityMap[p] {
				aTimesMap[int(v.ArrivalTime)] = v
				aTimeNums = append(aTimeNums, int(v.ArrivalTime))
			}
			sort.Ints(aTimeNums)

			for _, a := range aTimeNums {
				if aTimesMap[a].ArrivalTime > 0 {
					waitingTime = serviceTime
				}
				totalWait += float64(waitingTime)

				start := waitingTime

				completion := aTimesMap[a].BurstDuration + waitingTime
				lastCompletion = float64(completion)

				turnaround := completion - aTimesMap[a].ArrivalTime
				totalTurnaround += float64(turnaround)

				schedule[ct] = []string{
					fmt.Sprint(aTimesMap[a].ProcessID),
					fmt.Sprint(aTimesMap[a].Priority),
					fmt.Sprint(aTimesMap[a].BurstDuration),
					fmt.Sprint(aTimesMap[a].ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				serviceTime += aTimesMap[a].BurstDuration

				gantt = append(gantt, TimeSlice{
					PID:   aTimesMap[a].ProcessID,
					Start: start,
					Stop:  serviceTime,
				})

				ct += 1
			}

		}
	}

	count := float64(ct)
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, 100)
		gantt           = make([]TimeSlice, 0)
	)

	ct := 0

	aTimesMap := make(map[int]Process)
	processesLeft := make(map[int]Process)
	aTimeNums := make([]int, 0, len(processes))
	for i := range processes {
		aTimesMap[int(processes[i].ArrivalTime)] = processes[i]
		processesLeft[int(processes[i].ProcessID)] = processes[i]
		aTimeNums = append(aTimeNums, int(processes[i].ArrivalTime))
	}
	sort.Ints(aTimeNums)

	currProcess := aTimesMap[aTimeNums[0]]
	var start int64
	for i, a := range aTimeNums {
		if i != 0 {
			serviceTime += int64(aTimeNums[i] - aTimeNums[i-1])
		}
		/*
			if a > 0 {
				waitingTime = serviceTime
			}
			totalWait += float64(waitingTime)
		*/
		//start := waitingTime

		if i == 0 {
			continue
		}

		if int(currProcess.BurstDuration)-a > int(aTimesMap[a].BurstDuration) {
			waitingTime = start
			totalWait += float64(waitingTime)

			completion := a
			lastCompletion = float64(completion)

			turnaround := completion - int(currProcess.ArrivalTime)
			totalTurnaround += float64(turnaround)

			schedule[ct] = []string{
				fmt.Sprint(currProcess.ProcessID),
				fmt.Sprint(currProcess.Priority),
				fmt.Sprint(completion - int(start)),
				fmt.Sprint(currProcess.ArrivalTime),
				fmt.Sprint(start),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   currProcess.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})

			currProcess.BurstDuration -= int64(a)
			processesLeft[int(currProcess.ProcessID)] = currProcess

			currProcess = aTimesMap[a]

			start = int64(a)

			ct += 1
		} else if int(currProcess.BurstDuration)-a <= 0 || i == len(aTimeNums)-1 {
			waitingTime = start
			totalWait += float64(waitingTime)

			completion := start + currProcess.BurstDuration
			lastCompletion = float64(completion)

			turnaround := completion - currProcess.ArrivalTime
			totalTurnaround += float64(turnaround)

			schedule[ct] = []string{
				fmt.Sprint(currProcess.ProcessID),
				fmt.Sprint(currProcess.Priority),
				fmt.Sprint(completion - start),
				fmt.Sprint(currProcess.ArrivalTime),
				fmt.Sprint(start),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			serviceTime = completion

			gantt = append(gantt, TimeSlice{
				PID:   currProcess.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})

			delete(processesLeft, int(currProcess.ProcessID))

			currProcess = aTimesMap[a]

			start = int64(a)

			ct += 1
		}
	}

	var burstMap = make(map[int][]Process)

	for _, v := range processesLeft {
		val, ok := burstMap[int(v.BurstDuration)]

		if ok {
			burstMap[int(v.BurstDuration)] = append(val, v)
		} else {
			burstMap[int(v.BurstDuration)] = []Process{v}
		}
	}

	keys := make([]int, 0, len(burstMap))
	for k := range burstMap {
		keys = append(keys, k)
	}

	sort.Ints(keys)
	for _, k := range keys {

		if len(burstMap[k]) == 1 {
			process := burstMap[k][0]

			//if process.ArrivalTime > 0 {
			waitingTime = serviceTime
			//}
			totalWait += float64(waitingTime)

			start := waitingTime

			completion := process.BurstDuration + waitingTime
			lastCompletion = float64(completion)

			turnaround := completion - process.ArrivalTime
			totalTurnaround += float64(turnaround)

			schedule[ct] = []string{
				fmt.Sprint(process.ProcessID),
				fmt.Sprint(process.Priority),
				fmt.Sprint(process.BurstDuration),
				fmt.Sprint(process.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			serviceTime += process.BurstDuration

			gantt = append(gantt, TimeSlice{
				PID:   process.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})

			ct += 1
		} else {
			aTimesMap := make(map[int]Process)
			aTimeNums := make([]int, 0, len(burstMap[k]))
			for i := range burstMap[k] {
				aTimesMap[int(burstMap[k][i].ArrivalTime)] = burstMap[k][i]
				aTimeNums = append(aTimeNums, int(burstMap[k][i].ArrivalTime))
			}
			sort.Ints(aTimeNums)

			for _, a := range aTimeNums {
				if aTimesMap[a].ArrivalTime > 0 {
					waitingTime = serviceTime
				}
				totalWait += float64(waitingTime)

				start := waitingTime

				completion := aTimesMap[a].BurstDuration + waitingTime
				lastCompletion = float64(completion)

				turnaround := completion - aTimesMap[a].ArrivalTime
				totalTurnaround += float64(turnaround)

				schedule[ct] = []string{
					fmt.Sprint(aTimesMap[a].ProcessID),
					fmt.Sprint(aTimesMap[a].Priority),
					fmt.Sprint(aTimesMap[a].BurstDuration),
					fmt.Sprint(aTimesMap[a].ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				serviceTime += aTimesMap[a].BurstDuration

				gantt = append(gantt, TimeSlice{
					PID:   aTimesMap[a].ProcessID,
					Start: start,
					Stop:  serviceTime,
				})

				ct += 1
			}

		}
	}

	count := float64(ct)
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, 100)
		gantt           = make([]TimeSlice, 0)
	)
	ct := 0
	var timeQ int64 = 3 //time quantum of 3
	remProcesses := make(map[int64]Process)

	for _, p := range processes {
		remProcesses[p.ProcessID] = p
	}

	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	for len(remProcesses) > 0 {

		for _, p := range processes {

			process, ok := remProcesses[p.ProcessID]

			if !ok {
				continue
			}

			waitingTime = serviceTime
			totalWait += float64(waitingTime)

			start := waitingTime

			if process.BurstDuration < timeQ {

				completion := waitingTime + process.BurstDuration
				lastCompletion = float64(completion)

				turnaround := completion - process.ArrivalTime
				totalTurnaround += float64(turnaround)

				schedule[ct] = []string{
					fmt.Sprint(process.ProcessID),
					fmt.Sprint(process.Priority),
					fmt.Sprint(process.BurstDuration),
					fmt.Sprint(process.ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				ct += 1
				serviceTime += process.BurstDuration

				gantt = append(gantt, TimeSlice{
					PID:   process.ProcessID,
					Start: start,
					Stop:  serviceTime,
				})

				delete(remProcesses, process.ProcessID)

			} else {
				burst := timeQ
				completion := waitingTime + burst
				lastCompletion = float64(completion)

				turnaround := completion - process.ArrivalTime
				totalTurnaround += float64(turnaround)

				schedule[ct] = []string{
					fmt.Sprint(process.ProcessID),
					fmt.Sprint(process.Priority),
					fmt.Sprint(burst),
					fmt.Sprint(process.ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				ct += 1
				serviceTime += burst

				gantt = append(gantt, TimeSlice{
					PID:   process.ProcessID,
					Start: start,
					Stop:  serviceTime,
				})

				process.BurstDuration -= burst
				if process.BurstDuration == 0 {
					delete(remProcesses, process.ProcessID)
				} else {
					remProcesses[process.ProcessID] = process
				}
			}

		}
	}

	count := float64(ct)
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
