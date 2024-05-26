package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

/*
	***	baraye teste rahat tar joda az http handlere ye random data producer gozashtam ke halate stream kardane dataro shabih sazi kone
		bara teste http mitunid az commnet daresh biarid

	***	behtare bood tu proje az channel design pattern haye go estefade beshe ke khyli zaman nadashtam baraye kar kardan roosh

*/

// data structure of input data
type InputData struct {
	id       int
	userId   int
	postDate time.Time
}

// hit limit per userId
const PER_MINUTE_LIMIT = 5
const PER_MONTH_LIMIT = 15

var userHitCounter sync.Map

// kind of smiulating db... qatan moqeye ditribute kardan javab nmide va bayad pattern dorost entekhab beshe
var simulatedDB sync.Map

func main() {

	// Create a channel to receive and store inputs
	dataStream := make(chan InputData)
	storeStream := make(chan InputData)

	// Start a Goroutine to produce a stream of random ids and duplicate data.
	fmt.Printf("**** Start producing random data ****\n")
	go produceRandomData(dataStream)

	// Start a Goroutine to validate and process the stream and send it to storeStream(store blackbox).
	fmt.Printf("#### Start processing data ####\n")
	go processStream(dataStream, storeStream)

	// Print the stored data.
	fmt.Printf("LOG Start printing stored data LOG\n")
	for result := range storeStream {
		fmt.Printf("Stored successfully: %d\n", result.id)
	}
}

// validate data before proccess
func validateInput(itm InputData) bool {
	isValid := true
	reason := "duplicate data"
	// check limits
	isValid, reason = validateHitLimit(itm.userId)
	// check duplication
	value, ok := simulatedDB.Load(itm.id)
	if ok {
		value = value
		isValid = false
		reason = "duplicate data"
	}

	fmt.Printf("REJECT DATA: %d | Reason:%s \n", itm.id, reason)
	return isValid
}

// for support distribution we need appropriate data source and we need to refactor this function
func validateHitLimit(userId int) (bool, string) {
	isValid := true
	reason := ""

	value, ok := userHitCounter.Load(userId)
	if ok {
		theMp := value.(map[string]any)
		currentMinute := time.Now().Truncate(60 * time.Second)
		currentMonth := truncateToMonth(time.Now())
		// check per minute limit
		if theMp["minute"] == currentMinute {
			if theMp["minuteCount"].(int) >= PER_MINUTE_LIMIT {
				isValid = false
				reason = "per minute hit limit exceed"
			}
			theMp["minuteCount"] = theMp["minuteCount"].(int) + 1
		} else {
			theMp["minuteCount"] = 1
			theMp["minute"] = currentMinute
		}

		// check per month limit
		if theMp["month"] == currentMonth {
			if theMp["monthCount"].(int) >= PER_MONTH_LIMIT {
				isValid = false
				reason = "per month hit limit exceed"
			}
			theMp["monthCount"] = theMp["monthCount"].(int) + 1
		} else {
			theMp["monthCount"] = 1
			theMp["month"] = currentMonth
		}
		userHitCounter.Store(userId, theMp)
	} else {
		// initiate on first call per userid
		mp := map[string]any{
			"minuteCount": 1,
			"monthCount":  1,
			"minute":      time.Now().Truncate(60 * time.Second),
			"month":       truncateToMonth(time.Now()),
		}
		userHitCounter.Store(userId, mp)
	}
	return isValid, reason
}

// do any processing on input data.
func processStream(dataStream chan InputData, storedStream chan InputData) {
	for itm := range dataStream {
		if validateInput(itm) {
			fmt.Printf("[$$$] Processed %d\n", itm.id)
			storedStream <- itm
			simulatedDB.Store(itm.id, itm)
		}
	}
	close(storedStream)
}

// generates a stream of random integers and sends them to the dataStream channel.
func produceRandomData(dataStream chan InputData) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10; i++ {
		// randomNumber := rand.Intn(500)
		randomNumber := i
		randomData := InputData{
			id:       randomNumber,
			userId:   2,
			postDate: time.Now(),
		}
		dataStream <- randomData
		fmt.Printf("Received <-- %d\n", randomData.id)
		time.Sleep(time.Second * 1) // Simulate data streaming delay
	}
	// create duplicate data for test
	duplicateData := InputData{
		id:       2424,
		userId:   24,
		postDate: time.Now(),
	}
	dataStream <- duplicateData
	dataStream <- duplicateData
	dataStream <- duplicateData
	dataStream <- duplicateData

	close(dataStream)
}

// util functions
func truncateToMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 0, 0, 0, 0, 0, t.Location())
}

// http.HandleFunc("/", httpPostReceiver)

// fmt.Printf("Starting server for HTTP POST receiver...\n")
// if err := http.ListenAndServe(":2424", nil); err != nil {
// 	log.Fatal(err)
// }

// func httpPostReceiver(w http.ResponseWriter, r *http.Request) {
// 	if r.URL.Path != "/" {
// 		http.Error(w, "404 not found.", http.StatusNotFound)
// 		return
// 	}

// 	switch r.Method {
// 	case "POST":
// 		decoder := json.NewDecoder(r.Body)
// 		var input inputData
// 		err := decoder.Decode(&input)
// 		if err != nil {
// 			panic(err)
// 		}
// 		fmt.Fprintf(w, "Recieved: %v", input.id)

// 	default:
// 		fmt.Fprintf(w, "Sorry, only POST methods are supported.")
// 		http.Error(w, "GET not support.", http.StatusMethodNotAllowed)
// 		return
// 	}
// }
